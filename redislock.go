package redislock_go

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

var ErrLockFailed = errors.New("redislock:lock failed")
var ErrGetLockOvertime = errors.New("redislock:get lock overtime")
var ErrUnlockValEmpty = errors.New("redislock:unlock val is empty")
var ErrUnlockFailed = errors.New("redislock:unlock failed")
var ErrGetLockTimeout = errors.New("redislock:get lock timeout")

// Lock 分布式锁
type Lock struct {
	rds       *redis.Client // redis客户端
	key       string        // 锁的key
	val       string        // 锁的value
	ttl       time.Duration // 锁的过期时间-只支持1s以上的时间，小于1s默认为1s
	waitTime  time.Duration // 获取锁等待时间
	isWatch   bool          // 是否开启watch自动续期
	watchTime time.Duration // 开启watch后，最大监控持续时间，小于等于ttl表示不监控，等同于isWatch==false
	watchDone chan struct{} // 开启watch后，用于通知续期协程退出
}

// New 获取锁实例 - ！！！注意，请使用同一个实例加锁和解锁
func New(rds *redis.Client, key string, ttl, waitTime time.Duration, isWatch bool, watchTime time.Duration) *Lock {
	if ttl < time.Second {
		ttl = time.Second
	}
	isWatch = isWatch && watchTime > ttl
	var watchDone chan struct{}
	if isWatch {
		watchDone = make(chan struct{}, 1)
	}
	return &Lock{
		rds:       rds,
		key:       key,
		val:       uuid.NewString(),
		ttl:       ttl,
		waitTime:  waitTime,
		isWatch:   isWatch,
		watchTime: watchTime,
		watchDone: watchDone,
	}
}

type UnlockFunc func() error

// GetKey 获取锁的key
func (r *Lock) GetKey() string {
	return r.key
}

// GetValue 获取锁的value
func (r *Lock) GetValue() string {
	return r.val
}

// Lock 加锁
func (r *Lock) Lock(ctx context.Context) (unlockFunc UnlockFunc, err error) {
	// 如果开启watch，则加锁成功后自动开启锁续期
	defer func() {
		if err == nil && r.isWatch {
			go r.watch()
		}
	}()
	// 获取等待获取锁的超时时间
	waitTime := time.Now().Add(r.waitTime)
	get := func() (ok bool, err error) {
		ok, err = r.rds.SetNX(ctx, r.key, r.val, r.ttl).Result()
		// 报错，返回
		if err != nil {
			log.Printf("redislock lock failed. key:%s, value:%s, err:%v", r.key, r.val, err)
			return false, ErrLockFailed
		}
		// 加锁成功，返回
		if ok {
			return true, nil
		}
		// 如果等待时间小于当前时间，则返回获取锁超时
		if time.Now().After(waitTime) {
			return false, ErrGetLockOvertime
		}
		// 未获取到
		return false, nil
	}

	// 先获取一次
	ok, err := get()
	if err != nil {
		return nil, err
	}
	uCtx := context.Background()
	// 如果获取不到，则循环获取，如果获取到了，则返回
	if ok {
		return func() error {
			return r.Unlock(uCtx)
		}, nil
	}
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ErrGetLockTimeout
		case <-ticker.C:
			ok, err = get()
			if err != nil {
				return nil, err
			}
			if ok {
				return func() error {
					return r.Unlock(uCtx)
				}, nil
			}
		}
	}
}

// watch 锁自动续期
func (r *Lock) watch() {
	if !r.isWatch {
		return
	}
	log.Printf("redislock watch init. key:%s, value:%s", r.key, r.val)
	ctx, cancelFunc := context.WithTimeout(context.Background(), r.watchTime)
	defer cancelFunc()
	lua := `if redis.call("get", KEYS[1]) == ARGV[1] then
		redis.call("expire", KEYS[1], ARGV[2])
		return "1"
    else
        return "0"
    end`
	// 锁过期时间
	ttlMilli := time.Now().Add(r.ttl).UnixMilli()
	// 每200毫秒扫描一次
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("redislock watch timeout. key:%s, value:%s", r.key, r.val)
			return
		case <-r.watchDone:
			log.Printf("redislock watch done. key:%s, value:%s", r.key, r.val)
			return
		case <-ticker.C:
			s := ttlMilli - time.Now().UnixMilli()
			// 如果剩余过期时间已经过半，则进行续期操作，否则跳过
			if s > r.ttlMilli()/2 {
				continue
			}
		}
		log.Printf("redislock watch start set ttl. key:%s, value:%s", r.key, r.val)
		// 进行续期，重新设置为ttl，用lua脚本实现，判断value值相同则进行续期，不同则结束watch
		resp, err := r.rds.Eval(ctx, lua, []string{r.key}, r.val, r.ttlSec()).Result()
		if err != nil {
			log.Printf("redislock watch set ttl failed. key:%s, value:%s, err:%v", r.key, r.val, err)
			return
		}
		// 返回值是0，则表示value值已经更换，无需再进行watch
		if fmt.Sprintf("%s", resp) == "0" {
			log.Printf("redislock watch value has change. key:%s, value:%s", r.key, r.val)
			return
		}
		ttlMilli = time.Now().Add(r.ttl).UnixMilli()
		// 表示续期成功，继续watch
		log.Printf("redislock watch set ttl success. key:%s, value:%s", r.key, r.val)
	}
}

// Unlock 解锁
func (r *Lock) Unlock(ctx context.Context) error {
	if r.isWatch {
		r.watchDone <- struct{}{}
		close(r.watchDone)
	}
	if r.val == "" {
		return ErrUnlockValEmpty
	}
	lua := `if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
    else
        return "0"
    end`
	_, err := r.rds.Eval(ctx, lua, []string{r.key}, r.val).Result()
	if err != nil {
		log.Printf("redislock unlock failed. key:%s, value:%s, err:%v", r.key, r.val, err)
		return ErrUnlockFailed
	}
	return nil
}

// ttlSec 获取过期时间，单位秒
func (r *Lock) ttlSec() int64 {
	return int64(r.ttl / time.Second)
}

// ttlSec 获取过期时间，单位秒
func (r *Lock) ttlMilli() int64 {
	return int64(r.ttl / time.Millisecond)
}
