package redislock_go

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"testing"
	"time"
)

func TestRedisLock(t *testing.T) {
	ctx := context.Background()
	rl := New(
		makeRedisClient(),
		"test-lock-lock",
		10*time.Second,
		0,
		true,
		5*time.Minute)
	unlockFunc, err := rl.Lock(ctx)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Minute)
	err = unlockFunc()
	if err != nil {
		log.Fatal(err)
	}
}

type Config struct {
	Host           string        `ini:"host"`
	Port           int           `ini:"port,default(6379)"`
	User           string        `ini:"user"`
	Password       string        `ini:"password"`
	Database       int           `ini:"database,default(0)"`
	ConnectTimeout time.Duration `ini:"connect_timeout,default(5s)"`
	ReadTimeout    time.Duration `ini:"read_timeout,default(3s)"`
	WriteTimeout   time.Duration `ini:"write_timeout,default(3s)"`
}

func makeRedisClient() *redis.Client {
	conf := Config{
		Host:           "127.0.0.1",
		Port:           6379,
		User:           "",
		Password:       "xxxx",
		Database:       4,
		ConnectTimeout: 5 * time.Second,
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
	}
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		Username:     conf.User,
		Password:     conf.Password,
		DB:           conf.Database,
		DialTimeout:  conf.ConnectTimeout,
		ReadTimeout:  conf.ReadTimeout,
		WriteTimeout: conf.WriteTimeout,
	})
	return client
}
