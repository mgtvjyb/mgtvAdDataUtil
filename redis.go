package mgtvAdDataUtil

import (
	"errors"
	"fmt"
	"gopkg.in/redis.v3"
	"time"
)

var (
	client RedisClient = nil
)

type RedisClient interface {
	Del(keys ...string) *redis.IntCmd
}

func InitRedisClient(addr string, poolSize int, timeout time.Duration, password string) (RedisClient, error) {
	clientR := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     poolSize,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		Password:     password,
	})
	_, err := clientR.Ping().Result()
	if err != nil {
		fmt.Println("init redis ping error", err)
	}
	client = clientR
	fmt.Println("redis client-----------", client)
	return client, err
}

func (*RedisClient) Del(key string) error {
	return client.Del(key).Err()
}
