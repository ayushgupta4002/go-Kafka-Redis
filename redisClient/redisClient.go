package redisclient

import (
	"context"
	"log"
	"sync"

	"github.com/go-redis/redis/v8"
)

var instance *redis.Client
var mutex = &sync.Mutex{}
var ctx = context.Background()

func InitRedis() {
	if instance == nil {
		mutex.Lock()
		defer mutex.Unlock()
		instance = redis.NewClient(&redis.Options{
			Addr: "localhost:6379", // Redis address
			DB:   0,                // Use default DB
		})

		_, err := instance.Ping(ctx).Result()
		if err != nil {
			log.Fatal(err)
		}
	}
}
func GetInstance() *redis.Client {
	if instance == nil {
		log.Fatal("Redis client is not initialized")
	}
	return instance
}
func GetContext() context.Context {
	return ctx
}
