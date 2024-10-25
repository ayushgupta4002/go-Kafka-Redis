package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	redisclient "github.com/ayushgupta4002/go-kafka/redisClient"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
)

var rdb *redis.Client
var ctx context.Context

func main() {
	redisclient.InitRedis() // Initialize Redis client
	rdb = redisclient.GetInstance()
	ctx = redisclient.GetContext()
	// rdb = redis.NewClient(&redis.Options{
	// 	Addr: "localhost:6379", // Redis address
	// 	DB:   0,                // Use default DB
	// })
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed

	// Buffer to store messages
	var messages []string
	const messageBatchSize = 5
	messageQueue := make(chan string, 100) // Buffered channel
	go processQueueAsync(messageQueue)

	msgCount := 0
	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				messages = append(messages, string(msg.Value))
				// if msgCount%messageBatchSize == 0 {
				// 	concatenatedMessages := strings.Join(messages, ", ")
				// 	fmt.Printf("Processed last %d messages: %s\n", messageBatchSize, concatenatedMessages)
				// 	messages = []string{} // Clear the buffer
				// }
				messageQueue <- string(msg.Value)
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func connectConsumer(connectUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	conn, err := sarama.NewConsumer(connectUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func processQueueAsync(queue chan string) {
	// Process the message and store it in Redis
	for message := range queue {
		err := storeInRedis(message)
		if err != nil {
			log.Printf("Failed to store message in Redis: %v", err)
		}
	}
}

func storeInRedis(message string) error {
	// Create a unique key for each message
	key := fmt.Sprintf("message:%d", hashString(message))

	// Store the message in Redis with the generated key
	err := rdb.Set(ctx, key, message, 0).Err()
	if err != nil {
		return err
	}
	log.Printf("Message stored in Redis: %s", message)
	return nil
}

func hashString(s string) int {
	// Simple hash function (you can replace this with something more complex)
	return len(s) + strings.Count(s, "")
}
