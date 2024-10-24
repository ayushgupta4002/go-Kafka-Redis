package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `json:"text"`
}

var rdb *redis.Client

func main() {

	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Redis address
		DB:   0,                // Use default DB
	})

	app := fiber.New()
	api := app.Group("/api")
	api.Post("/comment", createComment)
	api.Get("/getdata", getRedis)

	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	comment := new(Comment)
	if err := c.BodyParser(comment); err != nil {
		return c.Status(400).SendString(err.Error())
	}
	cmtBytes, err := json.Marshal(comment)
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}

	//push commentt to kafka here
	pushKafka("comments", cmtBytes)

	err = c.JSON(&fiber.Map{
		"message": "Comment created",
		"comment": comment,
	})
	if err != nil {
		c.JSON(&fiber.Map{
			"message": "error in creating comment",
		})

		return err
	}
	return err

}

func pushKafka(topic string, message []byte) error {
	connectUrl := []string{"localhost:29092"}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	conn, err := sarama.NewSyncProducer(connectUrl, config)
	if err != nil {
		return err
	}
	defer conn.Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := conn.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil

}

var ctx = context.Background()

func getRedis(c *fiber.Ctx) error {

	key, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("Could not fetch keys: %v", err))
	}
	results := make(map[string]string)

	for _, k := range key {
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			log.Fatalf("Could not fetch value: %v", err)
			continue
		}
		results[k] = val
		fmt.Println(k)

	}
	return c.JSON(results)

}
