package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/xtruder/go-kafka-protobuf/protobuf"
	"github.com/xtruder/go-kafka-protobuf/protobuf/fixture"
	"github.com/xtruder/go-kafka-protobuf/srclient"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	topic := "myTopic"
	wg := sync.WaitGroup{}
	config := &kafka.ConfigMap{
		"bootstrap.servers": "broker",
		"group.id":          "myGroup",
		"auto.offset.reset": "latest",
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	wg.Add(1)
	go func() {
		for event := range p.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error delivering the message '%s'\n", message.Key)
				} else {
					fmt.Printf("Message '%s' delivered successfully!\n", message.Key)
				}
			}
		}
	}()

	client := srclient.NewClient(srclient.WithURL("http://schema-registry:8081"))
	registrator := protobuf.NewSchemaRegistrator(client)
	serde := protobuf.NewProtoSerDe()

	schemaID, err := registrator.RegisterValue(context.Background(), topic, &fixture.User{})
	if err != nil {
		panic(fmt.Errorf("error registring schmea: %w", err))
	}

	fmt.Println("schema registered")

	wg.Add(1)
	go func() {
		for {
			key := uuid.Must(uuid.NewUUID())

			msg := &fixture.User{
				Id:        key.String(),
				UserId:    "test",
				FirstName: "Jaka",
				LastName:  "Hudoklin",
				Message:   "message",
			}

			value, err := serde.Serialize(schemaID, msg)
			if err != nil {
				panic(fmt.Errorf("error serializing message: %w", err))
			}

			fmt.Printf("Producing message: '%s'\n", key)

			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(key.String()),
				Value: value,
			}, nil)

			time.Sleep(100 * time.Millisecond)
		}
	}()

	c, err := kafka.NewConsumer(config)

	go func() {
		c.Subscribe(topic, nil)

		for {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Printf("Message on %s with key: '%s'\n", msg.TopicPartition, msg.Key)
				usr := &fixture.User{}

				_, err := serde.Deserialize(msg.Value, usr)
				if err != nil {
					fmt.Printf("Error deserializing message: %v\n", err)
				}

				fmt.Printf("Received message: %v\n", usr)
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}
	}()

	wg.Wait()
}
