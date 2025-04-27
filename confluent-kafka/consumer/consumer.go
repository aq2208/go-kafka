package main

import (
	"log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Define the Kafka consumer configuration
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",    // Kafka broker address
		"group.id":             "my-consumer-group", // Consumer group ID
		"auto.offset.reset":    "earliest",          // Start reading from the earliest message
		"enable.auto.commit":   true,                // Enable auto commit for offsets
		"session.timeout.ms":   10000,               // Timeout for consumer session
		"max.poll.records":     500,                 // Max number of records to fetch per poll
		"fetch.max.wait.ms":    500,                 // Max wait time for fetching data
		"heartbeat.interval.ms": 3000,               // Heartbeat interval for consumer
		"security.protocol":    "PLAINTEXT",         // No encryption for this example
		"client.id":            "my-kafka-client",   // Client ID for identifying the consumer
		"debug":                "broker,protocol",   // Enable debugging for broker and protocol
	}

	// Create the Kafka consumer
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic
	topic := "my-topic"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1) // -1 to block until a message is received
		if err == nil {
			log.Printf("Received message: %s from topic: %s", string(msg.Value), *msg.TopicPartition.Topic)
		} else {
			log.Printf("Error consuming message: %s", err)
		}
	}
}