package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	// Define the Kafka consumer configuration
	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",    // Kafka broker address
		"group.id":          "my-consumer-group", // Consumer group ID
		"auto.offset.reset": "earliest",          // Start from the earliest available offset if no previous offset exists
		// "enable.auto.commit":   true,                // Enable auto commit for offsets
		"enable.auto.commit":    false,             // Disable auto-commit, will manually commit offsets
		"session.timeout.ms":    10000,             // Session timeout (in ms) for consumer group coordination
		"max.poll.records":      500,               // Max number of records to fetch per poll
		"fetch.max.wait.ms":     500,               // Max wait time for fetching data
		"heartbeat.interval.ms": 3000,              // Heartbeat interval (in ms) for keeping the session alive
		"security.protocol":     "PLAINTEXT",       // No encryption for this example
		"client.id":             "my-kafka-client", // Custom Client ID for identifying the consumer
		"debug":                 "broker,protocol", // Enable debugging for broker and protocol
	}

	// Create Kafka consumer with the defined configuration
	consumer, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close() // Close the consumer when done

	// Subscribe to the Kafka topic(s)
	topic := "my-topic"
	err = consumer.Subscribe(topic, nil) // Subscribe to "my-topic"
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	// Start consuming messages from the topic
	for {
		// Poll messages from Kafka
		msg, err := consumer.ReadMessage(-1) // -1 means it will block indefinitely waiting for a message
		if err != nil {
			log.Printf("Error consuming message: %v", err)
			continue
		}

		// Process the consumed message
		log.Printf("Consumed message: %s", string(msg.Value))

		// Manually commit the offset after processing the message
		_, err = consumer.CommitOffsets([]kafka.TopicPartition{
			{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}, // Increment the offset by 1
		})
		if err != nil {
			log.Printf("Failed to commit offset: %v", err)
		}
	}
}
