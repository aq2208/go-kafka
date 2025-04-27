package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go" // Import Kafka Go client
)

func main() {
	// Create a new Kafka writer (Producer)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9094"}, // Kafka broker address
		Topic:   "my-topic",                // Kafka topic to produce to
		Balancer: &kafka.LeastBytes{},      // Balancer used to select partition (optional)
	})

	// Produce 5 messages to the Kafka topic
	for i := 0; i < 5; i++ {
		// Create a message
		message := fmt.Sprintf("Message #%d", i+1)

		// Write the message to Kafka topic "my_topic"
		err := writer.WriteMessages(context.TODO(), kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", i)), // Key to ensure message ordering within a partition
			Value: []byte(message),                  // The actual message content
		})

		// Handle errors during message production
		if err != nil {
			log.Fatal("could not write message to Kafka:", err)
		}
		fmt.Printf("Produced: %s\n", message) // Print the message produced

		// Simulate work by waiting 1 second before sending the next message
		time.Sleep(1 * time.Second)
	}

	// Close the writer after sending all messages
	defer writer.Close()
}
