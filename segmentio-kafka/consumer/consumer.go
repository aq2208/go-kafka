package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go" // Import Kafka Go client
)

func main() {
	// Create a new Kafka reader (Consumer)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9094"}, // Kafka broker address
		Topic:   "my-topic",                // Kafka topic to consume from
		// GroupID: "my-consumer-group",       // Consumer group name (helps in load balancing)
		// MinBytes: 10e3,   // 10KB, minimum amount of data for a poll request
		// MaxBytes: 10e6,   // 10MB, maximum amount of data for a poll request
	})

	// Read messages in an infinite loop
	for {
		// Poll for messages from Kafka
		msg, err := reader.ReadMessage(context.TODO())
		if err != nil {
			log.Fatal("could not read message from Kafka:", err)
		}

		// Print out the message value (contents)
		fmt.Printf("Consumed: %s\n", string(msg.Value))
	}

	// Close the reader when done (this is not reached in this infinite loop)
	defer reader.Close()
}
