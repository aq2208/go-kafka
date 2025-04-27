package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	// Define the Kafka producer configuration
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",  // Kafka broker address
		"acks":               "all",             // Waiting for all replicas to acknowledge (strongest durability)
		"retries":            5,                 // Retry up to 5 times
		"batch.num.messages": 100,               // Maximum batch size (100 messages)
		"linger.ms":          100,               // Delay to accumulate batch
		"compression.codec":  "gzip",            // Use GZIP compression
		"message.max.bytes":  1048576,           // Max message size (1MB)
		"security.protocol":  "PLAINTEXT",       // No encryption for this example (can be changed to SSL or SASL)
		"client.id":          "my-kafka-client", // Client ID for identifying the producer
		"debug":              "broker,protocol", // Enable debugging for broker and protocol
	}

	// Create the Kafka producer
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Define the topic and the message to be sent
	topic := "my-topic"
	message := "Hello, Kafka!"

	// Produce a message
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny, // Let Kafka decide the partition
		},
		Value: []byte(message),
	}, nil)

	// Wait for message to be delivered
	producer.Flush(15 * 1000) // Timeout of 15 seconds
	if err != nil {
		log.Fatalf("Failed to send message: %s", err)
	} else {
		log.Printf("Message '%s' sent to topic '%s'", message, topic)
	}
}
