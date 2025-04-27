package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	// Configure Kafka producer
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",  // Kafka broker address
		"acks":               "all",             // Waiting for all replicas to acknowledge (strongest durability)
		"retries":            5,                 // Retry up to 5 times if the message delivery fails
		"batch.num.messages": 100,               // Maximum batch size before sending (100 messages)
		"linger.ms":          100,               // Delay to accumulate batch (message buffer time)
		"compression.codec":  "gzip",            // Use GZIP compression for messages to save space
		"message.max.bytes":  1048576,           // Max message size (1MB)
		"security.protocol":  "PLAINTEXT",       // No encryption for this example (can be changed to SSL or SASL)
		"client.id":          "my-kafka-client", // Custom Client ID for identifying the producer
		"debug":              "broker,protocol", // Enable debugging for broker and protocol
	}

	// Create Kafka producer with the defined configuration
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()  // Close the producer when done

	// Define the topic and the message to be sent
	topic := "my-topic"
	message := "Hello, Kafka!"

	// Create the producer message
	producerMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic, // Set the topic to send the message to
			Partition: kafka.PartitionAny, // Let Kafka decide the partition
		},
		Value: []byte(message), // Set the message content
	}

	// Send the message asynchronously
	err = producer.Produce(producerMessage, nil) // Second argument is for delivery report, set to nil for now
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	} else {
		log.Printf("Message '%s' sent to topic '%s'", message, topic)
	}

	// Wait for any remaining messages in the producer buffer to be delivered
	producer.Flush(15 * 1000) // Wait for a maximum of 15 seconds for pending messages
	if err != nil {
		log.Fatalf("Failed to send message: %s", err)
	} else {
		log.Printf("Message '%s' sent to topic '%s'", message, topic)
	}
}
