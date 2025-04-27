package main

import (
	"log"
	"github.com/IBM/sarama"
)

func main() {
	// Define Kafka Producer configuration for Sarama
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll        // Wait for all replicas to acknowledge the message
	producerConfig.Producer.Retry.Max = 5                          // Retry a maximum of 5 times on failure
	// producerConfig.Producer.Batch.Size = 1000                      // The size of the batch before sending (in bytes)
	// producerConfig.Producer.Batch.Timeout = 1000                    // Time to wait before sending the batch (in milliseconds)
	producerConfig.Producer.Compression = sarama.CompressionGZIP   // Use GZIP compression for messages
	producerConfig.Producer.Flush.Messages = 1000                   // Number of messages before flushing
	producerConfig.Producer.Flush.Frequency = 5000 * 1000          // Flush every 5 seconds (in nanoseconds)
	producerConfig.Net.SASL.Enable = false                          // Set to true if SASL is enabled (for authentication)
	producerConfig.Net.TLS.Enable = false                           // Set to true if TLS is enabled (for encryption)
	producerConfig.ClientID = "my-producer"                         // Custom client ID for the producer

	// Create Kafka producer
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.AsyncClose()

	// Define topic and message to send
	topic := "my-topic"
	message := "Hello, Kafka from Sarama!"

	// Send the message asynchronously
	messageToSend := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// Send message to Kafka
	producer.Input() <- messageToSend
	log.Printf("Message '%s' sent to topic '%s'", message, topic)

	// Wait for message success or error
	select {
	case err := <-producer.Errors():
		log.Printf("Error sending message: %v", err)
	case <-producer.Successes():
		log.Println("Message sent successfully")
	}
}