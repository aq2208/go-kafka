package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

// ConsumerGroupHandler handles Kafka consumer group events
type ConsumerGroupHandler struct{}

// Setup is called when a new session is started for a consumer group
func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session setup")
	return nil
}

// Cleanup is called at the end of a session
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session cleanup")
	return nil
}

// ConsumeClaim is where the messages are processed
func (ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Consumed message: %s", string(msg.Value))
		session.MarkMessage(msg, "") // Mark the message as processed
	}
	return nil
}

func main() {
	// Define Kafka Consumer configuration for Sarama
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true                     // Enable error return
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // Rebalancing strategy (Range or RoundRobin)
	consumerConfig.Consumer.Group.Session.Timeout = 30 * 1000 * 1000 // Session timeout (in nanoseconds)
	consumerConfig.Consumer.Group.Heartbeat.Interval = 10 * 1000 * 1000 // Heartbeat interval (in nanoseconds)
	consumerConfig.Net.SASL.Enable = false                          // Set to true if SASL is enabled (for authentication)
	consumerConfig.Net.TLS.Enable = false                           // Set to true if TLS is enabled (for encryption)
	consumerConfig.ClientID = "my-consumer"                         // Custom client ID for the consumer

	// Create a new consumer group
	groupID := "my-consumer-group"
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, groupID, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	// Create the handler for consuming messages
	handler := ConsumerGroupHandler{}

	// Subscribe to the topic
	topics := []string{"my-topic"}

	// Start consuming messages
	for {
		if err := consumer.Consume(context.TODO(), topics, handler); err != nil {
			log.Fatalf("Error consuming messages: %v", err)
		}
	}
}