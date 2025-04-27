package main

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	// Define Kafka Consumer configuration
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true                   // Return errors if any occur
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // Rebalancing strategy (Range or RoundRobin)
	consumerConfig.Consumer.Group.Session.Timeout = 30 * 1000 * 1000 // Timeout after 30 seconds for session
	consumerConfig.Consumer.Group.Heartbeat.Interval = 10 * 1000 * 1000 // Heartbeat interval (10 seconds)
	consumerConfig.Net.SASL.Enable = false                         // Enable SASL for authentication
	consumerConfig.Net.TLS.Enable = false                          // Enable TLS for encryption
	consumerConfig.ClientID = "my-consumer"                        // Set client ID for the consumer

	// Create a new consumer group
	groupID := "my-consumer-group"
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, groupID, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumer.Close()

	// Create a message handler function for the consumer
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

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler interface
type ConsumerGroupHandler struct{}

// Setup is run at the beginning of a new session
func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session setup")
	return nil
}

// Cleanup is run at the end of a session
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