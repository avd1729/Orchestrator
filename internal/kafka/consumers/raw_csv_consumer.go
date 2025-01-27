package consumers

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

// ConsumeMessages reads messages from a Kafka topic
func ConsumeMessages(kafkaBroker, topic string) {
	// Create a Kafka reader (consumer) with the provided broker and topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		GroupID:  "consumer-group", // Group ID for consumer groups
		MinBytes: 10e3,             // Minimum bytes to fetch
		MaxBytes: 10e6,             // Maximum bytes to fetch
	})
	defer reader.Close()

	// Start consuming messages
	for {
		// Read a message from the topic
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error while consuming message: %v", err)
			break
		}

		// Process the message
		fmt.Printf("Consumed message: Key = %s, Value = %s\n", string(message.Key), string(message.Value))
	}
}
