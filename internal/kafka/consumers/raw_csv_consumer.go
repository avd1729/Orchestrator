package consumers

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

// KafkaConsumer will listen for new messages and return them after a timeout of 1 second.
func KafkaConsumer(ctx context.Context) ([]string, error) {
	var messages []string
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "recommendations-topic",
		GroupID: "csv-consumer-group",
	})
	defer reader.Close()

	log.Println("Started consuming from Kafka")

	// Start an infinite loop to consume messages with a timeout
	for {
		select {
		case <-ctx.Done():
			return messages, nil // Exit when context is done
		default:
			// Set a 1-second timeout for reading a message
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if err.Error() == "context deadline exceeded" {
					// No messages were received within 1 second timeout
					log.Println("Timeout reached, no new messages.")
					return messages, nil
				}
				log.Printf("Error reading message: %v", err)
				continue
			}
			// Add message to the result list
			messages = append(messages, string(msg.Value))
			log.Printf("Received message: %s", string(msg.Value))
		}

		// Sleep for 1 second before trying to fetch new messages
		time.Sleep(1 * time.Second)
	}
}
