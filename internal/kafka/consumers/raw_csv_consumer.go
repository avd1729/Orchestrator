package consumers

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

// KafkaConsumer listens for messages and exits after 1 second of no new messages.
func KafkaConsumer(ctx context.Context) ([]string, error) {
	var messages []string
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "recommendations-topic",
		GroupID: "csv-consumer-group",
	})
	defer reader.Close()

	log.Println("Started consuming from Kafka")

	timeoutDuration := 1 * time.Second
	timeoutTicker := time.NewTicker(timeoutDuration) // Used to trigger timeout checks
	defer timeoutTicker.Stop()

	lastReadTime := time.Now()

	// Start an infinite loop to consume messages with a timeout
	for {
		select {
		case <-ctx.Done():
			return messages, nil // Exit when context is done
		case <-timeoutTicker.C:
			// Check if 1 second has passed since the last message read
			if time.Since(lastReadTime) >= timeoutDuration {
				log.Println("No new messages for 1 second, stopping consumer.")
				return messages, nil // Stop the consumer after 1 second of inactivity
			}
		default:
			// Try to read the message from Kafka
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message: %v", err)
				continue
			}

			// Add message to the result list
			messages = append(messages, string(msg.Value))
			log.Printf("Received message: %s", string(msg.Value))

			// Reset the last read time when a message is received
			lastReadTime = time.Now()
		}
	}
}
