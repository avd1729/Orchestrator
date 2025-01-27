package consumers

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

// MessageChan is a channel that will be used to send messages to the UI
var MessageChan = make(chan string, 100) // buffered channel to prevent blocking

func KafkaConsumer(ctx context.Context) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "recommendations-topic",
		GroupID: "csv-consumer-group",
	})
	defer reader.Close()

	log.Println("Started consuming from Kafka")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message: %v", err)
				continue
			}
			MessageChan <- string(msg.Value)
		}
	}
}
