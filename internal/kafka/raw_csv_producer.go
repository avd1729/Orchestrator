package kafka

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"mime/multipart"

	"github.com/segmentio/kafka-go"
)

// ProduceCSVToKafka reads a CSV file from a multipart.File and sends data to a Kafka topic
func ProduceCSVToKafka(csvFile multipart.File, kafkaBroker, topic string) {
	// Create a CSV reader to read from the multipart file
	reader := csv.NewReader(bufio.NewReader(csvFile))

	// Read all the records from the CSV
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %v", err)
	}

	// Initialize Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Produce CSV data to Kafka
	for i, record := range records {
		if len(record) == 0 {
			log.Printf("Skipping empty record at index %d", i)
			continue
		}
		message := kafka.Message{
			Key:   []byte(record[0]), // Use the first column as the key
			Value: []byte(fmt.Sprintf("%v", record)),
		}

		err := writer.WriteMessages(context.Background(), message)
		if err != nil {
			log.Printf("Failed to write message to Kafka at index %d: %v", i, err)
		} else {
			log.Printf("Produced message to Kafka at index %d: Key = %s, Value = %s", i, record[0], fmt.Sprintf("%v", record))
		}
	}

	log.Println("CSV data successfully published to Kafka.")
}
