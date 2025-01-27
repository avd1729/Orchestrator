package producers

import (
	"bufio"
	"context"
	"encoding/csv"
	"log"
	"mime/multipart"
	"strings"

	"github.com/segmentio/kafka-go"
)

// ProduceCSVToKafka uploads the entire CSV file content as a single key-value pair to Kafka
func ProduceCSVToKafka(csvFile multipart.File, kafkaBroker, topic, key string) {
	// Create a CSV reader to read from the multipart file
	reader := csv.NewReader(bufio.NewReader(csvFile))

	// Read all the records from the CSV
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %v", err)
	}

	// Convert all records into a single CSV string
	var csvBuilder strings.Builder
	for _, record := range records {
		csvBuilder.WriteString(strings.Join(record, ","))
		csvBuilder.WriteString("\n") // Add a newline after each row
	}

	csvContent := csvBuilder.String()

	// Initialize Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// Create the Kafka message with the entire CSV content
	message := kafka.Message{
		Key:   []byte(key), // Use the provided key
		Value: []byte(csvContent),
	}

	// Send the message to Kafka
	err = writer.WriteMessages(context.Background(), message)
	if err != nil {
		log.Fatalf("Failed to write message to Kafka: %v", err)
	}

	log.Println("CSV data successfully published to Kafka")
}
