// internal/handlers/upload_handler.go

package handlers

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"orchestrator/internal/kafka/consumers"
	"orchestrator/internal/kafka/producers"
	"orchestrator/internal/utils"
	"orchestrator/pkg/enums"
	"orchestrator/pkg/models"
)

// UploadHandler handles the CSV upload and validation for three CSV files (Users, Songs, Edges)
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the incoming form with file
	err := r.ParseMultipartForm(10 << 20) // Max file size: 10 MB
	if err != nil {
		http.Error(w, "Unable to parse form", http.StatusBadRequest)
		return
	}

	// Retrieve the files from the form-data
	usersFile, _, err := r.FormFile("usersfile")
	if err != nil {
		http.Error(w, "Error retrieving users file", http.StatusBadRequest)
		return
	}
	defer usersFile.Close()

	songsFile, _, err := r.FormFile("songsfile")
	if err != nil {
		http.Error(w, "Error retrieving songs file", http.StatusBadRequest)
		return
	}
	defer songsFile.Close()

	edgesFile, _, err := r.FormFile("edgesfile")
	if err != nil {
		http.Error(w, "Error retrieving edges file", http.StatusBadRequest)
		return
	}
	defer edgesFile.Close()

	// Read the CSV content for Users, Songs, and Edges
	usersReader := csv.NewReader(usersFile)
	usersHeader, err := usersReader.Read()
	if err != nil {
		http.Error(w, "Error reading users CSV header", http.StatusInternalServerError)
		return
	}
	usersRows, err := usersReader.ReadAll()
	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Error reading users CSV rows", http.StatusInternalServerError)
		return
	}

	songsReader := csv.NewReader(songsFile)
	songsHeader, err := songsReader.Read()
	if err != nil {
		http.Error(w, "Error reading songs CSV header", http.StatusInternalServerError)
		return
	}
	songsRows, err := songsReader.ReadAll()
	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Error reading songs CSV rows", http.StatusInternalServerError)
		return
	}

	edgesReader := csv.NewReader(edgesFile)
	edgesHeader, err := edgesReader.Read()
	if err != nil {
		http.Error(w, "Error reading edges CSV header", http.StatusInternalServerError)
		return
	}
	edgesRows, err := edgesReader.ReadAll()
	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Error reading edges CSV rows", http.StatusInternalServerError)
		return
	}

	// Create CSVData instances for each file
	usersData := models.CSVData{
		Type:   enums.FileType(0), // Assuming '0' for Users file type
		Header: usersHeader,
		Rows:   usersRows,
	}
	songsData := models.CSVData{
		Type:   enums.FileType(1), // Assuming '1' for Songs file type
		Header: songsHeader,
		Rows:   songsRows,
	}
	edgesData := models.CSVData{
		Type:   enums.FileType(2), // Assuming '2' for Edges file type
		Header: edgesHeader,
		Rows:   edgesRows,
	}

	// Validate CSV data
	err = utils.ValidateCSVData(usersData)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed for users: %v", err), http.StatusBadRequest)
		return
	}
	err = utils.ValidateCSVData(songsData)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed for songs: %v", err), http.StatusBadRequest)
		return
	}
	err = utils.ValidateCSVData(edgesData)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed for edges: %v", err), http.StatusBadRequest)
		return
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("CSV files validated successfully!"))

	// Reset the file pointers before passing to Kafka
	if _, err := usersFile.Seek(0, 0); err != nil {
		http.Error(w, "Failed to reset users file pointer", http.StatusInternalServerError)
		return
	}
	if _, err := songsFile.Seek(0, 0); err != nil {
		http.Error(w, "Failed to reset songs file pointer", http.StatusInternalServerError)
		return
	}
	if _, err := edgesFile.Seek(0, 0); err != nil {
		http.Error(w, "Failed to reset edges file pointer", http.StatusInternalServerError)
		return
	}

	kafkaBroker := "localhost:9092"
	// Produce CSV files to respective Kafka topics
	producers.ProduceCSVToKafka(usersFile, kafkaBroker, "users-topic", string(enums.FileType(0)))
	producers.ProduceCSVToKafka(songsFile, kafkaBroker, "songs-topic", string(enums.FileType(1)))
	producers.ProduceCSVToKafka(edgesFile, kafkaBroker, "edges-topic", string(enums.FileType(2)))

	// Consume Kafka messages with 1 second polling interval
	ctx := context.Background()
	messages, err := consumers.KafkaConsumer(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error consuming Kafka messages: %v", err), http.StatusInternalServerError)
		return
	}

	// Send the consumed messages as part of the HTTP response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	for _, message := range messages {
		// Send each message back to the UI (front-end)
		w.Write([]byte(message)) // You can format the response as needed (e.g., JSON)
		w.Write([]byte("\n"))    // Adding newline for better separation of messages
	}
}
