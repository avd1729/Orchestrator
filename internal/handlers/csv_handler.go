package handlers

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"orchestrator/internal/kafka"
	"orchestrator/internal/utils"
	"orchestrator/pkg/enums"
	"orchestrator/pkg/models"
)

// UploadHandler handles the CSV upload and validation
func UploadHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the incoming form with file
	err := r.ParseMultipartForm(10 << 20) // Max file size: 10 MB
	if err != nil {
		http.Error(w, "Unable to parse form", http.StatusBadRequest)
		return
	}

	// Retrieve the file from the form-data
	file, _, err := r.FormFile("csvfile")
	if err != nil {
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Read the CSV file content
	reader := csv.NewReader(file)

	// Read the header
	header, err := reader.Read()
	if err != nil {
		http.Error(w, "Error reading CSV header", http.StatusInternalServerError)
		return
	}

	// Read all rows
	rows, err := reader.ReadAll()
	if err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Error reading CSV rows", http.StatusInternalServerError)
		return
	}

	// Retrieve the file type from the form-data
	fileType := r.FormValue("fileType")
	var enumType enums.FileType
	switch fileType {
	case "0":
		enumType = enums.FileType(0)
	case "1":
		enumType = enums.FileType(1)
	case "2":
		enumType = enums.FileType(2)
	default:
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	}

	// Create CSVData instance
	data := models.CSVData{
		Type:   enumType,
		Header: header,
		Rows:   rows,
	}

	// Validate CSV data
	err = utils.ValidateCSVData(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Validation failed: %v", err), http.StatusBadRequest)
		return
	}

	// Respond with success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("CSV file validated successfully!"))

	// Reset the file pointer before passing to Kafka
	if _, err := file.Seek(0, 0); err != nil {
		http.Error(w, "Failed to reset file pointer", http.StatusInternalServerError)
		return
	}

	kafkaBroker := "localhost:9092"
	topic := "raw-csv-topic"
	kafka.ProduceCSVToKafka(file, kafkaBroker, topic)

}
