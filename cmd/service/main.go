package main

import (
	"encoding/csv"
	"fmt"
	"orchestrator/internal/validators"
	"orchestrator/pkg/enums"
	"orchestrator/pkg/models"
	"os"
)

func main() {
	// Specify the path to your CSV file
	filePath := "song_features.csv"

	// Open the CSV file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	// Read the CSV file
	reader := csv.NewReader(file)

	// Read the header row
	header, err := reader.Read()
	if err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}

	// Read all rows (data rows)
	rows, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading rows: %v\n", err)
		return
	}

	// Specify the file type (for testing purposes, modify as needed)
	var fileType enums.FileType
	fmt.Println("Enter the file type (0, 1, or 2):")
	_, err = fmt.Scan(&fileType)
	if err != nil {
		fmt.Printf("Invalid input for file type: %v\n", err)
		return
	}

	// Create a CSVData instance
	data := models.CSVData{
		Type:   fileType,
		Header: header,
		Rows:   rows,
	}

	// Validate the CSVData
	err = validators.ValidateCSVData(data)
	if err != nil {
		fmt.Printf("Validation failed: %v\n", err)
	} else {
		fmt.Println("Validation successful! The CSV data is valid.")
	}
}
