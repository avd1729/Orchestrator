// main.go

package main

import (
	"fmt"
	"net/http"
	"orchestrator/internal/handlers" // Import the handler for CSV upload
)

func main() {

	// Handle the /upload endpoint for CSV upload
	http.HandleFunc("/upload", handlers.UploadHandler)

	// Start the HTTP server
	fmt.Println("Server started at http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}
