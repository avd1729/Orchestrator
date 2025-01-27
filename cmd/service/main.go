package main

import (
	"fmt"
	"net/http"
	"orchestrator/internal/handlers"
)

func main() {
	http.HandleFunc("/upload", handlers.UploadHandler)

	fmt.Println("Server started at http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}

}
