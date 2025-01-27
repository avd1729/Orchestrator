package utils

import (
	"errors"
	"orchestrator/pkg/models"
)

func ValidateCSVData(data models.CSVData) error {

	// Feature count validation
	if len(data.Header) != 3 && data.Type != 2 || len(data.Header) != 2 && data.Type == 2 {
		return errors.New("no of columns is not matching")
	}

	// Data entry validation
	for i := 0; i < len(data.Rows); i++ {
		if len(data.Rows[i]) != len(data.Header) {
			return errors.New("no of elements in row does not match the number of features")
		}
	}
	return nil
}
