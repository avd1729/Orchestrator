package models

import "orchestrator/pkg/enums"

type CSVData struct {
	Type   enums.FileType
	Header []string
	Rows   [][]string
}
