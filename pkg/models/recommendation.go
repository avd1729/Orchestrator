package models

// Recommendation represents a recommendation message structure
type Recommendation struct {
	UserID string  `json:"user_id"`
	SongID string  `json:"song_id"`
	Score  float64 `json:"score"`
}
