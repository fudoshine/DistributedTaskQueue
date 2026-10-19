package execution

// Catalog represents a collection of items.

type PayloadSchema struct {
	ContentType string `json:"content_type"`
	Name        string `json:"name"`
	Version     int    `json:"version"`
	MaxBytes    int64  `json:"max_bytes"`
}

type ResultSchema struct {
	ContentType    string `json:"content_type"`
	Name           string `json:"name"`
	Version        int    `json:"version"`
	OutputRequired bool   `json:"output_required"`
	InlineMaxBytes int64  `json:"inline_max_bytes"`
	ObjectMaxBytes int64  `json:"object_max_bytes"`
}
