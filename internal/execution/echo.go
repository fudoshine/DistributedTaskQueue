package execution

import (
	"context"
	"encoding/json"
)

func ExecuteEchoV1(ctx context.Context, payload json.RawMessage) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := validateEchoPayload(payload); err != nil {
		return nil, err
	}
	var input struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, err
	}
	return []byte(input.Message), nil
}
