package execution

import (
	"encoding/json"
	"errors"
	"unicode/utf8"
)

func (c *Catalog) ValidatePayload(entry CatalogEntry, payload json.RawMessage) error {
	definition, _, err := c.schemasFor(entry)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return errors.New("payload is required")
	}
	if int64(len(payload)) > entry.PayloadSchema.MaxBytes {
		return errors.New("payload exceeds maximum size")
	}
	return definition.validate(payload)
}

func validateEchoPayload(payload json.RawMessage) error {
	if !utf8.Valid(payload) {
		return errors.New("payload is not valid UTF-8")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return errors.New("payload is not a valid JSON object")
	}
	rawMessage, ok := fields["message"]
	if !ok {
		return errors.New("payload must contain a 'message' field")
	}
	if len(fields) != 1 {
		return errors.New("payload must only contain a 'message' field")
	}
	var message *string
	if err := json.Unmarshal(rawMessage, &message); err != nil || message == nil {
		return errors.New("message field must be a string")
	}
	return nil
}
