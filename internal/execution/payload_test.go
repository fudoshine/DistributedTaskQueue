package execution

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestValidatePayload(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	messagePayload := func(size int) json.RawMessage {
		return json.RawMessage(`{"message":"` + strings.Repeat("x", size) + `"}`)
	}
	messageLimit := int(entry.PayloadSchema.MaxBytes) - len(`{"message":""}`)
	tests := []struct {
		name    string
		payload json.RawMessage
		modify  func(*CatalogEntry)
		wantErr bool
	}{
		{name: "message", payload: json.RawMessage(`{"message":"hello"}`)},
		{name: "empty string", payload: json.RawMessage(`{"message":""}`)},
		{name: "surrounding whitespace", payload: json.RawMessage(" \n" + `{"message":"hello"}` + "\t ")},
		{name: "unicode string", payload: json.RawMessage(`{"message":"नमस्ते"}`)},
		{name: "exact byte limit", payload: messagePayload(messageLimit)},
		{name: "one byte over limit", payload: messagePayload(messageLimit + 1), wantErr: true},
		{name: "missing payload", wantErr: true},
		{name: "missing message", payload: json.RawMessage(`{}`), wantErr: true},
		{name: "null message", payload: json.RawMessage(`{"message":null}`), wantErr: true},
		{name: "numeric message", payload: json.RawMessage(`{"message":42}`), wantErr: true},
		{name: "unknown field", payload: json.RawMessage(`{"message":"hello","extra":true}`), wantErr: true},
		{name: "wrong field case", payload: json.RawMessage(`{"Message":"hello"}`), wantErr: true},
		{name: "top level null", payload: json.RawMessage(`null`), wantErr: true},
		{name: "array", payload: json.RawMessage(`[]`), wantErr: true},
		{name: "malformed JSON", payload: json.RawMessage(`{"message":`), wantErr: true},
		{name: "trailing JSON", payload: json.RawMessage(`{"message":"hello"}{}`), wantErr: true},
		{name: "invalid UTF8", payload: append(append([]byte(`{"message":"`), 0xff), []byte(`"}`)...), wantErr: true},
		{name: "unsupported task type", payload: json.RawMessage(`{"message":"hello"}`), modify: func(e *CatalogEntry) { e.TaskType = "reference.echo.v1" }, wantErr: true},
		{name: "unsupported handler version", payload: json.RawMessage(`{"message":"hello"}`), modify: func(e *CatalogEntry) { e.HandlerVersion = "v2" }, wantErr: true},
		{name: "unsupported payload name", payload: json.RawMessage(`{"message":"hello"}`), modify: func(e *CatalogEntry) { e.PayloadSchema.Name = "invalid" }, wantErr: true},
		{name: "unsupported payload version", payload: json.RawMessage(`{"message":"hello"}`), modify: func(e *CatalogEntry) { e.PayloadSchema.Version = 2 }, wantErr: true},
		{name: "unsupported content type", payload: json.RawMessage(`{"message":"hello"}`), modify: func(e *CatalogEntry) { e.PayloadSchema.ContentType = "text/plain" }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := entry
			if tt.modify != nil {
				tt.modify(&candidate)
			}
			err := catalog.ValidatePayload(candidate, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidatePayload() error = %v, want error = %v", err, tt.wantErr)
			}
		})
	}
}

func TestReferenceEchoSchemaContract(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	wantPayload := PayloadSchema{ContentType: "application/json", Name: "reference.echo", Version: 1, MaxBytes: 1024}
	if entry.PayloadSchema != wantPayload {
		t.Fatalf("payload schema = %+v, want %+v", entry.PayloadSchema, wantPayload)
	}
	wantResult := ResultSchema{ContentType: "text/plain", Name: "reference.echo", Version: 1, OutputRequired: true, InlineMaxBytes: 1024}
	if entry.ResultSchema != wantResult {
		t.Fatalf("result schema = %+v, want %+v", entry.ResultSchema, wantResult)
	}
	for _, taskType := range []string{"reference.echo.v1", "email.send", "unknown"} {
		if _, err := catalog.Lookup(taskType); !errors.Is(err, ErrUnknownTaskType) {
			t.Errorf("Lookup(%q) error = %v, want ErrUnknownTaskType", taskType, err)
		}
	}
}
