package execution

import (
	"context"
	"encoding/json"
	"testing"
)

func TestCatalogExecuteValidEcho(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}

	result, err := catalog.Execute(
		context.Background(),
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result) != "hello" {
		t.Fatalf("expected result 'hello', got %q", result)
	}
}

func TestCatalogExecuteUnkownTaskType(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	entry.TaskType = "unknown.task"
	_, err = catalog.Execute(
		context.Background(),
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err == nil {
		t.Fatal("expected error for unknown task type")
	}
}

func TestCatalogExecuteUnsupportedHandlerVersion(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	entry.HandlerVersion = "v2"
	_, err = catalog.Execute(
		context.Background(),
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err == nil {
		t.Fatal("expected error for unsupported handler version")
	}
}

func TestCatalogExecuteOversizedPayload(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}

	// This test allows only 3 bytes of input.
	entry.PayloadSchema.MaxBytes = 3

	// Register the same limit so the entry and catalog agree.
	catalog, err = newCatalog(
		[]CatalogEntry{entry},
		[]payloadDefinition{
			{
				schema:   entry.PayloadSchema,
				validate: validateEchoPayload,
			},
		},
		[]resultDefinition{
			{
				schema:   entry.ResultSchema,
				validate: validateEchoResult,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Valid input produces "hello": 5 bytes of output.
	_, err = catalog.Execute(
		context.Background(),
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err == nil || err.Error() != "payload exceeds maximum size" {
		t.Fatalf("expected payload-size error, got %v", err)
	}
}

func TestCatalogExecuteCanceledContext(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = catalog.Execute(
		ctx,
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err == nil || err != context.Canceled {
		t.Fatalf("expected context.Canceled error, got %v", err)
	}
}

func TestCatalogExecuteHanlerOutputExceedsConfiguredResultLimit(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}

	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}

	// This test allows only 3 bytes of output.
	entry.ResultSchema.InlineMaxBytes = 3

	// Register the same limit so the entry and catalog agree.
	catalog, err = newCatalog(
		[]CatalogEntry{entry},
		[]payloadDefinition{
			{
				schema:   entry.PayloadSchema,
				validate: validateEchoPayload,
			},
		},
		[]resultDefinition{
			{
				schema:   entry.ResultSchema,
				validate: validateEchoResult,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := catalog.Execute(
		context.Background(),
		entry,
		json.RawMessage(`{"message":"hello"}`),
	)

	if err == nil || err.Error() != "result exceeds maximum inline size" {
		t.Fatalf("expected result-size error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil result, got %v", result)
	}
}
