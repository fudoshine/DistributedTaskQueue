package execution

import (
	"bytes"
	"testing"
)

func TestValidateResultRequiredOutput(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	err = catalog.ValidateResult(entry, nil)
	if err == nil {
		t.Fatalf("Expected an error but getting invalide input passed %v", err)
	}
	err = catalog.ValidateResult(entry, []byte{})
	if err != nil {
		t.Fatalf("Expected no error, an empty string is valid output %v", err)
	}
}

func TestNewCatalogRejectsMissingResultValidator(t *testing.T) {
	definition := resultDefinition{
		schema: ResultSchema{
			ContentType:    "application/json",
			Name:           "result_schema_1",
			Version:        1,
			InlineMaxBytes: 512,
			ObjectMaxBytes: 0,
			OutputRequired: true,
		},
		validate: validateEchoResult,
	}
	catalog, err := newCatalog(nil, nil, []resultDefinition{definition})
	if err != nil {
		t.Fatalf("Expected no error, passed correct input %v", err)
	}
	if catalog == nil {
		t.Fatal("With no error, we expect catalog, but getting empty")
	}
	definition.validate = nil

	catalog, err = newCatalog(nil, nil, []resultDefinition{definition})
	if err == nil {
		t.Fatalf("Expected error as result validator is missing from input %v", definition)
	}
	if catalog != nil {
		t.Fatalf("With error, we expect empty catalog %v", catalog)
	}
}

func TestNewCatalogRejectsDulicateResultSchema(t *testing.T) {
	definition := resultDefinition{
		schema: ResultSchema{
			ContentType:    "application/json",
			Name:           "result_schema_1",
			Version:        1,
			InlineMaxBytes: 512,
			ObjectMaxBytes: 0,
			OutputRequired: true,
		},
		validate: validateEchoResult,
	}
	catalog, err := newCatalog(nil, nil, []resultDefinition{definition, definition})
	if err == nil {
		t.Fatalf("Expected error as duplicate result validator is passed in the input %v", definition)
	}
	if catalog != nil {
		t.Fatalf("With error, we expect empty catalog %v", catalog)
	}
}

func TestValidateResultBoundaries(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatalf("Error on initializing contructor to get catalog %v", err)
	}
	entry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("Passed the correct task but got error %v", err)
	}
	limit := int(entry.ResultSchema.InlineMaxBytes)
	testes := []struct {
		name    string
		result  []byte
		wantErr bool
	}{
		{
			name:    "normal test",
			result:  []byte("hello"),
			wantErr: false,
		},
		{
			name:    "Valid text of max byte length",
			result:  bytes.Repeat([]byte("x"), limit),
			wantErr: false,
		},
		{
			name:    "text with more than max byte length",
			result:  bytes.Repeat([]byte("x"), limit+1),
			wantErr: true,
		}, {
			name:    "Invalide text",
			result:  []byte{0xff},
			wantErr: true,
		},
	}
	for _, tt := range testes {
		t.Run(tt.name, func(t *testing.T) {
			err := catalog.ValidateResult(entry, tt.result)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v,wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
