package execution

import "testing"

func TestNewCatalogRejectsMissingPayloadValidator(t *testing.T) {
	definition := payloadDefinition{
		schema: PayloadSchema{
			ContentType: "application/json",
			Name:        "reference.echo",
			Version:     1,
			MaxBytes:    1024,
		},
		validate: nil,
	}
	catalog, err := newCatalog(nil, []payloadDefinition{definition}, nil)
	if err == nil {
		t.Fatal("Invalide input is accepted")
	}
	if catalog != nil {
		t.Fatal("construction should not return a usable catalog after rejection")
	}
	definition.validate = validateEchoPayload
	catalog, err = newCatalog(nil, []payloadDefinition{definition}, nil)
	if err != nil {
		t.Fatal("Valid input is getting rejected")
	}
	if catalog == nil {
		t.Fatal("expected was to get the catalog for correct input, but getting null")
	}
}

func TestNewCatalogRejectsDuplicatePayloadSchema(t *testing.T) {
	definition := payloadDefinition{
		schema: PayloadSchema{
			ContentType: "application/json",
			Name:        "reference.echo",
			Version:     1,
			MaxBytes:    1024,
		},
		validate: validateEchoPayload,
	}
	catalog, err := newCatalog(nil, []payloadDefinition{definition, definition}, nil)
	if err == nil {
		t.Fatalf("Expected error as we passed the invalide input %v", err)
	}
	if catalog != nil {
		t.Fatalf("Expected empty as it was invalide input %v", catalog)
	}
}
