package execution

import (
	"encoding/json"
	"slices"
	"testing"
)

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

func TestNewCatalogRejectsSharedCapabilityStream(t *testing.T) {
	catalogEntry, err := NewCatalog()
	if err != nil {
		t.Fatalf("Something went wrong in initializing contructor of catalog %v", err)
	}
	first, err := catalogEntry.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("Getting error during lookup of correct entry %v", err)
	}
	payload := payloadDefinition{
		schema:   first.PayloadSchema,
		validate: validateEchoPayload,
	}
	result := resultDefinition{
		schema:   first.ResultSchema,
		validate: validateEchoResult,
	}
	second := first
	second.RouteRevision++
	catalog, err := newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	second.CapabilityStream = "queue:reference.echo:v1:r2:stream"
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err != nil {
		t.Fatalf("expected successful construction: %v", err)
	}
	if catalog == nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	second.RouteRevision = first.RouteRevision
	second.PayloadSchema.Version++
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}

	second.PayloadSchema.Version--
	second.ResultSchema.Version++
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	second = first
	second.HandlerVersion = "v2"
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	second.CapabilityStream = "queue:reference.echo:v2:r1:stream"
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err != nil {
		t.Fatalf("expected successful construction: %v", err)
	}
	if catalog == nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}

}
func TestNewCatalogRejectsDuplicateRoute(t *testing.T) {
	catalogEntry, err := NewCatalog()
	if err != nil {
		t.Fatalf("Something went wrong in initializing contructor of catalog %v", err)
	}
	first, err := catalogEntry.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("Getting error during lookup of correct entry %v", err)
	}
	payload := payloadDefinition{
		schema:   first.PayloadSchema,
		validate: validateEchoPayload,
	}
	result := resultDefinition{
		schema:   first.ResultSchema,
		validate: validateEchoResult,
	}
	second := first
	second.CapabilityStream = first.CapabilityStream + ":other"
	catalog, err := newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}

	second.RouteRevision++
	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err != nil {
		t.Fatalf("expected successful construction: %v", err)
	}
	if catalog == nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
}
func TestNewCatalogRequiresNewHandlerForSchemaChange(t *testing.T) {
	catalogEntry, err := NewCatalog()
	if err != nil {
		t.Fatalf("Something went wrong in initializing contructor of catalog %v", err)
	}
	first, err := catalogEntry.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("Getting error during lookup of correct entry %v", err)
	}
	payload := payloadDefinition{
		schema:   first.PayloadSchema,
		validate: validateEchoPayload,
	}
	result := resultDefinition{
		schema:   first.ResultSchema,
		validate: validateEchoResult,
	}
	payloadV2 := payload
	payloadV2.schema.Version++

	second := first
	second.RouteRevision++
	second.CapabilityStream = first.CapabilityStream + ":next"
	second.PayloadSchema = payloadV2.schema

	catalog, err := newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload, payloadV2},
		[]resultDefinition{result},
	)
	if err == nil {
		t.Fatalf("Expected error but got passed %v", catalog)
	}
	if catalog != nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	second.HandlerVersion = "v2"

	catalog, err = newCatalog(
		[]CatalogEntry{first, second},
		[]payloadDefinition{payload, payloadV2},
		[]resultDefinition{result},
	)
	if err != nil {
		t.Fatalf("expected successful construction: %v", err)
	}
	if catalog == nil {
		t.Fatalf("Expected no value when got error but getting value %v", catalog)
	}
	err = catalog.ValidatePayload(
		first,
		json.RawMessage(`{"message":"old job"}`),
	)
	if err != nil {
		t.Fatalf("Expected succesfully contructor intialization but got error %v", err)
	}
}

func TestCatalogRoutes(t *testing.T) {
	catalogEntry, err := NewCatalog()
	if err != nil {
		t.Fatalf("Something went wrong in initializing contructor of catalog %v", err)
	}
	first, err := catalogEntry.Lookup("reference.echo")
	if err != nil {
		t.Fatalf("Getting error during lookup of correct entry %v", err)
	}
	payload := payloadDefinition{
		schema:   first.PayloadSchema,
		validate: validateEchoPayload,
	}
	result := resultDefinition{
		schema:   first.ResultSchema,
		validate: validateEchoResult,
	}
	second := first
	second.RouteRevision++
	second.CapabilityStream = "queue:reference.echo:v1:r2:stream"
	entries := []CatalogEntry{first, second}
	catalog, err := newCatalog(
		entries,
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
	if err != nil {
		t.Fatal(err)
	}

	routes := catalog.Routes()
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	for _, want := range entries {
		if !slices.Contains(routes, want) {
			t.Fatalf("missing revision %d with stream %q",
				want.RouteRevision, want.CapabilityStream)
		}
	}

	routes[0].LeaseTTL = -4
	curRoutes := catalog.Routes()
	if routes[0].LeaseTTL == curRoutes[0].LeaseTTL || slices.Equal(routes, curRoutes) {
		t.Fatalf("Modified returned slice changing subsequent routes")
	}

	got, err := catalog.Lookup("reference.echo")
	if err != nil || got != second {
		t.Fatalf("lookup = %+v, error = %v; want newer revision", got, err)
	}

	entries[0] = CatalogEntry{}
	entries[1] = CatalogEntry{}

	got, err = catalog.Lookup("reference.echo")
	if err != nil || got != second {
		t.Fatalf("lookup = %+v, error = %v; want newer revision", got, err)
	}

	routes = catalog.Routes()
	if len(routes) != 2 {
		t.Fatalf("expected 2 routes, got %d", len(routes))
	}

	if !slices.Equal(routes, []CatalogEntry{first, second}) {
		t.Fatalf("unexpected routes or order: %+v", routes)
	}
}
