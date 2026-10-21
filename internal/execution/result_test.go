package execution

import (
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
