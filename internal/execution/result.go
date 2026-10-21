package execution

import (
	"errors"
	"unicode/utf8"
)

// A nil result means no output; an empty non-nil slice is an inline result.
func (c *Catalog) ValidateResult(entry CatalogEntry, result []byte) error {
	_, definition, err := c.schemasFor(entry)
	if err != nil {
		return err
	}
	if result == nil {
		if entry.ResultSchema.OutputRequired {
			return errors.New("result is required")
		}
		return nil
	}
	if int64(len(result)) > entry.ResultSchema.InlineMaxBytes {
		return errors.New("result exceeds maximum inline size")
	}
	return definition.validate(result)
}

func validateEchoResult(result []byte) error {
	if !utf8.Valid(result) {
		return errors.New("result is not valid UTF-8 text")
	}
	return nil
}
