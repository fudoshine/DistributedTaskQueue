package v1

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
)

func bindStrictJSON(c *gin.Context, dst any) error {
	decoder := json.NewDecoder(c.Request.Body)
	// Reject JSON fields missing from the Go struct.
	decoder.DisallowUnknownFields()

	// Decode the request into the destination struct.
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON request: %w", err)
	}

	// Ensure the request contains only one JSON value
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("Request must contain exactly one JSON value")
		}
		return fmt.Errorf("unexpected data after json value: %w", err)
	}

	//Run Gin's existing struct validation
	if binding.Validator != nil {
		if err := binding.Validator.ValidateStruct(dst); err != nil {
			return fmt.Errorf("request validation failed: %w", err)
		}
	}

	return nil
}
