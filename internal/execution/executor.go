package execution

import (
	"context"
	"encoding/json"
	"fmt"
)

func (c *Catalog) Execute(
	ctx context.Context,
	entry CatalogEntry,
	payload json.RawMessage,
) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := entry.Validate(); err != nil {
		return nil, err
	}
	if err := c.ValidatePayload(entry, payload); err != nil {
		return nil, err
	}
	if entry.TaskType != referenceEchoTaskType ||
		entry.HandlerVersion != referenceEchoHandlerVersion {
		return nil, fmt.Errorf(
			"unsupported handler for task %q version %q",
			entry.TaskType,
			entry.HandlerVersion,
		)
	}
	result, err := ExecuteEchoV1(ctx, payload)
	if err != nil {
		return nil, err
	}
	err = c.ValidateResult(entry, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
