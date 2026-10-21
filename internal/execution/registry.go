package execution

import (
	"encoding/json"
	"errors"
	"fmt"
)

type capability struct {
	taskType       string
	handlerVersion string
}

type schemaKey struct {
	contentType string
	name        string
	version     int
}

type taskDefinition struct {
	payload schemaKey
	result  schemaKey
}

type payloadDefinition struct {
	schema   PayloadSchema
	validate func(json.RawMessage) error
}

type resultDefinition struct {
	schema   ResultSchema
	validate func([]byte) error
}

// Entries are ordered oldest to newest; the last entry per task type accepts new jobs.
func newCatalog(entries []CatalogEntry, payloads []payloadDefinition, results []resultDefinition) (*Catalog, error) {
	catalog := &Catalog{
		entries:      make(map[string]CatalogEntry),
		taskTypes:    make(map[capability]taskDefinition),
		payloadTypes: make(map[schemaKey]payloadDefinition),
		resultTypes:  make(map[schemaKey]resultDefinition),
	}
	for _, definition := range payloads {
		schema := definition.schema
		key := schemaKey{schema.ContentType, schema.Name, schema.Version}
		if definition.validate == nil {
			return nil, fmt.Errorf("payload schema %q requires a validator", schema.Name)
		}
		if _, exists := catalog.payloadTypes[key]; exists {
			return nil, fmt.Errorf("duplicate payload schema %q version %d", schema.Name, schema.Version)
		}
		catalog.payloadTypes[key] = definition
	}
	for _, definition := range results {
		schema := definition.schema
		key := schemaKey{schema.ContentType, schema.Name, schema.Version}
		if definition.validate == nil {
			return nil, fmt.Errorf("result schema %q requires a validator", schema.Name)
		}
		if _, exists := catalog.resultTypes[key]; exists {
			return nil, fmt.Errorf("duplicate result schema %q version %d", schema.Name, schema.Version)
		}
		catalog.resultTypes[key] = definition
	}
	seenRoutes := make(map[struct {
		capability
		revision int
	}]bool)
	seenStreams := make(map[string]bool)
	for _, entry := range entries {
		if err := entry.Validate(); err != nil {
			return nil, fmt.Errorf("task %q: %w", entry.TaskType, err)
		}
		payloadKey := schemaKey{entry.PayloadSchema.ContentType, entry.PayloadSchema.Name, entry.PayloadSchema.Version}
		payload, exists := catalog.payloadTypes[payloadKey]
		if !exists || payload.schema != entry.PayloadSchema {
			return nil, fmt.Errorf("task %q references an unregistered payload schema", entry.TaskType)
		}
		resultKey := schemaKey{entry.ResultSchema.ContentType, entry.ResultSchema.Name, entry.ResultSchema.Version}
		result, exists := catalog.resultTypes[resultKey]
		if !exists || result.schema != entry.ResultSchema {
			return nil, fmt.Errorf("task %q references an unregistered result schema", entry.TaskType)
		}
		key := capability{entry.TaskType, entry.HandlerVersion}
		definition := taskDefinition{payload: payloadKey, result: resultKey}
		if previous, exists := catalog.taskTypes[key]; exists && previous != definition {
			return nil, fmt.Errorf("task %q must use a new handler version to change schemas", entry.TaskType)
		}
		route := struct {
			capability
			revision int
		}{key, entry.RouteRevision}
		if seenRoutes[route] {
			return nil, fmt.Errorf("duplicate task %q handler %q route revision %d", entry.TaskType, entry.HandlerVersion, entry.RouteRevision)
		}
		if seenStreams[entry.CapabilityStream] {
			return nil, fmt.Errorf(
				"capability stream %q is assigned to multiple routes",
				entry.CapabilityStream,
			)
		}
		seenStreams[entry.CapabilityStream] = true
		seenRoutes[route] = true
		catalog.taskTypes[key] = definition
		catalog.entries[entry.TaskType] = entry
	}
	return catalog, nil
}

func (c *Catalog) schemasFor(entry CatalogEntry) (payloadDefinition, resultDefinition, error) {
	task, exists := c.taskTypes[capability{entry.TaskType, entry.HandlerVersion}]
	if !exists {
		return payloadDefinition{}, resultDefinition{}, ErrUnknownTaskType
	}
	payload := c.payloadTypes[task.payload]
	result := c.resultTypes[task.result]
	if entry.PayloadSchema != payload.schema || entry.ResultSchema != result.schema {
		return payloadDefinition{}, resultDefinition{}, errors.New("schemas do not match the registered task capability")
	}
	return payload, result, nil
}
