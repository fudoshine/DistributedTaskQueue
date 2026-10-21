package execution

import (
	"errors"
	"math/bits"
	"strings"
	"time"
)

const (
	referenceEchoTaskType       = "reference.echo"
	referenceEchoHandlerVersion = "v1"
)

var (
	ErrUnknownTaskType = errors.New("unknown task type")
)

// Catalog represents a collection of items.

type PayloadSchema struct {
	ContentType string `json:"content_type"`
	Name        string `json:"name"`
	Version     int    `json:"version"`
	MaxBytes    int64  `json:"max_bytes"`
}

type ResultSchema struct {
	ContentType    string `json:"content_type"`
	Name           string `json:"name"`
	Version        int    `json:"version"`
	OutputRequired bool   `json:"output_required"`
	InlineMaxBytes int64  `json:"inline_max_bytes"`
	ObjectMaxBytes int64  `json:"object_max_bytes"`
}

type CatalogEntry struct {
	TaskType           string           `json:"task_type"`
	HandlerVersion     string           `json:"handler_version"`
	RouteRevision      int              `json:"route_revision"`
	CapabilityStream   string           `json:"capability_stream"`
	ExecutionPool      string           `json:"execution_pool"`
	PayloadSchema      PayloadSchema    `json:"payload_schema"`
	ResultSchema       ResultSchema     `json:"result_schema"`
	LeaseTTL           time.Duration    `json:"lease_ttl"`            //How long the worker’s permission to own this attempt lasts without renewal
	HeartbeatEvery     time.Duration    `json:"heartbeat_every"`      //How often the worker renews ownership
	HandlerTimeout     time.Duration    `json:"handler_timeout"`      // How much time the handler gets to do its work
	MaxAttemptDuration time.Duration    `json:"max_attempt_duration"` // Total time allowed for one attempt, including finishing and recording its output
	MaxAttempts        int              `json:"max_attempts"`         // Maximum number of times a job can be attempted
	MaxJobAge          time.Duration    `json:"max_job_age"`          // Total lifetime allowed after workflow seal, including waiting, attempts, and retry delays
	RetryBackoff       RetryBackoff     `json:"retry_backoff"`        // Backoff strategy for retries
	Limits             ConcurrencyLimit `json:"limits"`               // Concurrency limits for the task
	// Our initial reference.echo handler has no external business effect, so we’ll use "none"
	EffectStrategy string `json:"effect_strategy"` // records how repeated execution avoids repeating a business effect.
}

// Our rule will be exponential backoff: start at Initial, double after each failure, and stop increasing at Max.
type RetryBackoff struct {
	Initial time.Duration `json:"initial"`
	Max     time.Duration `json:"max"`
}

var ErrInvalidBackoff = errors.New("invalid backoff configuration")

func (b *RetryBackoff) Delay(retryNumber int) (time.Duration, error) {
	if retryNumber < 1 || b.Initial <= 0 || b.Max < b.Initial {
		return 0, ErrInvalidBackoff
	}
	shift := retryNumber - 1
	if shift >= bits.Len64(uint64(b.Max/b.Initial)) {
		return b.Max, nil
	}
	return b.Initial << time.Duration(shift), nil
}

type ConcurrencyLimit struct {
	TenantMaxConcurrent   int `json:"tenant_max_concurrent"`
	WorkflowMaxConcurrent int `json:"workflow_max_concurrent"`
	ServiceMaxConcurrent  int `json:"service_max_concurrent"`
}

// Validates server configuration, not a submitted job's payload.
func (e CatalogEntry) Validate() error {
	if strings.TrimSpace(e.TaskType) == "" || strings.TrimSpace(e.HandlerVersion) == "" || strings.TrimSpace(e.CapabilityStream) == "" || strings.TrimSpace(e.ExecutionPool) == "" {
		return errors.New("task_type, handler_version, capability_stream, and execution_pool are required")
	}
	if e.RouteRevision <= 0 {
		return errors.New("route_revision must be a positive integer")
	}

	if strings.TrimSpace(e.PayloadSchema.ContentType) == "" || strings.TrimSpace(e.PayloadSchema.Name) == "" || e.PayloadSchema.Version <= 0 || e.PayloadSchema.MaxBytes <= 0 || e.PayloadSchema.MaxBytes > (64*1024) {
		return errors.New("payload_schema fields are required and must be valid")
	}
	if strings.TrimSpace(e.ResultSchema.ContentType) == "" || strings.TrimSpace(e.ResultSchema.Name) == "" || e.ResultSchema.Version <= 0 || e.ResultSchema.InlineMaxBytes < 0 || e.ResultSchema.ObjectMaxBytes != 0 || e.ResultSchema.InlineMaxBytes > (64*1024) {
		return errors.New("result_schema fields are required and must be valid")
	}
	if e.ResultSchema.OutputRequired && e.ResultSchema.InlineMaxBytes <= 0 && e.ResultSchema.ObjectMaxBytes <= 0 {
		return errors.New("result_schema must have at least one of inline_max_bytes or object_max_bytes set if output_required is true")
	}

	if e.LeaseTTL <= 0 || e.HeartbeatEvery <= 0 || e.HandlerTimeout <= 0 || e.MaxAttemptDuration <= 0 || e.MaxAttempts <= 0 || e.MaxJobAge <= 0 {
		return errors.New("lease_ttl, heartbeat_every, handler_timeout, max_attempt_duration, max_attempts, and max_job_age must be positive durations")
	}
	if e.HeartbeatEvery > (e.LeaseTTL-1)/3 {
		return errors.New("lease_ttl must exceed three heartbeat intervals")
	}

	if e.HandlerTimeout >= e.MaxAttemptDuration {
		return errors.New("handler_timeout must be shorter than max_attempt_duration")
	}
	if _, err := e.RetryBackoff.Delay(1); err != nil {
		return err
	}
	if e.Limits.TenantMaxConcurrent <= 0 || e.Limits.WorkflowMaxConcurrent <= 0 || e.Limits.ServiceMaxConcurrent <= 0 {
		return errors.New("concurrency limits must be positive integers")
	}
	if strings.TrimSpace(e.EffectStrategy) != "none" {
		return errors.New("effect_strategy must be one of 'none'")
	}
	return nil
}

type Catalog struct {
	entries      map[string]CatalogEntry
	taskTypes    map[capability]taskDefinition
	payloadTypes map[schemaKey]payloadDefinition
	resultTypes  map[schemaKey]resultDefinition
}

func NewCatalog() (*Catalog, error) {
	payload := payloadDefinition{
		schema:   PayloadSchema{ContentType: "application/json", Name: referenceEchoTaskType, Version: 1, MaxBytes: 1024},
		validate: validateEchoPayload,
	}
	result := resultDefinition{
		schema:   ResultSchema{ContentType: "text/plain", Name: referenceEchoTaskType, Version: 1, OutputRequired: true, InlineMaxBytes: 1024},
		validate: validateEchoResult,
	}
	return newCatalog(
		[]CatalogEntry{
			{
				TaskType:           referenceEchoTaskType,
				HandlerVersion:     referenceEchoHandlerVersion,
				RouteRevision:      1,
				CapabilityStream:   "queue:reference.echo:v1:r1:stream",
				ExecutionPool:      "reference",
				PayloadSchema:      payload.schema,
				ResultSchema:       result.schema,
				LeaseTTL:           20 * time.Second,
				HeartbeatEvery:     5 * time.Second,
				HandlerTimeout:     20 * time.Second,
				MaxAttemptDuration: 30 * time.Second,
				MaxAttempts:        3,
				MaxJobAge:          1 * time.Hour,
				RetryBackoff: RetryBackoff{
					Initial: 2 * time.Second,
					Max:     10 * time.Second,
				},
				Limits: ConcurrencyLimit{
					TenantMaxConcurrent:   5,
					WorkflowMaxConcurrent: 3,
					ServiceMaxConcurrent:  5,
				},
				EffectStrategy: "none",
			},
		},
		[]payloadDefinition{payload},
		[]resultDefinition{result},
	)
}

func (c *Catalog) Lookup(taskType string) (CatalogEntry, error) {
	if entry, ok := c.entries[taskType]; ok {
		return entry, nil
	}
	return CatalogEntry{}, ErrUnknownTaskType
}
