package execution

import (
	"errors"
	"testing"
	"time"
)

func TestRetryBackoff(t *testing.T) {
	tests := []struct {
		name          string
		Initial       time.Duration
		Max           time.Duration
		retryNumber   int
		expectedDelay time.Duration
		expectError   bool
	}{
		{
			name:          "valid backoff",
			Initial:       2 * time.Second,
			Max:           10 * time.Second,
			retryNumber:   1,
			expectedDelay: 2 * time.Second,
			expectError:   false,
		},
		{
			name:          "valid backoff with doubling",
			Initial:       2 * time.Second,
			Max:           10 * time.Second,
			retryNumber:   2,
			expectedDelay: 4 * time.Second,
			expectError:   false,
		},
		{
			name:          "valid backoff with tripling",
			Initial:       2 * time.Second,
			Max:           10 * time.Second,
			retryNumber:   3,
			expectedDelay: 8 * time.Second,
			expectError:   false,
		},
		{
			name:          "valid backoff with max cap",
			Initial:       2 * time.Second,
			Max:           10 * time.Second,
			retryNumber:   4,
			expectedDelay: 10 * time.Second,
			expectError:   false,
		},
		{
			name:          "valid backoff with large number of retries",
			Initial:       2 * time.Second,
			Max:           10 * time.Second,
			retryNumber:   1000000,
			expectedDelay: 10 * time.Second,
			expectError:   false,
		},
		{
			name:        "invalid backoff with negative retry number",
			Initial:     2 * time.Second,
			Max:         10 * time.Second,
			retryNumber: -1,
			expectError: true,
		},
		{
			name:        "invalid backoff with zero initial",
			Initial:     0,
			Max:         10 * time.Second,
			retryNumber: 1,
			expectError: true,
		},
		{
			name:        "invalid backoff with max less than initial",
			Initial:     10 * time.Second,
			Max:         2 * time.Second,
			retryNumber: 1,
			expectError: true,
		},
		{
			name:          "large durations cap without overflow",
			Initial:       time.Duration(1 << 62),
			Max:           time.Duration(1<<63 - 1),
			retryNumber:   2,
			expectedDelay: time.Duration(1<<63 - 1),
			expectError:   false,
		},
		{
			name:        "zero retry number",
			Initial:     2 * time.Second,
			Max:         10 * time.Second,
			retryNumber: 0,
			expectError: true,
		},
		{
			name:        "negative initial duration",
			Initial:     -2 * time.Second,
			Max:         10 * time.Second,
			retryNumber: 1,
			expectError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &RetryBackoff{
				Initial: tt.Initial,
				Max:     tt.Max,
			}
			delay, err := b.Delay(tt.retryNumber)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if delay != tt.expectedDelay {
					t.Errorf("Expected delay %v but got %v", tt.expectedDelay, delay)
				}
			}
		})
	}
}
func TestCatalogEntryValidate(t *testing.T) {
	validEntries := []CatalogEntry{
		{
			TaskType:         "task1",
			HandlerVersion:   "v1",
			CapabilityStream: "stream1",
			ExecutionPool:    "pool1",
			RouteRevision:    1,
			PayloadSchema: PayloadSchema{
				ContentType: "application/json",
				Name:        "payload_schema_1",
				Version:     1,
				MaxBytes:    1024,
			},
			ResultSchema: ResultSchema{
				ContentType:    "application/json",
				Name:           "result_schema_1",
				Version:        1,
				InlineMaxBytes: 512,
				ObjectMaxBytes: 0,
				OutputRequired: true,
			},
			LeaseTTL:           10 * time.Second,
			HeartbeatEvery:     2 * time.Second,
			HandlerTimeout:     5 * time.Second,
			MaxAttemptDuration: 15 * time.Second,
			MaxAttempts:        3,
			MaxJobAge:          1 * time.Hour,
			RetryBackoff: RetryBackoff{
				Initial: 2 * time.Second,
				Max:     10 * time.Second,
			},
			Limits: ConcurrencyLimit{
				TenantMaxConcurrent:   5,
				WorkflowMaxConcurrent: 3,
				ServiceMaxConcurrent:  2,
			},
			EffectStrategy: "none",
		},
		{
			TaskType:         "task2",
			HandlerVersion:   "v2",
			CapabilityStream: "stream2",
			ExecutionPool:    "pool2",
			RouteRevision:    2,
			PayloadSchema: PayloadSchema{
				ContentType: "application/json",
				Name:        "payload_schema_2",
				Version:     2,
				MaxBytes:    2048,
			},
			ResultSchema: ResultSchema{
				ContentType:    "application/json",
				Name:           "result_schema_2",
				Version:        2,
				InlineMaxBytes: 1024,
				ObjectMaxBytes: 0,
				OutputRequired: true,
			},
			LeaseTTL:           10 * time.Second,
			HeartbeatEvery:     2 * time.Second,
			HandlerTimeout:     5 * time.Second,
			MaxAttemptDuration: 15 * time.Second,
			MaxAttempts:        3,
			MaxJobAge:          1 * time.Hour,
			RetryBackoff: RetryBackoff{
				Initial: 2 * time.Second,
				Max:     10 * time.Second,
			},
			Limits: ConcurrencyLimit{
				TenantMaxConcurrent:   5,
				WorkflowMaxConcurrent: 3,
				ServiceMaxConcurrent:  2,
			},
			EffectStrategy: "none",
		},
		{
			TaskType:         "task3",
			HandlerVersion:   "v3",
			CapabilityStream: "stream3",
			ExecutionPool:    "pool3",
			RouteRevision:    3,
			PayloadSchema: PayloadSchema{
				ContentType: "application/json",
				Name:        "payload_schema_3",
				Version:     3,
				MaxBytes:    4096,
			},
			ResultSchema: ResultSchema{
				ContentType:    "application/json",
				Name:           "result_schema_3",
				Version:        3,
				InlineMaxBytes: 2048,
				ObjectMaxBytes: 0,
				OutputRequired: true,
			},
			LeaseTTL:           10 * time.Second,
			HeartbeatEvery:     2 * time.Second,
			HandlerTimeout:     5 * time.Second,
			MaxAttemptDuration: 15 * time.Second,
			MaxAttempts:        3,
			MaxJobAge:          1 * time.Hour,
			RetryBackoff: RetryBackoff{
				Initial: 2 * time.Second,
				Max:     10 * time.Second,
			},
			Limits: ConcurrencyLimit{
				TenantMaxConcurrent:   5,
				WorkflowMaxConcurrent: 3,
				ServiceMaxConcurrent:  2,
			},
			EffectStrategy: "none",
		},
	}
	for _, entry := range validEntries {
		if err := entry.Validate(); err != nil {
			t.Errorf("Expected valid entry to pass validation, but got error: %v", err)
		}
	}

	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.TaskType = ""
		case 1:
			entry.HandlerVersion = ""
		case 2:
			entry.CapabilityStream = ""
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.RouteRevision = 0
		case 1:
			entry.RouteRevision = -1
		case 2:
			entry.PayloadSchema.MaxBytes = 100 * 1024 // Exceeds 64KB
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.ExecutionPool = ""
		case 1:
			entry.PayloadSchema = PayloadSchema{}
		case 2:
			entry.ResultSchema = ResultSchema{}
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.PayloadSchema.ContentType = ""
		case 1:
			entry.PayloadSchema.Name = ""
		case 2:
			entry.PayloadSchema.Version = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.ResultSchema.ContentType = ""
		case 1:
			entry.ResultSchema.Name = ""
		case 2:
			entry.ResultSchema.Version = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.ResultSchema.InlineMaxBytes = 0
		case 1:
			entry.ResultSchema.ObjectMaxBytes = 5
		case 2:
			entry.ResultSchema.OutputRequired = true
			entry.ResultSchema.InlineMaxBytes = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.Limits.TenantMaxConcurrent = 0
		case 1:
			entry.Limits.WorkflowMaxConcurrent = 0
		case 2:
			entry.Limits.ServiceMaxConcurrent = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.RetryBackoff.Initial = entry.RetryBackoff.Max + 1
		case 1:
			entry.RetryBackoff.Max = 0
		case 2:
			entry.LeaseTTL = 3 * entry.HeartbeatEvery
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.MaxAttempts = -1
		case 1:
			entry.HandlerTimeout = entry.MaxAttemptDuration
		case 2:
			entry.HeartbeatEvery = -1
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.MaxJobAge = 0
		case 1:
			entry.MaxAttemptDuration = 0
		case 2:
			entry.MaxAttempts = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
	for idx, entry := range validEntries {
		switch idx {
		case 0:
			entry.EffectStrategy = "invalid"
		case 1:
			entry.EffectStrategy = ""
		case 2:
			entry.RouteRevision = 0
		}
		if err := entry.Validate(); err == nil {
			t.Errorf("Expected invalid entry to fail validation, but it passed")
		}
	}
}

func TestCatalogEntryValidateEffectStrategy(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatalf("Expected catalog to be initialized, but got error: %v", err)
	}
	if err := catalog.entries["reference.echo"].Validate(); err != nil {
		t.Errorf("Expected valid entry to pass validation, but got error: %v", err)
	}
	if _, err := catalog.Lookup("email.send"); !errors.Is(err, ErrUnknownTaskType) {
		t.Errorf("Not expected valid entry to be found in catalog, but got error: %v", err)
	}
	validEntry, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal("Expected entry to be found in catalog, but got nil")
	}
	copyEntry := validEntry
	copyEntry.EffectStrategy = "json:invalid"
	copyEntry.ResultSchema.InlineMaxBytes = 2048
	copyEntry.Limits.TenantMaxConcurrent = 10
	if got, _ := catalog.Lookup("reference.echo"); got != validEntry {
		t.Fatal("modifying a returned entry changed the catalog")
	}
}
