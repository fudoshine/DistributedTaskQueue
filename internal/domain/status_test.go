package domain

import "testing"

func TestValidWorkflowStatus(t *testing.T) {
	status := []WorkflowStatus{
		WorkflowStatusRunning,
		WorkflowStatusPending,
		WorkflowStatusFailed,
		WorkflowStatusCompleted,
		WorkflowStatusCancelled,
		WorkflowStatusDraft,
	}
	for _, st := range status {
		if !st.IsValid() {
			t.Fatalf("Getting invalid for valid status %v", st)
		}
	}
}

func TestInvalidWorkflowStatus(t *testing.T) {
	status := []WorkflowStatus{
		"",
		"Hello world",
	}
	for _, st := range status {
		if st.IsValid() {
			t.Fatalf("Getting Valid for invalid status %v", st)
		}
	}
}
