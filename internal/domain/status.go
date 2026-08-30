package domain

type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "PENDING"
	WorkflowStatusRunning   WorkflowStatus = "RUNNING"
	WorkflowStatusCompleted WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed    WorkflowStatus = "FAILED"
	WorkflowStatusCancelled WorkflowStatus = "CANCELLED"
)

func (s WorkflowStatus) IsValid() bool {
	switch s {
	case WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusRunning, WorkflowStatusCancelled, WorkflowStatusPending:
		return true
	}
	return false
}

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "PENDING"
	TaskStatusQueued    TaskStatus = "QUEUED"
	TaskStatusRunning   TaskStatus = "RUNNING"
	TaskStatusCompleted TaskStatus = "COMPLETED"
	TaskStatusFailed    TaskStatus = "FAILED"
	TaskStatusCancelled TaskStatus = "CANCELLED"
)

func (s TaskStatus) IsValid() bool {
	switch s {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusRunning, TaskStatusCancelled, TaskStatusPending, TaskStatusQueued:
		return true
	}
	return false
}
