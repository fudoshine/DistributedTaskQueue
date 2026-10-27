package repository_test

import (
	"distributed-queue/internal/api/service"
	"distributed-queue/internal/execution"
	"distributed-queue/pkg/repository"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCreateJobRejectsInvalidMetadata(t *testing.T) {
	RequireRepositoryIntegration(t)
	contract := referenceContract(t)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "invalid creation metadata")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*repository.Task)
	}{
		{"missing contract", func(task *repository.Task) { task.ExecutionContract = execution.CatalogEntry{} }},
		{"mismatched task type", func(task *repository.Task) { task.TaskType = "email.send" }},
		{"missing effect key", func(task *repository.Task) { task.EffectKey = uuid.Nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := repository.Task{
				ID: uuid.New(), WorkflowID: workflow.ID, TaskType: contract.TaskType,
				ExecutionContract: contract, EffectKey: uuid.New(),
				Payload: json.RawMessage(`{"message":"hello"}`),
			}
			tc.mutate(&task)
			beforeJobs, beforeOutbox := taskSubmissionRowCounts(t)
			result, err := sharedRepo.CreateJob(t.Context(), "queue:test", task)
			if err == nil || !reflect.DeepEqual(result, repository.JobRow{}) {
				t.Fatalf("invalid metadata returned job=%+v error=%v", result, err)
			}
			afterJobs, afterOutbox := taskSubmissionRowCounts(t)
			if afterJobs != beforeJobs || afterOutbox != beforeOutbox {
				t.Fatal("invalid metadata changed job/outbox counts")
			}
		})
	}
}

func TestCreateJobPinsExecutionContract(t *testing.T) {
	RequireRepositoryIntegration(t)
	original := referenceContract(t)
	revised := original
	revised.RouteRevision++
	revised.CapabilityStream = fmt.Sprintf("queue:%s:%s:r%d:stream", revised.TaskType, revised.HandlerVersion, revised.RouteRevision)
	revised.LeaseTTL += time.Second
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "pinned contracts")
	if err != nil {
		t.Fatal(err)
	}
	var firstID, firstEffectKey uuid.UUID
	for i, contract := range []execution.CatalogEntry{original, revised} {
		effectKey := uuid.New()
		result, err := sharedRepo.CreateJob(t.Context(), "queue:test", repository.Task{
			ID: uuid.New(), WorkflowID: workflow.ID, TaskType: contract.TaskType,
			ExecutionContract: contract, EffectKey: effectKey,
			Payload: json.RawMessage(`{"message":"hello"}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		stored, err := sharedRepo.GetTask(t.Context(), result.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range []repository.JobRow{result, stored} {
			if row.ExecutionContract == nil || *row.ExecutionContract != contract ||
				row.EffectKey == nil || *row.EffectKey != effectKey {
				t.Fatalf("accepted metadata did not round trip: %+v", row)
			}
		}
		if i == 0 {
			firstID, firstEffectKey = result.ID, effectKey
		}
	}
	first, err := sharedRepo.GetTask(t.Context(), firstID)
	if err != nil {
		t.Fatal(err)
	}
	if first.ExecutionContract == nil || *first.ExecutionContract != original ||
		first.EffectKey == nil || *first.EffectKey != firstEffectKey {
		t.Fatalf("creating a new revision changed the earlier job: %+v", first)
	}
}

func TestTaskServicePinsContractAndCreatesDistinctEffectKeys(t *testing.T) {
	RequireRepositoryIntegration(t)
	catalog, err := execution.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := catalog.Lookup("reference.echo")
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewTaskService(sharedRepo, catalog)
	workflow, err := sharedRepo.CreateWorkflow(t.Context(), "service effect identities")
	if err != nil {
		t.Fatal(err)
	}
	var previousEffectKey uuid.UUID
	for range 2 {
		result, err := svc.CreateTask(t.Context(), "queue:test", workflow.ID, nil,
			contract.TaskType, json.RawMessage(`{"message":"hello"}`), 3)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := sharedRepo.GetTask(t.Context(), result.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.ExecutionContract == nil || *stored.ExecutionContract != contract ||
			stored.EffectKey == nil || *stored.EffectKey == uuid.Nil || *stored.EffectKey == previousEffectKey {
			t.Fatalf("missing contract or distinct nonzero effect key: %+v", stored)
		}
		previousEffectKey = *stored.EffectKey
	}
}
