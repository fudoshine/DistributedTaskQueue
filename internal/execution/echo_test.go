package execution

import (
	"context"
	"errors"
	"testing"
)

func TestExecuteEchoV1(t *testing.T) {
	ctx := context.Background()
	payloads := []struct {
		name    string
		payload []byte
		want    []byte
		wantErr bool
	}{
		{
			name:    "valid payload",
			payload: []byte(`{"message": "Hello, World!"}`),
			want:    []byte("Hello, World!"),
			wantErr: false,
		},
		{
			name:    "invalid payload",
			payload: []byte(`{"msg": "Hello, World!"}`),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty message",
			payload: []byte(`{"message": ""}`),
			want:    []byte(""),
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			payload: []byte(`{"message": "42"`),
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range payloads {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecuteEchoV1(ctx, tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteEchoV1() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got == nil {
				t.Fatal("successful echo must return a non-nil result")
			}
			if string(got) != string(tt.want) {
				t.Errorf("ExecuteEchoV1() got = %s, want %s", got, tt.want)
			}
		})
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := ExecuteEchoV1(ctx, []byte(`{"message": "Hello, World!"}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
