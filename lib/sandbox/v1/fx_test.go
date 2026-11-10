package sandbox

import (
	"strings"
	"testing"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
)

func TestNewWorkflows(t *testing.T) {
	worker := &config.Config{TemporalAddress: "temporal:7233", TemporalNamespace: "tns"}
	tests := []struct {
		name     string
		env      map[string]string
		wantPods Pods
		wantErr  string
	}{
		{
			name:    "error - no sidecar image",
			env:     map[string]string{},
			wantErr: "SANDBOX_SIDECAR_IMAGE",
		},
		{
			name:     "success - a pod reaches Temporal as the worker does by default",
			env:      map[string]string{"SANDBOX_SIDECAR_IMAGE": "sidecar:1"},
			wantPods: Pods{Namespace: "agentsandbox", SidecarImage: "sidecar:1", TemporalAddress: "temporal:7233", TemporalNamespace: "tns"},
		},
		{
			name: "success - SANDBOX_* overrides the defaults",
			env: map[string]string{
				"SANDBOX_SIDECAR_IMAGE":    "sidecar:1",
				"SANDBOX_NAMESPACE":        "ns-1",
				"SANDBOX_TEMPORAL_ADDRESS": "host:7233",
			},
			wantPods: Pods{Namespace: "ns-1", SidecarImage: "sidecar:1", TemporalAddress: "host:7233", TemporalNamespace: "tns"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{"SANDBOX_SIDECAR_IMAGE", "SANDBOX_NAMESPACE", "SANDBOX_TEMPORAL_ADDRESS"} {
				t.Setenv(key, tt.env[key])
			}
			got, err := NewWorkflows(worker)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.pods != tt.wantPods {
				t.Errorf("got %+v, want %+v", got.pods, tt.wantPods)
			}
		})
	}
}
