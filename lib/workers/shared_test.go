package workers_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/agent"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/sandboxes"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/sidecar"
)

// TestModule builds each worker's graph as its cmd/workers main does, without
// running a constructor, so a missing dependency fails here instead of at startup.
func TestModule(t *testing.T) {
	tests := []struct {
		name   string
		worker fx.Option
	}{
		{name: "success - the agent worker's graph resolves", worker: agent.Module},
		{name: "success - the sandboxes worker's graph resolves", worker: sandboxes.Module},
		{name: "success - the sidecar worker's graph resolves", worker: sidecar.Module},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := fx.ValidateApp(workers.Module, tt.worker); err != nil {
				t.Fatal(err)
			}
		})
	}
}
