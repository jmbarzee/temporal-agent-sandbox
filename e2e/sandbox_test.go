//go:build integration

package e2e

import (
	"strings"
	"testing"

	"github.com/jmbarzee/temporal-agent-sandbox/e2e/harness"
)

func TestSandbox_ConfinesCommands(t *testing.T) {
	t.Parallel()
	const command = "id -u; ps"
	output := harness.New(t).Agent(harness.Sh(command), harness.Done()).ToolOutputs()[0].GetOutput()

	tests := []struct {
		name  string
		holds bool
	}{
		{"success - commands run as the chart's sandbox UID", strings.HasPrefix(output, "1000\n")},
		{"success - no sidecar process is visible to commands", !strings.Contains(output, "worker")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.holds {
				t.Errorf("does not hold for %q's output:\n%s", command, output)
			}
		})
	}
}

func TestSandbox_IsolatesWorkspaces(t *testing.T) {
	t.Parallel()
	for _, file := range []string{"alpha", "bravo"} {
		t.Run("success - "+file+" lists only its own file", func(t *testing.T) {
			t.Parallel()
			outputs := harness.New(t).Agent(harness.Sh("touch /workspace/"+file), harness.Sh("ls /workspace"), harness.Done()).ToolOutputs()
			if got := outputs[1].GetOutput(); got != file+"\n" {
				t.Errorf("ls printed %q, want %q", got, file+"\n")
			}
		})
	}
}
