package agent

import (
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/worker"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
)

// Histories come from real runs: `temporal workflow show --workflow-id <id> --output json`.
func TestAgentsWorkflows_Replay(t *testing.T) {
	files, err := filepath.Glob("testdata/replay/*/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Skip("no recorded histories under testdata/replay")
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			pb.RegisterAgentsWorkflows(replayer, Workflows{})
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, file); err != nil {
				t.Fatal(err)
			}
		})
	}
}
