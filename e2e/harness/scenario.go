//go:build integration

package harness

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	sandboxv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
)

type Scenario struct {
	t *testing.T
}

func New(t *testing.T) *Scenario {
	if agents == nil {
		t.Fatal("nothing is started; the package's TestMain must call harness.Main")
	}
	return &Scenario{t: t}
}

// Agent starts an agent under the test's name, its model giving replies one
// per call.
func (s *Scenario) Agent(replies ...*agentv1.CallModelOutput) Run {
	s.t.Helper()
	id := s.t.Name()
	model.register(s.t, id, replies)
	run, err := agents.AgentAsync(s.t.Context(), &agentv1.AgentInput{
		Instructions: &agentv1.Instructions{Text: "Use the sh tool."},
		Task:         &agentv1.Task{Scope: id, RelevantAssets: &sandboxv1.Assets{}},
		Tools: &agentv1.Tools{Tools: []*agentv1.Tool{{
			Name: "sh", Description: "Run a shell command",
			InputSchema: `{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`,
		}}},
	}, agentv1.NewAgentOptions().WithID(id))
	if err != nil {
		s.t.Fatal(err)
	}
	return Run{run: run, t: s.t}
}

type Run struct {
	run agentv1.AgentRun
	t   *testing.T
}

// ToolOutputs waits for the run to end and returns its tool outputs, in order.
func (r Run) ToolOutputs() []*sandboxv1.ToolOutput {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.t.Context(), 5*time.Minute)
	defer cancel()
	out, err := r.run.Get(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	var outputs []*sandboxv1.ToolOutput
	for _, m := range out.GetContext().GetMessages() {
		outputs = append(outputs, m.GetToolOutputs()...)
	}
	return outputs
}

func Sh(command string) *agentv1.CallModelOutput {
	input, _ := json.Marshal(map[string]string{"command": command})
	return &agentv1.CallModelOutput{Reply: &agentv1.Message{
		Role: "assistant", ToolCalls: []*sandboxv1.ToolCall{{Name: "sh", Input: string(input)}},
	}}
}

func Done() *agentv1.CallModelOutput {
	return &agentv1.CallModelOutput{Reply: &agentv1.Message{Role: "assistant", Content: "done"}, Done: true}
}
