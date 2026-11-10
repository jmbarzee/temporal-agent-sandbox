//go:build integration

package harness

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	agentv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
)

// stub answers as the Messages API would.
type stub struct {
	mu      sync.Mutex
	scripts map[string][]*agentv1.CallModelOutput
}

func (s *stub) register(t *testing.T, task string, replies []*agentv1.CallModelOutput) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.scripts[task]; ok {
		t.Fatalf("%q already runs an agent; start another in a subtest", task)
	}
	s.scripts[task] = replies
	t.Cleanup(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.scripts, task)
	})
}

func (s *stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 || len(req.Messages[0].Content) == 0 {
		http.Error(w, "the request holds no task", http.StatusBadRequest)
		return
	}
	task, replies := req.Messages[0].Content[0].Text, 0
	for _, m := range req.Messages {
		if m.Role == "assistant" {
			replies++
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	script, ok := s.scripts[task]
	if !ok {
		http.Error(w, fmt.Sprintf("no script is registered for %q", task), http.StatusBadRequest)
		return
	}
	if replies >= len(script) {
		http.Error(w, fmt.Sprintf("the model was called after the script's %d replies", len(script)), http.StatusBadRequest)
		return
	}
	reply := script[replies].GetReply()
	content, stopReason := []any{}, "end_turn"
	if reply.GetContent() != "" {
		content = append(content, map[string]string{"type": "text", "text": reply.GetContent()})
	}
	for i, call := range reply.GetToolCalls() {
		content = append(content, map[string]any{
			"type": "tool_use", "id": fmt.Sprintf("toolu_%d_%d", replies, i), "name": call.GetName(), "input": json.RawMessage(call.GetInput()),
		})
		stopReason = "tool_use"
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"content": content, "stop_reason": stopReason})
}
