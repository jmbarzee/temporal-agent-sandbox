package agent

import (
	"go.uber.org/fx"

	agent "github.com/jmbarzee/temporal-agent-sandbox/lib/agent/v1"
)

// Module is the agent worker. It never shares a binary with sandbox
// orchestration; its only interface to sandboxes is the sandbox proto.
var Module = fx.Module("agent-worker",
	agent.Module,
	fx.Invoke(agent.RegisterWorkflows, agent.RegisterActivities),
)
