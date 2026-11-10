package agent

import (
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
)

var Module = fx.Module("agent",
	fx.Provide(NewActivities, func() Workflows { return Workflows{} }),
)

func RegisterWorkflows(w worker.Worker, workflows Workflows) {
	pb.RegisterAgentsWorkflows(w, workflows)
}

func RegisterActivities(w worker.Worker, activities *Activities) {
	pb.RegisterAgentsActivities(w, activities)
}
