package sandbox

import (
	"cmp"
	"errors"
	"os"

	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
)

var Module = fx.Module("sandbox",
	fx.Provide(NewActivities, NewWorkflows),
)

// NewWorkflows reads SANDBOX_*. By default a pod reaches Temporal as this
// worker does, which holds wherever the two share a network.
func NewWorkflows(cfg *config.Config) (Workflows, error) {
	pods := Pods{
		Namespace:         cmp.Or(os.Getenv("SANDBOX_NAMESPACE"), "agentsandbox"),
		SidecarImage:      os.Getenv("SANDBOX_SIDECAR_IMAGE"),
		TemporalAddress:   cmp.Or(os.Getenv("SANDBOX_TEMPORAL_ADDRESS"), cfg.TemporalAddress),
		TemporalNamespace: cfg.TemporalNamespace,
	}
	if pods.SidecarImage == "" {
		return Workflows{}, errors.New("SANDBOX_SIDECAR_IMAGE is required")
	}
	return Workflows{pods: pods}, nil
}

func RegisterWorkflows(w worker.Worker, workflows Workflows) {
	pb.RegisterSandboxesWorkflows(w, workflows)
}

func RegisterActivities(w worker.Worker, activities *Activities) {
	pb.RegisterSandboxesActivities(w, activities)
}
