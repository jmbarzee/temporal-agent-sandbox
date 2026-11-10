package sidecar

import (
	"go.uber.org/fx"

	sandbox "github.com/jmbarzee/temporal-agent-sandbox/lib/sandbox/v1"
)

// Module runs inside each sandbox pod, polling that sandbox's own queue.
var Module = fx.Module("sidecar-worker",
	sandbox.Module,
	fx.Invoke(sandbox.RegisterActivities),
)
