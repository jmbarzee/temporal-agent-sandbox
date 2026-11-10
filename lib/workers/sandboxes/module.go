package sandboxes

import (
	"io/fs"

	"go.uber.org/fx"

	helm "github.com/metacensus/library-temporal-kube/lib/helm/v1"

	charts "github.com/jmbarzee/temporal-agent-sandbox/helm"
	sandbox "github.com/jmbarzee/temporal-agent-sandbox/lib/sandbox/v1"
)

// Module is the sandbox provider. RenderChart is hosted here so the sandbox
// chart compiles into the same binary as the workflow that names it. Swap
// the helm and kube backed implementation without touching the agent worker.
var Module = fx.Module("sandboxes-worker",
	fx.Provide(func() fs.FS { return charts.FS }),
	helm.Module,
	sandbox.Module,
	fx.Invoke(sandbox.RegisterWorkflows, helm.RegisterActivities),
)
