package sandboxes

import (
	"io/fs"

	"go.uber.org/fx"
	"k8s.io/client-go/rest"

	helm "github.com/metacensus/library-temporal-kube/lib/helm/v1"
	kube "github.com/metacensus/library-temporal-kube/lib/kube/v1"

	charts "github.com/jmbarzee/temporal-agent-sandbox/helm"
	sandbox "github.com/jmbarzee/temporal-agent-sandbox/lib/sandbox/v1"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
)

// Module is the sandbox provider, and the only worker that holds the cluster
// credentials. RenderChart is hosted here so the sandbox chart compiles into
// the same binary as the workflow that names it. Swap the helm and kube backed
// implementation without touching the agent worker.
var Module = fx.Module("sandboxes-worker",
	fx.Provide(func() fs.FS { return charts.FS }),
	fx.Provide(config.KubernetesFromEnv, restConfig),
	helm.Module,
	kube.Module(kube.WithFieldManager("agentsandbox")),
	sandbox.Module,
	fx.Invoke(sandbox.RegisterWorkflows, helm.RegisterActivities, kube.RegisterActivities),
)

// restConfig is the kubeconfig KUBECONFIG names, or in-cluster credentials.
func restConfig(cfg *config.KubernetesConfig) (*rest.Config, error) {
	return kube.LoadConfig(cfg.KubeconfigPath, "")
}
