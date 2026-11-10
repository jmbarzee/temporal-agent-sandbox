package infra

import (
	"go.uber.org/fx"
	"k8s.io/client-go/rest"

	kube "github.com/metacensus/library-temporal-kube/lib/kube/v1"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
)

// Module is the only worker that holds the cluster credentials.
var Module = fx.Module("infra-worker",
	fx.Provide(config.KubernetesFromEnv, restConfig),
	kube.Module(kube.WithFieldManager("agentsandbox")),
	fx.Invoke(kube.RegisterActivities),
)

// restConfig is the kubeconfig KUBECONFIG names, or in-cluster credentials.
func restConfig(cfg *config.KubernetesConfig) (*rest.Config, error) {
	return kube.LoadConfig(cfg.KubeconfigPath, "")
}
