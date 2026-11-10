//go:build integration

// Package harness runs agents through every worker against Temporal and a
// cluster in containers, with a scripted stub in place of the model.
package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"
	"go.uber.org/fx"

	agentv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	sandboxv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/agent"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/infra"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/sandboxes"
)

const (
	// Pinned for reproducible runs.
	temporalImage = "temporalio/temporal:1.5.1"
	k3sImage      = "rancher/k3s:v1.31.4-k3s1"
	namespace     = "agentsandbox"
)

var (
	agents agentv1.AgentsClient
	model  = &stub{scripts: map[string][]*agentv1.CallModelOutput{}}
)

func Main(m *testing.M) {
	code, err := runTests(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "set up:", err)
	}
	os.Exit(code)
}

func runTests(m *testing.M) (code int, err error) {
	sidecarImage := os.Getenv("SIDECAR_IMAGE")
	if sidecarImage == "" {
		return 1, errors.New("SIDECAR_IMAGE is unset; make test-integration builds and sets it")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	temporal, err := testcontainers.Run(ctx, temporalImage,
		testcontainers.WithCmd("server", "start-dev", "--ip", "0.0.0.0", "--namespace", namespace),
		testcontainers.WithExposedPorts("7233/tcp"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"temporal", "operator", "cluster", "health", "--address", "localhost:7233"})),
	)
	defer stop(temporal, &code)
	if err != nil {
		return 1, err
	}
	cluster, err := k3s.Run(ctx, k3sImage)
	defer stop(cluster, &code)
	if err != nil {
		return 1, err
	}
	if err := cluster.LoadImages(ctx, sidecarImage); err != nil {
		return 1, err
	}

	address, err := temporal.PortEndpoint(ctx, "7233/tcp", "")
	if err != nil {
		return 1, err
	}
	// Pods leave the cluster's container by its address, so they reach
	// Temporal at its own rather than at the port published to this host.
	podAddress, err := temporal.ContainerIP(ctx)
	if err != nil {
		return 1, err
	}
	kubeconfig, err := cluster.GetKubeConfig(ctx)
	if err != nil {
		return 1, err
	}
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		return 1, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	kubeconfigPath := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		return 1, err
	}
	server := httptest.NewServer(model)
	defer server.Close()

	for key, value := range map[string]string{
		"ANTHROPIC_BASE_URL":       server.URL,
		"ANTHROPIC_API_KEY":        "stub",
		"KUBECONFIG":               kubeconfigPath,
		"SANDBOX_NAMESPACE":        "default",
		"SANDBOX_SIDECAR_IMAGE":    sidecarImage,
		"SANDBOX_TEMPORAL_ADDRESS": podAddress + ":7233",
	} {
		if err := os.Setenv(key, value); err != nil {
			return 1, err
		}
	}
	for queue, worker := range map[string]fx.Option{"agent": agent.Module, "sandboxes": sandboxes.Module, "infra": infra.Module} {
		app := fx.New(workers.Module, worker, fx.NopLogger, fx.Replace(&config.Config{
			TemporalAddress: address, TemporalNamespace: namespace, TaskQueue: queue, HealthAddress: "127.0.0.1:0",
		}))
		if err := app.Start(ctx); err != nil {
			return 1, err
		}
		defer func() { _ = app.Stop(context.Background()) }()
	}

	c, err := client.Dial(client.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		return 1, err
	}
	defer c.Close()
	if _, err := sandboxv1.NewSandboxesClient(c).SandboxOrchestratorAsync(ctx); err != nil {
		return 1, err
	}
	agents = agentv1.NewAgentsClient(c)
	return m.Run(), nil
}

func stop(c testcontainers.Container, code *int) {
	// Run returns a typed nil when it could not create the container.
	if *code != 0 && !reflect.ValueOf(c).IsNil() {
		printLogs(c)
	}
	_ = testcontainers.TerminateContainer(c)
}

func printLogs(c testcontainers.Container) {
	logs, err := c.Logs(context.Background())
	if err != nil {
		return
	}
	defer func() { _ = logs.Close() }()
	out, _ := io.ReadAll(logs)
	fmt.Fprintf(os.Stderr, "%s logs, last 16 KiB:\n%s\n", c.GetContainerID(), out[max(0, len(out)-16*1024):])
}
