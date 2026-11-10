package sandbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"

	helmv1 "github.com/metacensus/library-temporal-kube/gen/helm/v1"
	kubev1 "github.com/metacensus/library-temporal-kube/gen/kube/v1"
	helm "github.com/metacensus/library-temporal-kube/lib/helm/v1"
	helmmocks "github.com/metacensus/library-temporal-kube/mocks/helm/v1"
	kubemocks "github.com/metacensus/library-temporal-kube/mocks/kube/v1"

	agentv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	charts "github.com/jmbarzee/temporal-agent-sandbox/helm"
)

const podManifest = `apiVersion: v1
kind: Pod
metadata:
  name: sandbox-sb-1
  namespace: agentsandbox
`

// errCluster is non-retryable so a failing activity is called exactly once.
var errCluster = temporal.NewNonRetryableApplicationError("cluster unavailable", "ClusterError", nil)

var errRequesterGone = &temporal.UnknownExternalWorkflowExecutionError{}

func identity(workflowID string) *pb.Identity {
	return &pb.Identity{WorkflowId: workflowID}
}

func TestSandboxOrchestratorWorkflow_Execute(t *testing.T) {
	assets := &pb.Assets{Assets: []*pb.Asset{{Repo: "example/repo"}}}
	tests := []struct {
		name         string
		gone         string
		wantAssigned int
	}{
		{name: "success - every request starts a sandbox and assigns it", wantAssigned: requestsPerRun},
		{name: "success - a requester gone leaves the orchestrator serving", gone: "agent-0", wantAssigned: requestsPerRun - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			client := pb.NewTestSandboxesClient(env, Workflows{}, nil)

			started := map[string]bool{}
			env.OnWorkflow(pb.SandboxWorkflowName, mock.Anything, mock.Anything).
				Return(func(_ workflow.Context, in *pb.SandboxInput) error {
					if in.GetAssets().GetAssets()[0].GetRepo() != "example/repo" {
						t.Errorf("sandbox started with assets %v", in.GetAssets())
					}
					started[in.GetSandboxId()] = true
					return nil
				})
			assigned := map[string]string{}
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", agentv1.SandboxAssignSignalName, mock.Anything).
				Return(func(_, workflowID, _, _ string, arg any) error {
					if workflowID == tt.gone {
						return errRequesterGone
					}
					assigned[workflowID] = arg.(*agentv1.SandboxAssignInput).GetSandboxId()
					return nil
				})

			// One request per tick: a callback posting them all would fill the
			// channel its own main loop drains.
			var send func(i int)
			send = func(i int) {
				env.SignalWorkflow(pb.RequestSignalName, &pb.RequestInput{Requester: identity(fmt.Sprintf("agent-%d", i)), Assets: assets})
				if i+1 < requestsPerRun {
					env.RegisterDelayedCallback(func() { send(i + 1) }, time.Second)
				}
			}
			env.RegisterDelayedCallback(func() { send(0) }, 0)

			var can *workflow.ContinueAsNewError
			if err := client.SandboxOrchestrator(context.Background()); !errors.As(err, &can) {
				t.Fatalf("want continue-as-new, got %v", err)
			}
			if len(started) != requestsPerRun {
				t.Errorf("started %d sandboxes, want %d", len(started), requestsPerRun)
			}
			if len(assigned) != tt.wantAssigned {
				t.Errorf("assigned %d requesters, want %d", len(assigned), tt.wantAssigned)
			}
			for requester, id := range assigned {
				if !started[id] {
					t.Errorf("%s assigned %q, which was never started", requester, id)
				}
			}
		})
	}
}

func TestNewSandboxWorkflow(t *testing.T) {
	tests := []struct {
		name    string
		input   *pb.SandboxInput
		wantErr string
	}{
		{
			name:  "success - valid input",
			input: &pb.SandboxInput{SandboxId: "sb-1"},
		},
		{
			name:    "error - missing sandbox id",
			input:   &pb.SandboxInput{},
			wantErr: "sandbox_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newSandboxWorkflow(&pb.SandboxWorkflowInput{Req: tt.input}, Pods{})
			if tt.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

// leaseEvent is a signal the test delivers at a workflow-time offset.
type leaseEvent struct {
	at        time.Duration
	release   bool
	requester string
}

func TestSandboxWorkflow_Execute(t *testing.T) {
	const requester, gone, late = "agent-1", "agent-gone", "agent-late"
	tests := []struct {
		name                string
		events              []leaseEvent
		requestDuringDelete bool
		renderErr           error
		createErr           error
		readyErr            error
		deleteErr           error
		wantOwners          []string
		wantDenied          []string
		wantDuration        time.Duration
		wantErr             bool
	}{
		{
			name:         "success - no request, pod deleted after grace",
			wantDuration: grace,
		},
		{
			name:         "success - request during pod creation granted once the pod is ready",
			events:       []leaseEvent{{requester: requester}},
			wantOwners:   []string{requester},
			wantDuration: leaseDuration + grace,
		},
		{
			name: "success - owner extends",
			events: []leaseEvent{
				{requester: requester},
				{at: 5 * time.Minute, requester: requester},
			},
			wantOwners:   []string{requester, requester},
			wantDuration: 5*time.Minute + leaseDuration + grace,
		},
		{
			name: "success - stranger denied while the lease is live",
			events: []leaseEvent{
				{requester: requester},
				{at: 5 * time.Minute, requester: "agent-2"},
			},
			wantOwners:   []string{requester},
			wantDenied:   []string{"agent-2"},
			wantDuration: leaseDuration + grace,
		},
		{
			name: "success - stranger granted after expiry",
			events: []leaseEvent{
				{requester: requester},
				{at: 16 * time.Minute, requester: "agent-2"},
			},
			wantOwners:   []string{requester, "agent-2"},
			wantDuration: 16*time.Minute + leaseDuration + grace,
		},
		{
			name: "success - release then stranger granted",
			events: []leaseEvent{
				{requester: requester},
				{at: 5 * time.Minute, release: true, requester: requester},
				{at: 6 * time.Minute, requester: "agent-2"},
			},
			wantOwners:   []string{requester, "agent-2"},
			wantDuration: 6*time.Minute + leaseDuration + grace,
		},
		{
			name: "success - requester gone leaves the sandbox ownerless",
			events: []leaseEvent{
				{at: time.Minute, requester: gone},
				{at: 2 * time.Minute, requester: requester},
			},
			wantOwners:   []string{requester},
			wantDuration: 2*time.Minute + leaseDuration + grace,
		},
		{
			name:                "success - request during pod deletion denied",
			requestDuringDelete: true,
			wantDenied:          []string{late},
			wantDuration:        grace,
		},
		{
			name:       "error - render fails, nothing to delete",
			events:     []leaseEvent{{requester: requester}},
			renderErr:  errCluster,
			wantDenied: []string{requester},
			wantErr:    true,
		},
		{
			name:       "error - create fails, pod deleted by name",
			events:     []leaseEvent{{requester: requester}},
			createErr:  errCluster,
			wantDenied: []string{requester},
			wantErr:    true,
		},
		{
			name:       "error - pod never ready, pod deleted",
			events:     []leaseEvent{{requester: requester}},
			readyErr:   errCluster,
			wantDenied: []string{requester},
			wantErr:    true,
		},
		{
			name:       "error - delete fails after the pod never became ready",
			events:     []leaseEvent{{requester: requester}},
			readyErr:   errCluster,
			deleteErr:  errCluster,
			wantDenied: []string{requester},
			wantErr:    true,
		},
		{
			name:      "error - delete fails after serving",
			deleteErr: errCluster,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			env.SetStartTime(start)

			render := helmmocks.NewMockHelmActivities(t)
			kube := kubemocks.NewMockKubernetesActivities(t)
			helmv1.RegisterHelmActivities(env, render)
			kubev1.RegisterKubernetesActivities(env, kube)
			// Registrations must precede any env.On… mock.
			client := pb.NewTestSandboxesClient(env, Workflows{pods: Pods{Namespace: "ns-1"}}, nil)

			render.EXPECT().RenderChart(mock.Anything, mock.MatchedBy(func(req *helmv1.RenderChartInput) bool {
				return req.GetChart() == chartName && req.GetReleaseName() == "sandbox-sb-1" && strings.Contains(req.GetValuesYaml(), TaskQueue("sb-1"))
			})).Return(&helmv1.RenderChartOutput{
				Resources: []*kubev1.Resource{{Id: "pod", ManifestYaml: []byte(podManifest)}},
			}, tt.renderErr).Once()

			pod := &kubev1.ResourceIdentifier{Id: "pod", ApiVersion: "v1", Kind: "Pod", Name: "sandbox-sb-1", Namespace: "ns-1", Uid: "uid-1"}
			// A failed creation must delete and deny at once: neither may wait on the other.
			concurrent := (tt.createErr != nil || tt.readyErr != nil) && len(tt.events) > 0
			deleteStarted, deniedSent := make(chan struct{}), make(chan struct{})
			meet := func(mine, theirs chan struct{}) {
				if !concurrent {
					return
				}
				close(mine)
				select {
				case <-theirs:
				case <-time.After(5 * time.Second):
					t.Error("deleting and denying waited on each other")
				}
			}

			var ready bool
			if tt.renderErr == nil {
				kube.EXPECT().Create(mock.Anything, mock.MatchedBy(func(req *kubev1.CreateInput) bool {
					return len(req.GetResources()) == 1 && req.GetResources()[0].GetId() == "pod"
				})).Return(&kubev1.CreateOutput{Resources: []*kubev1.ResourceIdentifier{pod}}, tt.createErr).Once()

				wantDeleted := pod
				if tt.createErr != nil {
					wantDeleted = podIdentifier("sb-1", "ns-1")
				} else {
					kube.EXPECT().Ready(mock.Anything, mock.MatchedBy(func(req *kubev1.ReadyInput) bool {
						return len(req.GetResources()) == 1 && req.GetResources()[0].GetName() == "sandbox-sb-1"
					})).Run(func(context.Context, *kubev1.ReadyInput) { ready = tt.readyErr == nil }).Return(&kubev1.ReadyOutput{}, tt.readyErr).Once()
				}
				kube.EXPECT().Delete(mock.Anything, mock.MatchedBy(func(req *kubev1.DeleteInput) bool {
					return len(req.GetResources()) == 1 && proto.Equal(req.GetResources()[0], wantDeleted)
				})).Run(func(context.Context, *kubev1.DeleteInput) {
					meet(deleteStarted, deniedSent)
					if tt.requestDuringDelete {
						env.SignalWorkflow(pb.LeaseRequestSignalName, &pb.LeaseRequestInput{Requester: identity(late)})
					}
				}).Return(&kubev1.DeleteOutput{}, tt.deleteErr).Once()
			}

			var owners, denied []string
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, agentv1.SandboxLeaseGrantSignalName, mock.Anything).
				Return(func(_, workflowID, _, _ string, arg any) error {
					if !ready {
						t.Error("lease granted before the pod was ready")
					}
					if workflowID == gone {
						return errRequesterGone
					}
					lease := arg.(*agentv1.SandboxLeaseGrantInput).GetLease()
					owner := lease.GetOwner().GetWorkflowId()
					owners = append(owners, owner)
					if workflowID != owner {
						t.Errorf("lease sent to %q, owner is %q", workflowID, owner)
					}
					if lease.GetTaskQueue() != TaskQueue("sb-1") {
						t.Errorf("lease names queue %q", lease.GetTaskQueue())
					}
					return nil
				})
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, agentv1.SandboxLeaseDenySignalName, mock.Anything).
				Return(func(_, workflowID, _, _ string, arg any) error {
					if arg.(*agentv1.SandboxLeaseDenyInput).GetReason() == "" {
						t.Error("denied without a reason")
					}
					meet(deniedSent, deleteStarted)
					denied = append(denied, workflowID)
					return nil
				})

			for _, ev := range tt.events {
				env.RegisterDelayedCallback(func() {
					if ev.release {
						env.SignalWorkflow(pb.LeaseReleaseSignalName, &pb.LeaseReleaseInput{Requester: identity(ev.requester)})
						return
					}
					env.SignalWorkflow(pb.LeaseRequestSignalName, &pb.LeaseRequestInput{Requester: identity(ev.requester)})
				}, ev.at)
			}

			err := client.Sandbox(context.Background(), &pb.SandboxInput{SandboxId: "sb-1", Assets: &pb.Assets{}})
			if tt.wantErr {
				if err == nil {
					t.Fatal("want error, got none")
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Join(owners, ",") != strings.Join(tt.wantOwners, ",") {
				t.Errorf("leases went to %v, want %v", owners, tt.wantOwners)
			}
			if strings.Join(denied, ",") != strings.Join(tt.wantDenied, ",") {
				t.Errorf("denials went to %v, want %v", denied, tt.wantDenied)
			}
			if got := env.Now().Sub(start); !tt.wantErr && got != tt.wantDuration {
				t.Errorf("sandbox lived %v, want %v", got, tt.wantDuration)
			}
		})
	}
}

func TestChartValues(t *testing.T) {
	pods := Pods{Namespace: "ns-1", SidecarImage: "sidecar:1", TemporalAddress: "temporal-1:7233", TemporalNamespace: "tns-1"}
	values, err := chartValues("sb-1", &pb.Assets{}, pods)
	if err != nil {
		t.Fatal(err)
	}
	out, err := helm.NewActivities(charts.FS).RenderChart(context.Background(), &helmv1.RenderChartInput{
		Chart: chartName, ReleaseName: "sandbox-sb-1", ValuesYaml: values,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.GetResources()) != 1 {
		t.Fatalf("chart renders %d resources; failed-Create cleanup names one", len(out.GetResources()))
	}
	id := podIdentifier("sb-1", pods.Namespace)
	manifest := string(out.GetResources()[0].GetManifestYaml())

	tests := []struct {
		name  string
		lines []string
	}{
		{
			name:  "success - the pod is the one failed-Create cleanup deletes",
			lines: []string{"apiVersion: " + id.GetApiVersion(), "kind: " + id.GetKind(), "  name: " + id.GetName(), "  namespace: " + id.GetNamespace()},
		},
		{
			name:  "success - the sidecar runs the configured image",
			lines: []string{"image: " + pods.SidecarImage},
		},
		{
			name:  "success - the sidecar reaches the configured Temporal",
			lines: []string{`value: "` + pods.TemporalAddress + `"`, `value: "` + pods.TemporalNamespace + `"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, line := range tt.lines {
				if !strings.Contains(manifest, line+"\n") {
					t.Errorf("chart manifest lacks %q:\n%s", line, manifest)
				}
			}
		})
	}
}
