// Package sandbox is the provider boundary; sandbox.proto holds its contract.
package sandbox

import (
	"errors"
	"time"

	"github.com/google/uuid"
	enumsv1 "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	helmv1 "github.com/metacensus/library-temporal-kube/gen/helm/v1"
	kubev1 "github.com/metacensus/library-temporal-kube/gen/kube/v1"

	agentv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
)

const (
	leaseDuration = 15 * time.Minute
	renewBefore   = 5 * time.Minute
	grace         = 10 * time.Minute
	// requestsPerRun bounds the orchestrator's history before continue_as_new.
	requestsPerRun = 1000
	chartName      = "sandbox"
	infraQueue     = "infra"
	podTimeout     = 5 * time.Minute
)

// TaskQueue is where a sandbox's sidecar polls for RunTool calls.
func TaskQueue(sandboxID string) string {
	return "sandbox-" + sandboxID
}

// Pods is where sandbox pods run and how they reach Temporal.
type Pods struct {
	Namespace         string
	SidecarImage      string
	TemporalAddress   string
	TemporalNamespace string
}

type Workflows struct {
	pods Pods
}

func (Workflows) SandboxOrchestrator(_ workflow.Context, input *pb.SandboxOrchestratorWorkflowInput) (pb.SandboxOrchestratorWorkflow, error) {
	return &orchestrator{input: input}, nil
}

func (w Workflows) Sandbox(_ workflow.Context, input *pb.SandboxWorkflowInput) (pb.SandboxWorkflow, error) {
	return newSandboxWorkflow(input, w.pods)
}

// orchestrator is the singleton that creates sandboxes.
type orchestrator struct {
	input *pb.SandboxOrchestratorWorkflowInput
}

func (o *orchestrator) Execute(ctx workflow.Context) error {
	for served := 0; served < requestsPerRun; served++ {
		req, _ := o.input.Request.Receive(ctx)
		if err := o.start(ctx, req); err != nil {
			return err
		}
	}
	// Requests buffered after the loop would otherwise be lost to continue_as_new.
	for req := o.input.Request.ReceiveAsync(); req != nil; req = o.input.Request.ReceiveAsync() {
		if err := o.start(ctx, req); err != nil {
			return err
		}
	}
	return workflow.NewContinueAsNewError(ctx, pb.SandboxOrchestratorWorkflowName)
}

func (o *orchestrator) start(ctx workflow.Context, req *pb.RequestInput) error {
	var id string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any { return uuid.NewString() }).Get(&id); err != nil {
		return err
	}
	run, err := pb.SandboxChildAsync(ctx, &pb.SandboxInput{SandboxId: id, Assets: req.GetAssets()},
		pb.NewSandboxChildOptions().WithParentClosePolicy(enumsv1.PARENT_CLOSE_POLICY_ABANDON))
	if err != nil {
		return err
	}
	if _, err := run.WaitStart(ctx); err != nil {
		return err
	}
	requester := req.GetRequester().GetWorkflowId()
	if err := agentv1.SandboxAssignExternal(ctx, requester, "", &agentv1.SandboxAssignInput{SandboxId: id}); err != nil {
		workflow.GetLogger(ctx).Warn("requester not assigned; sandbox expires unleased", "requester", requester, "sandbox", id, "error", err)
	}
	return nil
}

type sandbox struct {
	input *pb.SandboxWorkflowInput
	pods  Pods
	lease *pb.Lease
}

func newSandboxWorkflow(input *pb.SandboxWorkflowInput, pods Pods) (*sandbox, error) {
	s := &sandbox{input: input, pods: pods}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sandbox) validate() error {
	if s.input == nil || s.input.Req.GetSandboxId() == "" {
		return errors.New("sandbox_id is required")
	}
	return nil
}

func (s *sandbox) GetLease() (*pb.GetLeaseOutput, error) {
	return &pb.GetLeaseOutput{Lease: s.lease}, nil
}

func (s *sandbox) Execute(ctx workflow.Context) error {
	reason := "sandbox closed"
	resources, err := s.createPod(ctx)
	if err == nil {
		s.serve(ctx)
	} else {
		reason = err.Error()
	}
	if len(resources) == 0 {
		s.denyQueued(ctx, reason)
		return err
	}

	// Nothing is in flight by now: any lease has been expired for the whole
	// grace period. The pod's termination grace period covers the sidecar.
	// Started first so denying queued requests cannot delay it.
	deleted := kubev1.DeleteAsync(ctx, &kubev1.DeleteInput{Resources: resources},
		kubev1.NewDeleteActivityOptions().WithTaskQueue(infraQueue))
	s.denyQueued(ctx, reason)
	_, deleteErr := deleted.Get(ctx)
	s.denyQueued(ctx, reason)
	if err == nil {
		return deleteErr
	}
	if deleteErr != nil {
		workflow.GetLogger(ctx).Error("pod not deleted", "sandbox", s.input.Req.GetSandboxId(), "error", deleteErr)
	}
	return err
}

// createPod returns what to delete when the sandbox is done, even when it fails.
func (s *sandbox) createPod(ctx workflow.Context) ([]*kubev1.ResourceIdentifier, error) {
	req := s.input.Req
	id := req.GetSandboxId()

	// Chart: sandbox container + sidecar worker polling the sandbox's queue.
	values, err := chartValues(id, req.GetAssets(), s.pods)
	if err != nil {
		return nil, err
	}
	rendered, err := helmv1.RenderChart(ctx, &helmv1.RenderChartInput{Chart: chartName, ReleaseName: "sandbox-" + id, ValuesYaml: values})
	if err != nil {
		return nil, err
	}
	created, err := kubev1.Create(ctx, &kubev1.CreateInput{Resources: rendered.GetResources()},
		kubev1.NewCreateActivityOptions().WithTaskQueue(infraQueue).WithScheduleToCloseTimeout(podTimeout))
	if err != nil {
		// Create can fail after reaching the cluster.
		return []*kubev1.ResourceIdentifier{podIdentifier(id, s.pods.Namespace)}, err
	}
	// Ready fails until every resource is current; the retry policy is the wait.
	_, err = kubev1.Ready(ctx, &kubev1.ReadyInput{Resources: created.GetResources()},
		kubev1.NewReadyActivityOptions().WithTaskQueue(infraQueue).
			WithScheduleToCloseTimeout(podTimeout).
			WithRetryPolicy(&temporal.RetryPolicy{InitialInterval: 2 * time.Second, MaximumInterval: 10 * time.Second}))
	return created.GetResources(), err
}

func (s *sandbox) serve(ctx workflow.Context) {
	deadline := workflow.Now(ctx).Add(grace)
	for {
		if s.lease != nil {
			deadline = s.lease.GetExpiry().AsTime().Add(grace)
		}
		now := workflow.Now(ctx)
		if !now.Before(deadline) {
			break
		}
		timerCtx, cancelTimer := workflow.WithCancel(ctx)
		sel := workflow.NewSelector(ctx)
		s.input.LeaseRequest.Select(sel, func(r *pb.LeaseRequestInput) { s.request(ctx, r.GetRequester()) })
		s.input.LeaseRelease.Select(sel, func(r *pb.LeaseReleaseInput) {
			if s.owns(r.GetRequester()) {
				s.lease = nil
			}
		})
		sel.AddFuture(workflow.NewTimer(timerCtx, deadline.Sub(now)), func(workflow.Future) {})
		sel.Select(ctx)
		cancelTimer()
	}
}

func (s *sandbox) denyQueued(ctx workflow.Context, reason string) {
	for r := s.input.LeaseRequest.ReceiveAsync(); r != nil; r = s.input.LeaseRequest.ReceiveAsync() {
		s.deny(ctx, r.GetRequester(), reason)
	}
}

func (s *sandbox) deny(ctx workflow.Context, requester *pb.Identity, reason string) {
	if err := agentv1.SandboxLeaseDenyExternal(ctx, requester.GetWorkflowId(), "", &agentv1.SandboxLeaseDenyInput{Reason: reason}); err != nil {
		workflow.GetLogger(ctx).Warn("requester not denied", "requester", requester.GetWorkflowId(), "reason", reason, "error", err)
	}
}

func (s *sandbox) request(ctx workflow.Context, requester *pb.Identity) {
	if s.lease != nil && !s.owns(requester) && workflow.Now(ctx).Before(s.lease.GetExpiry().AsTime()) {
		s.deny(ctx, requester, "leased to another holder")
		return
	}
	id := s.input.Req.GetSandboxId()
	lease := &pb.Lease{
		SandboxId:   id,
		Owner:       requester,
		Expiry:      timestamppb.New(workflow.Now(ctx).Add(leaseDuration)),
		RenewBefore: durationpb.New(renewBefore),
		TaskQueue:   TaskQueue(id),
	}
	// Failing here would leak the pod.
	if err := agentv1.SandboxLeaseGrantExternal(ctx, requester.GetWorkflowId(), "", &agentv1.SandboxLeaseGrantInput{Lease: lease}); err != nil {
		workflow.GetLogger(ctx).Warn("requester not granted", "requester", requester.GetWorkflowId(), "sandbox", id, "error", err)
		return
	}
	s.lease = lease
}

func (s *sandbox) owns(requester *pb.Identity) bool {
	return s.lease != nil && s.lease.GetOwner().GetWorkflowId() == requester.GetWorkflowId()
}

func chartValues(id string, assets *pb.Assets, pods Pods) (string, error) {
	repos := make([]string, 0, len(assets.GetAssets()))
	for _, a := range assets.GetAssets() {
		repos = append(repos, a.GetRepo())
	}
	out, err := yaml.Marshal(map[string]any{
		"sandboxId": id,
		"taskQueue": TaskQueue(id),
		"repos":     repos,
		"namespace": pods.Namespace,
		"images":    map[string]any{"sidecar": pods.SidecarImage},
		"temporal":  map[string]any{"address": pods.TemporalAddress, "namespace": pods.TemporalNamespace},
	})
	return string(out), err
}

func podIdentifier(id, namespace string) *kubev1.ResourceIdentifier {
	return &kubev1.ResourceIdentifier{ApiVersion: "v1", Kind: "Pod", Name: "sandbox-" + id, Namespace: namespace}
}
