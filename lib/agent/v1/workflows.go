// Package agent is one unbroken LLM context working one Task inside one
// Sandbox. Lease handling is the workflow's; the LLM never sees it.
package agent

import (
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	sandboxv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
)

const (
	// orchestratorID is the SandboxOrchestrator's fixed workflow id (sandbox.proto).
	orchestratorID = "sandboxes"
	// turnsPerRun bounds history before continue_as_new.
	turnsPerRun = 50
	// leaseWait outlasts the Sandbox's own creation caps, so a slow sandbox answers first.
	leaseWait = 15 * time.Minute
)

type Workflows struct{}

func (Workflows) Agent(_ workflow.Context, input *pb.AgentWorkflowInput) (pb.AgentWorkflow, error) {
	return newAgentWorkflow(input)
}

type agentWorkflow struct {
	input   *pb.AgentWorkflowInput
	lease   *sandboxv1.Lease
	pending bool
	context *pb.Context
}

func newAgentWorkflow(input *pb.AgentWorkflowInput) (*agentWorkflow, error) {
	w := &agentWorkflow{input: input}
	if err := w.validate(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *agentWorkflow) validate() error {
	if w.input == nil {
		return errors.New("input is required")
	}
	req := w.input.Req
	if resume := req.GetResume(); resume != nil {
		if resume.GetLease().GetSandboxId() == "" || resume.GetLease().GetTaskQueue() == "" {
			return errors.New("resume.lease must name a sandbox and its task queue")
		}
		if resume.GetContext() == nil {
			return errors.New("resume.context is required")
		}
		return nil
	}
	if req.GetInstructions().GetText() == "" {
		return errors.New("instructions.text is required")
	}
	if req.GetTask().GetScope() == "" {
		return errors.New("task.scope is required")
	}
	if req.GetTools() == nil {
		return errors.New("tools is required")
	}
	return nil
}

func (w *agentWorkflow) Execute(ctx workflow.Context) (*pb.AgentOutput, error) {
	req := w.input.Req
	me := &sandboxv1.Identity{WorkflowId: workflow.GetInfo(ctx).WorkflowExecution.ID}
	requestLease := func(sandboxID string) error {
		w.pending = true
		return sandboxv1.LeaseRequestExternal(ctx, sandboxWorkflowID(sandboxID), "", &sandboxv1.LeaseRequestInput{Requester: me})
	}

	if req.GetResume() == nil {
		deadline := workflow.Now(ctx).Add(leaseWait)
		err := sandboxv1.RequestExternal(ctx, orchestratorID, "", &sandboxv1.RequestInput{
			Requester: me, Assets: req.GetTask().GetRelevantAssets(),
		})
		if err != nil {
			return nil, err
		}
		assigned, ok, _ := w.input.SandboxAssign.ReceiveWithTimeout(ctx, leaseWait)
		if !ok {
			return nil, leaseTimeout("no sandbox assigned")
		}
		if err := requestLease(assigned.GetSandboxId()); err != nil {
			return nil, err
		}
		if err := w.awaitLease(ctx, deadline.Sub(workflow.Now(ctx))); err != nil {
			return nil, err
		}
		w.context = &pb.Context{Messages: []*pb.Message{
			{Role: "system", Content: req.GetInstructions().GetText()},
			{Role: "user", Content: req.GetTask().GetScope()},
		}}
	} else {
		w.lease = req.GetResume().GetLease()
		w.context = req.GetResume().GetContext()
	}

	for turns := 1; ; turns++ {
		for g := w.input.SandboxLeaseGrant.ReceiveAsync(); g != nil; g = w.input.SandboxLeaseGrant.ReceiveAsync() {
			w.granted(g)
		}
		if d := w.input.SandboxLeaseDeny.ReceiveAsync(); d != nil {
			return nil, deniedError(d)
		}
		if w.pending && !workflow.Now(ctx).Before(w.lease.GetExpiry().AsTime()) {
			return nil, leaseTimeout("the lease expired with its renewal unanswered")
		}

		// Deterministic clock; ask early, keep working while it answers.
		renewAt := w.lease.GetExpiry().AsTime().Add(-w.lease.GetRenewBefore().AsDuration())
		if !w.pending && workflow.Now(ctx).After(renewAt) {
			if err := requestLease(w.lease.GetSandboxId()); err != nil {
				return nil, err
			}
		}

		reply, err := pb.CallModel(ctx, &pb.CallModelInput{Context: w.context, Tools: req.GetTools()})
		if err != nil {
			return nil, err
		}
		w.context.Messages = append(w.context.Messages, reply.GetReply())

		// The model declaring done ends the run.
		if reply.GetDone() {
			break
		}

		// Tool calls run one at a time.
		for _, call := range reply.GetReply().GetToolCalls() {
			out, err := sandboxv1.RunTool(ctx, &sandboxv1.RunToolInput{Call: call},
				sandboxv1.NewRunToolActivityOptions().WithTaskQueue(w.lease.GetTaskQueue()))
			if err != nil {
				return nil, err
			}
			w.context.Messages = append(w.context.Messages, &pb.Message{
				Role: "tool", ToolOutputs: []*sandboxv1.ToolOutput{out.GetOutput()},
			})
		}

		if turns >= turnsPerRun {
			// Continue as new to bound history; the context carries over whole.
			return nil, workflow.NewContinueAsNewError(ctx, pb.AgentWorkflowName, &pb.AgentInput{
				Instructions: req.GetInstructions(), Task: req.GetTask(), Tools: req.GetTools(),
				Resume: &pb.Resume{Lease: w.lease, Context: w.context},
			})
		}
	}

	err := sandboxv1.LeaseReleaseExternal(ctx, sandboxWorkflowID(w.lease.GetSandboxId()), "", &sandboxv1.LeaseReleaseInput{Requester: me})
	if err != nil {
		return nil, err
	}
	return &pb.AgentOutput{Context: w.context}, nil
}

func (w *agentWorkflow) awaitLease(ctx workflow.Context, timeout time.Duration) error {
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()
	var err error
	sel := workflow.NewSelector(ctx)
	w.input.SandboxLeaseGrant.Select(sel, w.granted)
	w.input.SandboxLeaseDeny.Select(sel, func(d *pb.SandboxLeaseDenyInput) { err = deniedError(d) })
	sel.AddFuture(workflow.NewTimer(timerCtx, timeout), func(workflow.Future) { err = leaseTimeout("no answer to the lease request") })
	sel.Select(ctx)
	return err
}

func (w *agentWorkflow) granted(in *pb.SandboxLeaseGrantInput) {
	w.lease = in.GetLease()
	w.pending = false
}

func deniedError(in *pb.SandboxLeaseDenyInput) error {
	return temporal.NewNonRetryableApplicationError("sandbox lease denied: "+in.GetReason(), "SandboxLeaseDenied", nil)
}

func leaseTimeout(waited string) error {
	return temporal.NewNonRetryableApplicationError("sandbox lease timed out: "+waited, "SandboxLeaseTimeout", nil)
}

// sandboxWorkflowID is the Sandbox workflow's id (sandbox.proto).
func sandboxWorkflowID(sandboxID string) string {
	return "sandbox/" + sandboxID
}
