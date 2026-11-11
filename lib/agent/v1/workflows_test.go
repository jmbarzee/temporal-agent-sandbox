package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	sandboxv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	agentmocks "github.com/jmbarzee/temporal-agent-sandbox/mocks/agent/v1"
	sandboxmocks "github.com/jmbarzee/temporal-agent-sandbox/mocks/sandbox/v1"
)

// errModel is non-retryable so a failing activity is called exactly once.
var errModel = temporal.NewNonRetryableApplicationError("model unavailable", "ModelError", nil)

var errSandboxGone = &temporal.UnknownExternalWorkflowExecutionError{}

// errToolLost is retryable, so RunTool's retry policy bounds its calls.
var errToolLost = errors.New("execd closed without a reply")

func validInput() *pb.AgentInput {
	return &pb.AgentInput{
		Instructions: &pb.Instructions{Text: "be brief"},
		Task:         &pb.Task{Scope: "list the repo"},
		Tools:        &pb.Tools{},
	}
}

func TestNewAgentWorkflow(t *testing.T) {
	lease := &sandboxv1.Lease{SandboxId: "sb-1", TaskQueue: "sandbox-sb-1"}
	tests := []struct {
		name    string
		input   *pb.AgentInput
		wantErr string
	}{
		{name: "success - fresh start", input: validInput()},
		{
			name:  "success - resume carries lease and context",
			input: &pb.AgentInput{Resume: &pb.Resume{Lease: lease, Context: &pb.Context{}}},
		},
		{
			name:    "error - missing instructions",
			input:   &pb.AgentInput{Task: &pb.Task{Scope: "x"}, Tools: &pb.Tools{}},
			wantErr: "instructions",
		},
		{
			name:    "error - missing task scope",
			input:   &pb.AgentInput{Instructions: &pb.Instructions{Text: "x"}, Tools: &pb.Tools{}},
			wantErr: "task.scope",
		},
		{
			name:    "error - missing tools",
			input:   &pb.AgentInput{Instructions: &pb.Instructions{Text: "x"}, Task: &pb.Task{Scope: "x"}},
			wantErr: "tools",
		},
		{
			name:    "error - resume without a lease",
			input:   &pb.AgentInput{Resume: &pb.Resume{Context: &pb.Context{}}},
			wantErr: "resume.lease",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newAgentWorkflow(&pb.AgentWorkflowInput{Req: tt.input})
			if tt.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func reply(content string, done bool, calls ...*sandboxv1.ToolCall) *pb.CallModelOutput {
	return &pb.CallModelOutput{Reply: &pb.Message{Role: "assistant", Content: content, ToolCalls: calls}, Done: done}
}

func TestAgentWorkflow_Execute(t *testing.T) {
	const workflowID = "agent-1"
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease := &sandboxv1.Lease{
		SandboxId:   "sb-1",
		Owner:       &sandboxv1.Identity{WorkflowId: workflowID},
		Expiry:      timestamppb.New(start.Add(15 * time.Minute)),
		RenewBefore: durationpb.New(5 * time.Minute),
		TaskQueue:   "sandbox-sb-1",
	}
	expiring := proto.Clone(lease).(*sandboxv1.Lease)
	expiring.Expiry = timestamppb.New(start.Add(time.Minute))
	expired := proto.Clone(lease).(*sandboxv1.Lease)
	expired.Expiry = timestamppb.New(start)
	bound := make([]*pb.CallModelOutput, turnsPerRun)
	for i := range bound {
		bound[i] = reply("thinking", false)
	}
	tests := []struct {
		name              string
		noAssign          bool
		unansweredAt      int // the lease request, counted from 1, that gets no answer
		leases            []*sandboxv1.Lease
		deniedReason      string
		leaseRequestErr   error
		replies           []*pb.CallModelOutput
		modelErr          error
		toolErr           error
		wantMessages      int
		wantToolCalls     int
		wantErr           bool
		wantErrType       string
		wantContinueAsNew bool
	}{
		{
			name:         "success - model finishes on the first turn",
			replies:      []*pb.CallModelOutput{reply("done", true)},
			wantMessages: 3,
		},
		{
			name: "success - a tool call runs in the sandbox before the model finishes",
			replies: []*pb.CallModelOutput{
				reply("", false, &sandboxv1.ToolCall{Id: "c1", Name: "sh", Input: "ls"}),
				reply("done", true),
			},
			wantMessages:  5,
			wantToolCalls: 1,
		},
		{
			name:   "success - requests the same sandbox again inside the renew window",
			leases: []*sandboxv1.Lease{expiring},
			replies: []*pb.CallModelOutput{
				reply("thinking", false),
				reply("done", true),
			},
			wantMessages: 4,
		},
		{
			name:              "success - continues as new at the turn bound",
			replies:           bound,
			wantContinueAsNew: true,
		},
		{
			name:            "error - the assigned sandbox is gone",
			leaseRequestErr: errSandboxGone,
			wantErr:         true,
		},
		{
			name:         "error - first lease request denied",
			deniedReason: "pod never became ready",
			wantErr:      true,
			wantErrType:  "SandboxLeaseDenied",
		},
		{
			name:         "error - renewal denied",
			leases:       []*sandboxv1.Lease{expiring},
			replies:      []*pb.CallModelOutput{reply("thinking", false)},
			deniedReason: "pod never became ready",
			wantErr:      true,
			wantErrType:  "SandboxLeaseDenied",
		},
		{
			name:        "error - no sandbox assigned in time",
			noAssign:    true,
			wantErr:     true,
			wantErrType: "SandboxLeaseTimeout",
		},
		{
			name:         "error - first lease request unanswered",
			unansweredAt: 1,
			wantErr:      true,
			wantErrType:  "SandboxLeaseTimeout",
		},
		{
			name:         "error - renewal unanswered when the lease expires",
			leases:       []*sandboxv1.Lease{expired},
			unansweredAt: 2,
			replies:      []*pb.CallModelOutput{reply("thinking", false)},
			wantErr:      true,
			wantErrType:  "SandboxLeaseTimeout",
		},
		{
			name:     "error - model fails",
			modelErr: errModel,
			wantErr:  true,
		},
		{
			name:          "error - a tool call that keeps failing stops at its third attempt",
			replies:       []*pb.CallModelOutput{reply("", false, &sandboxv1.ToolCall{Id: "c1", Name: "sh", Input: "ls"})},
			toolErr:       errToolLost,
			wantToolCalls: 3,
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			env.SetStartTime(start)
			env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})

			agents := agentmocks.NewMockAgentsActivities(t)
			sandboxes := sandboxmocks.NewMockSandboxesActivities(t)
			pb.RegisterAgentsActivities(env, agents)
			sandboxv1.RegisterSandboxesActivities(env, sandboxes)
			// Registrations must precede any env.On… mock.
			client := pb.NewTestAgentsClient(env, Workflows{}, nil)

			if tt.modelErr != nil {
				agents.EXPECT().CallModel(mock.Anything, mock.Anything).Return(nil, tt.modelErr).Once()
			}
			for _, r := range tt.replies {
				agents.EXPECT().CallModel(mock.Anything, mock.Anything).Return(r, nil).Once()
			}
			if tt.wantToolCalls > 0 {
				sandboxes.EXPECT().RunTool(mock.Anything, mock.MatchedBy(func(req *sandboxv1.RunToolInput) bool {
					return req.GetCall().GetId() == "c1"
				})).Return(&sandboxv1.RunToolOutput{
					Output: &sandboxv1.ToolOutput{ToolCallId: "c1", Output: "README.md"},
				}, tt.toolErr).Times(tt.wantToolCalls)
			}

			env.OnSignalExternalWorkflow(mock.Anything, orchestratorID, "", sandboxv1.RequestSignalName, mock.Anything).
				Run(func(args mock.Arguments) {
					if got := args.Get(4).(*sandboxv1.RequestInput).GetRequester().GetWorkflowId(); got != workflowID {
						t.Errorf("requested as %q", got)
					}
					if !tt.noAssign {
						env.SignalWorkflow(pb.SandboxAssignSignalName, &pb.SandboxAssignInput{SandboxId: "sb-1"})
					}
				}).Return(nil).Once()
			leases := append(tt.leases, lease)
			requests := 0
			env.OnSignalExternalWorkflow(mock.Anything, sandboxWorkflowID("sb-1"), "", sandboxv1.LeaseRequestSignalName, mock.Anything).
				Return(func(_, _, _, _ string, arg any) error {
					if got := arg.(*sandboxv1.LeaseRequestInput).GetRequester().GetWorkflowId(); got != workflowID {
						t.Errorf("lease requested as %q", got)
					}
					if tt.leaseRequestErr != nil {
						return tt.leaseRequestErr
					}
					if requests++; requests == tt.unansweredAt {
						return nil
					}
					if len(leases) == 1 && tt.deniedReason != "" {
						env.SignalWorkflow(pb.SandboxLeaseDenySignalName, &pb.SandboxLeaseDenyInput{Reason: tt.deniedReason})
						return nil
					}
					env.SignalWorkflow(pb.SandboxLeaseGrantSignalName, &pb.SandboxLeaseGrantInput{Lease: leases[0]})
					leases = leases[1:]
					return nil
				})
			if !tt.wantErr && !tt.wantContinueAsNew {
				env.OnSignalExternalWorkflow(mock.Anything, sandboxWorkflowID("sb-1"), "", sandboxv1.LeaseReleaseSignalName, mock.Anything).
					Return(nil).Once()
			}

			out, err := client.Agent(context.Background(), validInput())
			switch {
			case tt.wantErr:
				if err == nil {
					t.Fatal("want error, got none")
				}
				if !strings.Contains(err.Error(), tt.deniedReason) {
					t.Errorf("error %q does not name %q", err, tt.deniedReason)
				}
				var appErr *temporal.ApplicationError
				if tt.wantErrType != "" && (!errors.As(err, &appErr) || appErr.Type() != tt.wantErrType) {
					t.Errorf("want error type %q, got %v", tt.wantErrType, err)
				}
			case tt.wantContinueAsNew:
				var can *workflow.ContinueAsNewError
				if !errors.As(err, &can) {
					t.Fatalf("want continue-as-new, got %v", err)
				}
				var next pb.AgentInput
				if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
					t.Fatal(err)
				}
				if next.GetResume().GetLease().GetSandboxId() != "sb-1" {
					t.Errorf("resume lost the lease: %v", next.GetResume().GetLease())
				}
				if got := len(next.GetResume().GetContext().GetMessages()); got != 2+turnsPerRun {
					t.Errorf("resume carries %d messages, want %d", got, 2+turnsPerRun)
				}
			default:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got := len(out.GetContext().GetMessages()); got != tt.wantMessages {
					t.Errorf("context has %d messages, want %d", got, tt.wantMessages)
				}
				if len(leases) != 0 {
					t.Errorf("%d lease answers never requested", len(leases))
				}
			}
		})
	}
}
