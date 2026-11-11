package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/durationpb"

	execdv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/execd/v1"
	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	execd "github.com/jmbarzee/temporal-agent-sandbox/lib/execd/v1"
)

const (
	// shTool is the one tool a sandbox runs today: a shell command in the workspace.
	shTool      = "sh"
	replyMargin = 10 * time.Second
)

type Activities struct {
	socket string
	// done keeps each finished call's output by callKey, for the pod's life.
	done sync.Map
}

// NewActivities reaches execd at EXECD_SOCKET.
func NewActivities() (*Activities, error) {
	socket := os.Getenv("EXECD_SOCKET")
	if socket == "" {
		return nil, errors.New("EXECD_SOCKET is required")
	}
	return &Activities{socket: socket}, nil
}

func (a *Activities) RunTool(ctx context.Context, req *pb.RunToolInput) (*pb.RunToolOutput, error) {
	call := req.GetCall()
	if call.GetName() != shTool {
		return nil, temporal.NewNonRetryableApplicationError(fmt.Sprintf("unknown tool %q", call.GetName()), "UnknownTool", nil)
	}
	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal([]byte(call.GetInput()), &args); err != nil || args.Command == "" {
		return nil, temporal.NewNonRetryableApplicationError("sh needs a command", "BadToolInput", err)
	}

	// A retry of a call that finished, but whose output never reached
	// Temporal, gets that output instead of running the command again.
	key := callKey(activity.GetInfo(ctx))
	if out, ok := a.done.Load(key); ok {
		return out.(*pb.RunToolOutput), nil
	}

	deadline, _ := ctx.Deadline()
	res, err := execd.Call(ctx, a.socket, &execdv1.Command{
		Command: args.Command,
		Timeout: durationpb.New(time.Until(deadline) - replyMargin),
	})
	if err != nil {
		return nil, err
	}
	text := strings.ToValidUTF8(string(res.GetOutput()), "\uFFFD")
	if res.GetTruncated() {
		text += "\n[truncated]"
	}
	out := &pb.RunToolOutput{Output: &pb.ToolOutput{
		ToolCallId: call.GetId(),
		Output:     text,
		ExitCode:   res.GetExitCode(),
	}}
	a.done.Store(key, out)
	return out, nil
}

// callKey is the same for every attempt of one call, and differs between
// calls and between runs of a workflow.
func callKey(info activity.Info) string {
	return info.WorkflowExecution.ID + "/" + info.WorkflowExecution.RunID + "/" + info.ActivityID
}
