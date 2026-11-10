package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

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

	deadline, _ := ctx.Deadline()
	res, err := execd.Call(ctx, a.socket, &execdv1.Command{
		Command: args.Command,
		Timeout: durationpb.New(time.Until(deadline) - replyMargin),
	})
	if err != nil {
		return nil, err
	}
	out := strings.ToValidUTF8(string(res.GetOutput()), "\uFFFD")
	if res.GetTruncated() {
		out += "\n[truncated]"
	}
	return &pb.RunToolOutput{Output: &pb.ToolOutput{
		ToolCallId: call.GetId(),
		Output:     out,
		ExitCode:   res.GetExitCode(),
	}}, nil
}
