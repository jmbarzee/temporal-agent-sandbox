package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
	execd "github.com/jmbarzee/temporal-agent-sandbox/lib/execd/v1"
)

// newExecd's socket path is short enough for macOS, which t.TempDir's is not.
func newExecd(t *testing.T) *Activities {
	t.Helper()
	dir, err := os.MkdirTemp("", "execd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "sock")
	ln, err := execd.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() { _ = execd.Serve(ln, uint32(os.Getuid())) }()
	return &Activities{socket: socket}
}

func sh(command string) *pb.RunToolInput {
	input, _ := json.Marshal(map[string]string{"command": command})
	return &pb.RunToolInput{Call: &pb.ToolCall{Id: "c1", Name: shTool, Input: string(input)}}
}

func TestActivities_RunTool(t *testing.T) {
	tests := []struct {
		name         string
		input        *pb.RunToolInput
		wantOutput   string
		wantExitCode int32
		wantErrType  string
	}{
		{
			name:        "error - unknown tool",
			input:       &pb.RunToolInput{Call: &pb.ToolCall{Id: "c1", Name: "python"}},
			wantErrType: "UnknownTool",
		},
		{
			name:        "error - sh without a command",
			input:       &pb.RunToolInput{Call: &pb.ToolCall{Id: "c1", Name: shTool, Input: `{}`}},
			wantErrType: "BadToolInput",
		},
		{
			name:         "success - output and exit code come back for the call",
			input:        sh("echo hi; exit 2"),
			wantOutput:   "hi\n",
			wantExitCode: 2,
		},
		{
			name:       "success - output past the cap comes back cut and marked truncated",
			input:      sh("head -c 40000 /dev/zero | tr '\\0' a"),
			wantOutput: strings.Repeat("a", 32*1024) + "\n[truncated]",
		},
		{
			name:       "success - output that is not UTF-8 is made valid",
			input:      sh(`printf 'a\377b'`),
			wantOutput: "a\uFFFDb",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			// A refused call never reaches execd, so it has none.
			if tt.wantErrType != "" {
				_, err := (&Activities{}).RunTool(ctx, tt.input)
				var appErr *temporal.ApplicationError
				if !errors.As(err, &appErr) || appErr.Type() != tt.wantErrType {
					t.Fatalf("want error type %q, got %v", tt.wantErrType, err)
				}
				return
			}
			out, err := newExecd(t).RunTool(ctx, tt.input)
			if err != nil {
				t.Fatal(err)
			}
			got := out.GetOutput()
			if got.GetToolCallId() != "c1" || got.GetOutput() != tt.wantOutput || got.GetExitCode() != tt.wantExitCode {
				t.Errorf("got %q exit %d for %q, want %q exit %d", got.GetOutput(), got.GetExitCode(), got.GetToolCallId(), tt.wantOutput, tt.wantExitCode)
			}
		})
	}
}
