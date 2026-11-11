package sandbox

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

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

// runTool runs one attempt of RunTool, as a worker would. Every attempt
// belongs to the same workflow run and activity, as a retry's does.
func runTool(a *Activities, input *pb.RunToolInput) (*pb.RunToolOutput, error) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivity(a)
	val, err := env.ExecuteActivity(a.RunTool, input)
	if err != nil {
		return nil, err
	}
	out := &pb.RunToolOutput{}
	return out, val.Get(out)
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
			wantOutput: "a�b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A refused call never reaches execd, so it has none.
			if tt.wantErrType != "" {
				_, err := runTool(&Activities{}, tt.input)
				var appErr *temporal.ApplicationError
				if !errors.As(err, &appErr) || appErr.Type() != tt.wantErrType {
					t.Fatalf("want error type %q, got %v", tt.wantErrType, err)
				}
				return
			}
			out, err := runTool(newExecd(t), tt.input)
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

func TestActivities_RunTool_retry(t *testing.T) {
	// The first attempt finished, but Temporal never heard; the retry gets its
	// output, and the command ran once.
	a := newExecd(t)
	marks := filepath.Join(t.TempDir(), "marks")
	input := sh("echo ran >> " + marks + "; echo done")
	first, err := runTool(a, input)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := runTool(a, input)
	if err != nil {
		t.Fatal(err)
	}
	if retry.GetOutput().GetOutput() != first.GetOutput().GetOutput() {
		t.Errorf("retry got %q, want the first attempt's %q", retry.GetOutput().GetOutput(), first.GetOutput().GetOutput())
	}
	got, err := os.ReadFile(marks)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(got), "ran"); n != 1 {
		t.Errorf("the command ran %d times, want once", n)
	}
}

func TestCallKey(t *testing.T) {
	info := func(run, activityID string, attempt int32) activity.Info {
		i := activity.Info{ActivityID: activityID, Attempt: attempt}
		i.WorkflowExecution.ID, i.WorkflowExecution.RunID = "agent-1", run
		return i
	}
	first := callKey(info("r1", "7", 1))
	tests := []struct {
		name string
		info activity.Info
		same bool
	}{
		{name: "success - a retry shares its call's key", info: info("r1", "7", 2), same: true},
		{name: "success - another call has its own", info: info("r1", "8", 1)},
		{name: "success - the same call in another run has its own", info: info("r2", "7", 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := callKey(tt.info) == first; got != tt.same {
				t.Errorf("same key as the first attempt: got %v, want %v", got, tt.same)
			}
		})
	}
}
