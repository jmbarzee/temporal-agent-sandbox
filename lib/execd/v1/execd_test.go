package execd

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/execd/v1"
)

// socketPath is short: t.TempDir can pass the 104 bytes macOS allows a socket path.
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "execd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "sock")
}

func serving(clientUID int) func(t *testing.T) string {
	return func(t *testing.T) string {
		socket := socketPath(t)
		ln, err := Listen(socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() { _ = Serve(ln, uint32(clientUID)) }()
		return socket
	}
}

func hostile(reply *pb.Result) func(t *testing.T) string {
	return func(t *testing.T) string {
		socket := socketPath(t)
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
			_, _ = protodelim.MarshalTo(conn, reply)
		}()
		return socket
	}
}

func firstPID(t *testing.T, output []byte) int {
	t.Helper()
	pid, err := strconv.Atoi(strings.SplitN(string(output), "\n", 2)[0])
	if err != nil {
		t.Fatalf("no PID in %q", output)
	}
	return pid
}

func TestCall(t *testing.T) {
	me := os.Getuid()
	tests := []struct {
		name          string
		socket        func(t *testing.T) string
		command       string
		timeout       time.Duration
		wantOutput    string
		wantTruncated bool
		wantExitCode  int32
		wantErr       bool
		after         func(t *testing.T, output []byte)
	}{
		{
			name:         "success - stdout and stderr come back with the exit code",
			socket:       serving(me),
			command:      "echo out; echo err >&2; exit 3",
			wantOutput:   "out\nerr\n",
			wantExitCode: 3,
		},
		{
			name:          "success - output past the cap comes back cut and marked",
			socket:        serving(me),
			command:       "head -c 40000 /dev/zero",
			wantOutput:    strings.Repeat("\x00", maxOutput),
			wantTruncated: true,
		},
		{
			name:         "success - a command past its timeout is killed with its children",
			socket:       serving(me),
			command:      "sleep 30 & echo $!; wait",
			timeout:      200 * time.Millisecond,
			wantExitCode: -1,
			after: func(t *testing.T, output []byte) {
				pid := firstPID(t, output)
				for deadline := time.Now().Add(2 * time.Second); syscall.Kill(pid, 0) == nil; time.Sleep(10 * time.Millisecond) {
					if time.Now().After(deadline) {
						t.Fatalf("child %d outlived its command", pid)
					}
				}
			},
		},
		{
			name:    "success - a child left running cannot hold the reply open",
			socket:  serving(me),
			command: "sleep 30 & echo $!",
			after: func(t *testing.T, output []byte) {
				_ = syscall.Kill(firstPID(t, output), syscall.SIGKILL)
			},
		},
		{
			name:    "error - a UID other than the client's is refused",
			socket:  serving(me + 1),
			command: "true",
			wantErr: true,
		},
		{
			name:    "error - a reply past the size cap",
			socket:  hostile(&pb.Result{Output: make([]byte, maxReply)}),
			wantErr: true,
		},
		{
			name:    "error - a reply under the size cap whose output is past the cap",
			socket:  hostile(&pb.Result{Output: make([]byte, maxOutput+1)}),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout := tt.timeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, err := Call(ctx, tt.socket(t), &pb.Command{Command: tt.command, Timeout: durationpb.New(timeout)})
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %v", res)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantOutput != "" && string(res.GetOutput()) != tt.wantOutput {
				t.Errorf("output %q, want %q", res.GetOutput(), tt.wantOutput)
			}
			if res.GetTruncated() != tt.wantTruncated {
				t.Errorf("truncated %v, want %v", res.GetTruncated(), tt.wantTruncated)
			}
			if res.GetExitCode() != tt.wantExitCode {
				t.Errorf("exit code %d, want %d", res.GetExitCode(), tt.wantExitCode)
			}
			if tt.after != nil {
				tt.after(t, res.GetOutput())
			}
		})
	}
}
