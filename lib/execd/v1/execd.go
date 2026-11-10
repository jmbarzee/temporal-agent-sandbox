// Package execd runs model-chosen commands in the sandbox container for the
// sidecar worker alone.
package execd

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/execd/v1"
)

const (
	// maxOutput bounds what goes back into workflow history.
	maxOutput = 32 * 1024
	maxReply  = maxOutput + 64 // Result's other fields
)

// Listen replaces socket with one open to every UID; Serve's peer check decides who is answered.
func Listen(socket string) (*net.UnixListener, error) {
	_ = os.Remove(socket)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0o666); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// Serve closes any connection not from clientUID.
func Serve(ln *net.UnixListener, clientUID uint32) error {
	for {
		conn, err := ln.AcceptUnix()
		if err != nil {
			return err
		}
		go func() {
			defer func() { _ = conn.Close() }()
			if err := answer(conn, clientUID); err != nil {
				log.Println("execd:", err)
			}
		}()
	}
}

// Call treats the reply as hostile: bounded in size and by ctx, and refused
// when its output is past the cap.
func Call(ctx context.Context, socket string, cmd *pb.Command) (*pb.Result, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	if _, err := protodelim.MarshalTo(conn, cmd); err != nil {
		return nil, err
	}
	res := &pb.Result{}
	if err := (protodelim.UnmarshalOptions{MaxSize: maxReply}).UnmarshalFrom(bufio.NewReader(conn), res); err != nil {
		return nil, fmt.Errorf("execd reply: %w", err)
	}
	if len(res.GetOutput()) > maxOutput {
		return nil, fmt.Errorf("execd reply: %d bytes of output, past the %d cap", len(res.GetOutput()), maxOutput)
	}
	return res, nil
}

func answer(conn *net.UnixConn, clientUID uint32) error {
	uid, err := peerUID(conn)
	if err != nil {
		return err
	}
	if uid != clientUID {
		return fmt.Errorf("refused uid %d", uid)
	}
	cmd := &pb.Command{}
	if err := protodelim.UnmarshalFrom(bufio.NewReader(conn), cmd); err != nil {
		return err
	}
	res, err := run(cmd)
	if err != nil {
		return err
	}
	_, err = protodelim.MarshalTo(conn, res)
	return err
}

func run(c *pb.Command) (*pb.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.GetTimeout().AsDuration())
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", c.GetCommand())
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	out := &capped{}
	cmd.Stdout, cmd.Stderr = out, out
	err := cmd.Run()
	if cmd.ProcessState == nil {
		return nil, err
	}
	return &pb.Result{Output: out.bytes, Truncated: out.truncated, ExitCode: int32(cmd.ProcessState.ExitCode())}, nil
}

type capped struct {
	bytes     []byte
	truncated bool
}

func (c *capped) Write(p []byte) (int, error) {
	n := min(len(p), maxOutput-len(c.bytes))
	c.bytes = append(c.bytes, p[:n]...)
	c.truncated = c.truncated || n < len(p)
	return len(p), nil
}

func peerUID(conn *net.UnixConn) (uint32, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid uint32
	var uidErr error
	if err := raw.Control(func(fd uintptr) { uid, uidErr = fdPeerUID(int(fd)) }); err != nil {
		return 0, err
	}
	return uid, uidErr
}
