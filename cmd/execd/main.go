package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"os/exec"
	"syscall"

	execd "github.com/jmbarzee/temporal-agent-sandbox/lib/execd/v1"
)

func main() {
	socket := flag.String("socket", "", "the Unix socket to listen on")
	clientUID := flag.Uint("client-uid", 0, "the one UID whose commands run; never root")
	flag.Parse()
	if *socket == "" || *clientUID == 0 {
		log.Fatal("usage: execd -socket PATH -client-uid UID")
	}
	if os.Getpid() == 1 {
		reapWhileServing()
	}
	ln, err := execd.Listen(*socket)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(execd.Serve(ln, uint32(*clientUID)))
}

// reapWhileServing reaps the orphans PID 1 inherits, and serves from a child
// so the reaper cannot take the exit statuses os/exec waits for.
func reapWhileServing() {
	self, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	server := exec.Command(self, os.Args[1:]...)
	server.Stdout, server.Stderr = os.Stdout, os.Stderr
	if err := server.Start(); err != nil {
		log.Fatal(err)
	}
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}
		if pid == server.Process.Pid {
			os.Exit(status.ExitStatus())
		}
	}
}
