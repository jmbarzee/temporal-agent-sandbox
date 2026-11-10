package main

import (
	"go.uber.org/fx"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/sidecar"
)

func main() {
	fx.New(workers.Module, sidecar.Module).Run()
}
