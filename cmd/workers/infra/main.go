package main

import (
	"go.uber.org/fx"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/infra"
)

func main() {
	fx.New(workers.Module, infra.Module).Run()
}
