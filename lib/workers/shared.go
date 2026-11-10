package workers

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	"github.com/jmbarzee/temporal-agent-sandbox/lib/healthcheck"
	"github.com/jmbarzee/temporal-agent-sandbox/lib/workers/config"
)

// Module is what every worker binary shares: config, client, worker, and
// the lifecycle that runs them. Each worker adds its own registrations.
var Module = fx.Options(
	fx.Provide(config.FromEnv, NewClient, fx.Annotate(NewWorker, fx.As(fx.Self()), fx.As(new(worker.ActivityRegistry)))),
	fx.Invoke(StartHealthcheck, RunWorker),
)

func NewClient(cfg *config.Config) (client.Client, error) {
	return client.Dial(client.Options{HostPort: cfg.TemporalAddress, Namespace: cfg.TemporalNamespace})
}

func NewWorker(c client.Client, cfg *config.Config) worker.Worker {
	return worker.New(c, cfg.TaskQueue, worker.Options{})
}

func StartHealthcheck(lc fx.Lifecycle, c client.Client, cfg *config.Config) {
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		go func() {
			if err := healthcheck.NewServer(c).Run(cfg.HealthAddress); err != nil {
				log.Fatalln("healthcheck:", err)
			}
		}()
		return nil
	}})
}

// RunWorker starts the worker after every fx.Invoke has registered on it.
func RunWorker(lc fx.Lifecycle, w worker.Worker, cfg *config.Config) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				log.Println("polling task queue", cfg.TaskQueue)
				if err := w.Run(worker.InterruptCh()); err != nil {
					log.Fatalln("worker:", err)
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			w.Stop()
			return nil
		},
	})
}
