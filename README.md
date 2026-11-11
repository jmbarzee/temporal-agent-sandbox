# temporal-agent-sandbox

A small, working example of an LLM agent run as a durable [Temporal](https://temporal.io) workflow, whose tool calls execute inside a disposable Kubernetes pod.

The conversation is workflow state, so it survives a worker restart. The commands the model asks for run in a pod the agent holds no credentials for, leased while the agent works and deleted once nobody holds it.

## The pieces

![Architecture: a workflow starter, three Temporal workers, and a pod per sandbox](docs/architecture.svg)

- **Agent** — one LLM conversation. Each turn calls the model, then runs the tool calls it returns on the sandbox's own task queue. It renews its lease before expiry and continues-as-new every 50 turns.
- **SandboxOrchestrator** — a singleton that starts one Sandbox per request and tells the agent its id.
- **Sandbox** — owns one pod: renders a Helm chart, creates the pod, waits until it is ready, grants leases, and deletes the pod once it has gone unleased past a grace period.
- **Sidecar** — a Temporal worker inside the pod. It runs each command through `execd`, an exec server in the sandbox container that only the sidecar's UID may call.

Only the sandboxes worker holds cluster credentials; agents reach sandboxes through signals and task queues alone.

## One run

![One run: the agent asks the distributor for a sandbox, then leases it from the sandbox instance for each turn and releases it after; a user prompt starts the second turn](docs/workflows.svg)

The sandbox distributor is the `SandboxOrchestrator` workflow; each sandbox instance is a `Sandbox` workflow.

1. The agent asks the distributor for a sandbox.
2. The distributor starts a `Sandbox` as a detached child, which renders its chart, creates the pod, and waits until it is ready.
3. The distributor tells the agent the sandbox's id.
4. The agent asks that sandbox for a lease, and (5) the sandbox grants it once the pod is ready. Holding the lease, the agent calls the model and runs the tool calls it returns. `RunTool` is the agent's activity, but it runs on the task queue `sandbox-{id}`, which only that pod's sidecar polls.
6. The agent releases the lease, and the sandbox starts its grace timer.
7. A user prompt arrives, and (8, 9) the agent leases the same sandbox again before the grace runs out.
10. The agent releases again; this time the grace timer fires and the sandbox deletes the pod.

The diagram simplifies twice. The agent here works a single task and has no `UserPrompt` signal; the second turn shows how a conversational agent would reuse its sandbox. And it leaves out lease renewal.

## Why it is shaped this way

- **Leases bound every pod's life.** A crashed agent never leaks a pod: its lease expires, the grace passes, and the sandbox deletes the pod.
- **A task queue per sandbox** routes each tool call to exactly one pod. A `schedule_to_start` timeout on `RunTool` notices a lost pod, and the call never retries, because a command may not be safe to run twice.
- **The design leads the code.** `twf/` declares every workflow, activity, worker, and queue in [TWF](https://www.npmjs.com/package/@temporal-architect/twf), and `make check` validates it.

## Layout

| Path | |
|---|---|
| `twf/` | The design |
| `proto/`, `gen/`, `lib/` | Contracts, generated Temporal stubs, implementations |
| `cmd/workers/`, `lib/workers/` | The three workers |
| `cmd/execd`, `lib/execd/` | The exec server |
| `helm/sandbox` | The sandbox pod chart |
| `e2e/` | Runs an agent through every worker against k3s and a stub model |

`make help` lists targets. `make test` runs unit and replay tests; `make test-integration` needs Docker.
