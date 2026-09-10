# End-to-end scaling tests

A real client bursts work at a real scheduler; its scaling policy drives a real worker manager that boots
real workers in Docker containers. Two of the three backends run the **shipped** AWS worker managers
unmodified (only the boto3 endpoint and a couple of config values differ from production).

## Run

Each needs a Docker daemon, builds images, and takes minutes, so it is opt-in and runs one topology:

```bash
./scripts/e2e/run.sh container                 # container_waterfall | ecs | ec2 | ecs_ec2
DOCKER="sudo docker" ./scripts/e2e/run.sh ecs  # if the docker socket is root-only
```

- Builds any missing wheel/image, then runs `SCALER_E2E=<topology> python -m unittest
  tests.integration.test_scaling -v`.
- Prints `web GUI: http://localhost:PORT` to watch the pool scale live.
- In CI: the **E2E Scaling Tests** workflow — Actions tab (pick a topology) or an `e2e-<topology>` PR label.
- Each scenario prints its result on an `[e2e]` line; `grep '\[e2e\]'` is the run's summary. CI fails a run
  that produced none.

## Topologies

| `SCALER_E2E` | Manager(s) | What is real | Stands in for the cloud |
|---|---|---|---|
| `container` | one container manager | client, scheduler, workers in containers with their own IPs | nothing: a container *is* the machine |
| `container_waterfall` | two, priorities 1 (capped) and 2 | above + `waterfall_v1` spill | nothing |
| `ecs` | **shipped ECS manager** | its boto3 calls + workers in real ECS task containers | [floci](https://github.com/floci-io/floci) launches `RunTask` containers |
| `ec2` | **shipped ORB/EC2 manager** | the ORB SDK + its UserData + workers on real AL2023 instances | floci launches `RunInstances` containers |
| `ecs_ec2` | **both** AWS managers, one scheduler | ECS tasks + EC2 instances, spill and all | floci drives both |

floci is a free local AWS emulator that actually launches the containers it is asked to (moto and community
LocalStack do not), so the shipped managers run a real scale curve without an AWS account.

## Scenarios

Backend-agnostic (`scenarios.py`); each topology lists the ones it runs in `test_scaling.py`.

- `burst_and_drain` — a held burst brings the pool up with correct results; idle drains it to zero.
- `rising_load` — a sustained burst runs more units **at once** than a concurrency-1 trickle.
- `steady_load_stable` — a steady load settles on a stable pool (units created ≈ peak concurrent), not churn.
- `waterfall_spills` — (≥ 2 managers) a saturated top pool spills to the next tier.

## Known reds

Kept red until scaler is fixed — findings, not broken tests. The same scenario passing on another backend
is what distinguishes the two.

- `container` / `rising_load` + `steady_load_stable` — the vanilla policy has no scale-down cooldown, so
  where boot (~3s) outlasts task time (0.15s) it churns 0↔1 instead of climbing. The shipped ECS manager
  passes both.
- `ec2` / `steady_load_stable` — on scale-down the scheduler sends to a just-closed worker socket and
  `task_controller.__routing` re-raises `ConnectorSocketClosedByRemoteEndError`; the scheduler wedges.
  `assert_healthy` names it; the client `TimeoutError` is the downstream symptom.

## Layout

| File | |
|---|---|
| `test_scaling.py` | one class per topology — the whole matrix, top to bottom |
| `scenarios.py` | the assertions, written once, reused by every backend |
| `backends.py` | the three backends + the entry point that runs each manager in its own process |
| `framework.py` | `Profile`, `Deployment`, pool observation, and `deploy()` |
| `harness.py` | the real scheduler + object storage processes |
| `docker.py`, `floci.py` | the container CLI, the images, the emulator |

Two unifications let one scenario drive every backend: all workers run in Docker containers, so a pool is
one prefix-scoped `docker ps` (`ContainerPool`); and tasks are cloudpickled **by value** and tag their unit
with `SCALER_IT_MACHINE_ID or hostname`, covering the container backend's env id and the AWS managers'
hostname in one expression. `Deployment.assert_healthy` (every `tearDown`) fails **naming the fault** from a
tee'd scheduler log when the scheduler or a manager crashes or wedges, instead of only a client timeout.

## Tune a run

Set as `workflow_dispatch` inputs or exported before `run.sh`; blank falls back to the backend default.

- `SCALER_IT_NUM_TASKS` — tasks per burst wave.
- `SCALER_IT_TASK_SECONDS` — sleep per task. Timeouts are fixed, so a large value can exceed them.
- `SCALER_IT_WATERFALL_POLICY` — raw `waterfall_v1` policy for the waterfall topologies, one rule per line
  `priority,worker_manager_id[,max_task_concurrency]`. Ids are `wm-<backend>-p<position>` (logged at
  start-up); an unknown id fails fast. A cap here also sizes that manager's pool.
- Also: `SCALER_IT_CONTAINER_CLI` (default `docker`), `SCALER_IT_REBUILD=1`, `SCALER_IT_MANYLINUX_WHEEL_DIR`,
  `SCALER_IT_FLOCI_IMAGE`.

```bash
SCALER_IT_NUM_TASKS=60 SCALER_IT_TASK_SECONDS=0.3 \
  SCALER_IT_WATERFALL_POLICY=$'1,wm-ecs-p1,4\n2,wm-ec2-p2' ./scripts/e2e/run.sh ecs_ec2
```

## Extend

- **Scenario** — add a `(test_case, deployment)` function to `scenarios.py`; read timing off
  `deployment.profile`, submit via `deployment.tasks`, observe with `deployment.running()` /
  `deployment.handles`. List it on the topologies in `test_scaling.py`.
- **Backend** — add a class satisfying `WorkerManagerBackend` (a `Profile`, `ensure_image()`, `provision()`
  returning a `ManagerHandle` with a prefix-scoped `ContainerPool`). New shared infra (emulator, wheel
  server) goes in `deploy()` behind a `needs_*` flag.
- **Topology** — add a class to `test_scaling.py`: an ordered `ManagerSpec` list (priority = position; cap
  higher tiers to force spill) and its scenarios. Add its name to the workflow's `topology` choices.

## Notes

- `worker.Dockerfile` bakes in the `dist/` wheel + the custom capnp/kj libs, so a container matches the host
  scheduler's build and wire protocol with no bind-mounts. `ecs.Dockerfile` adds an entrypoint that execs
  `$COMMAND` (how the shipped ECS provisioner delivers the worker command).
- The `dist/` wheel is `linux_x86_64` and won't load on AL2023's older glibc, so the EC2 topologies build a
  `manylinux` wheel (`scripts/e2e/build_cibuildwheel.sh`) served over the bridge gateway; nothing compiles
  in the instance.
- Two floci shims (`ec2.Dockerfile`, `_install_floci_ec2_compat`) exist only because floci's AL2023 image
  lacks `tar` and its `DescribeLaunchTemplates` returns empty where real AWS raises — both harness-side, so
  the shipped provisioner is unmodified.
- The container backend keeps `--per-worker-task-queue-size 1` deliberately; dropping it breaks the
  scale-up scenarios.
