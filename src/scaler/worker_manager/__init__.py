"""
Worker managers.

A worker manager keeps the scheduler supplied with workers. It heartbeats to the scheduler, reads
back the desired task concurrency, and asks its provisioner to converge on it. `runner.py` drives
that loop, `capacity_coordinator.py` rate-limits the scaling, and `mixins.py` declares the
`DeclarativeWorkerProvisioner` interface that all seven adapters implement.

The adapters are grouped by what a provisioner unit is:

- `native/` - one local worker process, spawned directly.
- `nested/` - one platform resource (an EC2 instance, an ECS task, an OCI Container Instance) that
  runs a native worker manager of its own, which in turn owns the worker processes.
- `proxy/` - one local proxy worker process that looks like an ordinary worker to the scheduler but
  submits each task to an external execution service and reports the result back.
"""
