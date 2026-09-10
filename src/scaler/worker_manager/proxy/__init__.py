"""
Proxy worker managers.

One unit is a local proxy worker process. The proxy holds the only connection to the scheduler and
looks like an ordinary worker to it, but instead of running tasks itself it submits each one to an
external execution service (AWS Batch, OCI Container Instances, IBM Spectrum Symphony) and reports
the result back. The shared proxy runtime lives in this package: `worker_process.py`,
`task_manager.py`, `heartbeat_manager.py` and the `mixins.py` an adapter implements.
"""
