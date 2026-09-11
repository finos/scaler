"""
Nested worker managers.

One unit is a platform resource - an EC2 instance, an ECS task, an OCI Container Instance - that
runs a native worker manager of its own. That child manager owns the worker processes, so a nested
manager commands a child manager rather than commanding workers directly. Each adapter here builds
the child's command line with the helpers in `child_command.py`.
"""
