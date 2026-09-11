from typing import Optional

import psutil

# cgroup memory controller paths: v2 (unified hierarchy) first, then v1.
_CGROUP_V2_MAX = "/sys/fs/cgroup/memory.max"
_CGROUP_V2_CURRENT = "/sys/fs/cgroup/memory.current"
_CGROUP_V1_LIMIT = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
_CGROUP_V1_USAGE = "/sys/fs/cgroup/memory/memory.usage_in_bytes"

# cgroup v1 reports "unlimited" as a huge page-aligned sentinel near INT64_MAX; anything above this is
# treated as no limit rather than a real ceiling.
_CGROUP_UNLIMITED_THRESHOLD = 2**62


def get_process_memory(process: psutil.Process) -> int:
    """Return a process's memory footprint in bytes: PSS on Linux, RSS where PSS is unavailable.

    PSS (proportional set size) splits shared pages across the processes that map them, so summing PSS
    across a worker and its forked processors does not double-count the copy-on-write pages they share.
    psutil only exposes ``pss`` where the kernel does (Linux, via smaps), so on Linux this is always PSS;
    on macOS/Windows ``memory_full_info()`` has no ``pss`` field and RSS is used. psutil errors (e.g. the
    process has exited) propagate for the caller to handle.

    On macOS ``memory_full_info()`` reads USS through ``task_for_pid``, which requires elevated privileges
    and raises a permission error under System Integrity Protection / the hardened runtime even for the
    caller's own process. ``memory_info().rss`` reads through ``proc_pidinfo`` and needs no such privilege,
    so we fall back to RSS on a permission denial rather than let the heartbeat routine crash.
    """
    try:
        memory = process.memory_full_info()
    except (psutil.AccessDenied, PermissionError):
        return int(process.memory_info().rss)
    return int(getattr(memory, "pss", memory.rss))


def _read_cgroup_int(path: str) -> Optional[int]:
    try:
        with open(path) as cgroup_file:
            value = cgroup_file.read().strip()
    except OSError:
        return None
    if value in ("max", ""):  # cgroup v2 encodes "no limit" as the literal "max"
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _cgroup_limit_and_usage() -> Optional[tuple[int, int]]:
    """Return (limit_bytes, usage_bytes) from the cgroup memory controller, or None when the process is
    not under a memory-limited cgroup. Prefers cgroup v2, falls back to v1."""
    limit = _read_cgroup_int(_CGROUP_V2_MAX)
    if limit is not None:
        usage = _read_cgroup_int(_CGROUP_V2_CURRENT)
        if usage is not None:
            return limit, usage

    limit = _read_cgroup_int(_CGROUP_V1_LIMIT)
    if limit is not None and limit < _CGROUP_UNLIMITED_THRESHOLD:
        usage = _read_cgroup_int(_CGROUP_V1_USAGE)
        if usage is not None:
            return limit, usage

    return None


def get_memory_limit_and_available() -> tuple[int, int]:
    """Return (limit_bytes, available_bytes) for the memory ceiling the process actually runs under.

    Inside a memory-limited cgroup (e.g. a Kubernetes pod) this is the cgroup limit and its headroom
    (limit - current usage) -- the numbers that predict an OOM kill. Otherwise it falls back to the host's
    total and available memory from /proc/meminfo, so bare-metal workers still report something sensible.
    """
    cgroup = _cgroup_limit_and_usage()
    if cgroup is not None:
        limit, usage = cgroup
        return limit, max(0, limit - usage)

    virtual = psutil.virtual_memory()
    return virtual.total, virtual.available
