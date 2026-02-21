"""Memory-based backpressure utilities for job submission throttling."""
import os
from typing import Tuple


def get_memory_usage_percent() -> float:
    """
    Get current container memory usage as percentage (0-100).
    Reads from cgroup v2 (Docker default) or falls back to v1.

    Returns 0.0 if memory limits can't be determined (allows jobs to proceed).
    """
    try:
        # cgroup v2 (modern Docker)
        with open('/sys/fs/cgroup/memory.current', 'r') as f:
            current = int(f.read().strip())
        with open('/sys/fs/cgroup/memory.max', 'r') as f:
            max_mem = f.read().strip()
            if max_mem == 'max':
                return 0.0  # No limit set
            max_mem = int(max_mem)
        return (current / max_mem) * 100
    except FileNotFoundError:
        try:
            # cgroup v1 fallback
            with open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r') as f:
                current = int(f.read().strip())
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                max_mem = int(f.read().strip())
            return (current / max_mem) * 100
        except Exception:
            return 0.0  # Can't determine, assume OK


def get_memory_usage_bytes() -> Tuple[int, int]:
    """
    Get current and max memory in bytes.
    Returns (current_bytes, max_bytes) or (0, 0) if can't determine.
    """
    try:
        # cgroup v2
        with open('/sys/fs/cgroup/memory.current', 'r') as f:
            current = int(f.read().strip())
        with open('/sys/fs/cgroup/memory.max', 'r') as f:
            max_mem = f.read().strip()
            if max_mem == 'max':
                return (current, 0)
            max_mem = int(max_mem)
        return (current, max_mem)
    except FileNotFoundError:
        try:
            # cgroup v1 fallback
            with open('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'r') as f:
                current = int(f.read().strip())
            with open('/sys/fs/cgroup/memory/memory.limit_in_bytes', 'r') as f:
                max_mem = int(f.read().strip())
            return (current, max_mem)
        except Exception:
            return (0, 0)


def should_submit_jobs(threshold_percent: float = 80.0) -> bool:
    """
    Return True if memory usage is below threshold and jobs should be submitted.

    Args:
        threshold_percent: Memory usage threshold (0-100). Default 80%.

    Returns:
        True if safe to submit jobs, False if backpressure should be applied.
    """
    usage = get_memory_usage_percent()
    return usage < threshold_percent


def format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"
