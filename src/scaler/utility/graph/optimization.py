from collections import deque
from typing import Any, Callable, Union


def cull_graph(
    graph: dict[str, tuple[Union[Callable, Any], ...]], keys: list[str]
) -> dict[str, tuple[Union[Callable, Any], ...]]:
    queue = deque(keys)
    visited = set()
    for target_key in keys:
        visited.add(target_key)

    while queue:
        key = queue.popleft()

        task = graph[key]
        if not (isinstance(task, tuple) and task and callable(task[0])):
            continue

        dependencies = set(task[1:])
        for predecessor_key in dependencies:
            if predecessor_key in visited:
                continue
            visited.add(predecessor_key)
            queue.append(predecessor_key)

    return {key: graph[key] for key in visited}
