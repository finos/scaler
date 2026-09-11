
from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule


def parse_waterfall_rules(policy_content: str) -> list[WaterfallRule]:
    """Parse waterfall rules from policy_content.

    Expected format (one rule per line, ``#`` comments supported)::

        #priority,worker_manager_id[,max_task_concurrency]
        1,native
        2,ecs,20

    When ``max_task_concurrency`` is omitted, the manager's heartbeat-reported capacity is used.

    Raises ``ValueError`` on malformed input.
    """
    rules: list[WaterfallRule] = []
    for line_number, raw_line in enumerate(policy_content.splitlines(), start=1):
        # Strip inline comments
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) not in (2, 3):
            raise ValueError(
                f"waterfall_v1 policy_content line {line_number}: "
                f"expected 'priority,worker_manager_id[,max_task_concurrency]', got {raw_line.strip()!r}"
            )

        raw_priority, worker_manager_id = parts[0], parts[1]
        raw_max_task_concurrency = parts[2] if len(parts) == 3 else None

        if not worker_manager_id:
            raise ValueError(f"waterfall_v1 policy_content line {line_number}: worker_manager_id cannot be empty")

        rules.append(
            WaterfallRule(
                priority=int(raw_priority),
                worker_manager_id=worker_manager_id.encode(),
                max_task_concurrency=int(raw_max_task_concurrency) if raw_max_task_concurrency is not None else None,
            )
        )

    if not rules:
        raise ValueError("waterfall_v1 policy_content: no rules specified")

    seen: set = set()
    for rule in rules:
        if rule.worker_manager_id in seen:
            raise ValueError(f"waterfall_v1 policy_content: duplicate worker_manager_id {rule.worker_manager_id!r}")
        seen.add(rule.worker_manager_id)

    return rules
