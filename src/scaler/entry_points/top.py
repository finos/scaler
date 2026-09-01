# PYTHON_ARGCOMPLETE_OK
import curses
import functools
from typing import Literal, Union

from scaler.config.section.top import TopConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import BaseMessage, StateScheduler, TaskState
from scaler.utility.formatter import (
    format_bytes,
    format_integer,
    format_microseconds,
    format_percentage,
    format_seconds,
)

SORT_BY_OPTIONS = {
    ord("g"): "manager",
    ord("n"): "worker",
    ord("C"): "agt_cpu",
    ord("M"): "agt_rss",
    ord("c"): "cpu",
    ord("m"): "rss",
    ord("F"): "rss_free",
    ord("f"): "free",
    ord("w"): "sent",
    ord("d"): "queued",
    ord("s"): "suspended",
    ord("l"): "lag",
}

SORT_BY_STATE: dict[str, Union[str, bool]] = {"sort_by": "cpu", "sort_by_previous": "cpu", "sort_reverse": True}


def main():
    curses.wrapper(poke, TopConfig.parse("A Top-like Application for Scaler Monitoring", "top"))


def poke(screen, config: TopConfig):
    screen.nodelay(1)

    identity = generate_identity_from_name("top")
    backend = get_network_backend_from_env()

    try:
        subscriber = backend.create_sync_subscriber(
            identity=identity,
            address=config.monitor_address,
            callback=functools.partial(show_status, screen=screen),
            timeout=config.timeout,
            security_config=config.security,
        )
        subscriber.run()
    except KeyboardInterrupt:
        pass
    finally:
        backend.destroy()


def show_status(status: BaseMessage, screen):
    if not isinstance(status, StateScheduler):
        return

    __change_option_state(screen.getch())

    scheduler_table = __generate_keyword_data(
        "scheduler",
        {
            "cpu": format_percentage(status.scheduler.cpu),
            "rss": format_bytes(status.scheduler.rss),
            "rss_free": format_bytes(status.rssFree),
        },
    )

    task_manager_table = __generate_keyword_data(
        "task_manager",
        dict(sorted((TaskState(pair.state).name, pair.count) for pair in status.taskManager.stateToCount)),
        format_integer_flag=True,
    )
    object_manager = __generate_keyword_data("object_manager", {"num_of_objs": status.objectManager.numberOfObjects})
    sent_table = __generate_keyword_data(
        "scheduler_sent", {pair.client: pair.number for pair in status.binder.sent}, format_integer_flag=True
    )
    received_table = __generate_keyword_data(
        "scheduler_received", {pair.client: pair.number for pair in status.binder.received}, format_integer_flag=True
    )
    client_table = __generate_keyword_data(
        "client_manager",
        {
            pair.client.decode() if isinstance(pair.client, (bytes, bytearray)) else str(pair.client): pair.numTask
            for pair in status.clientManager.clientToNumOfTask
        },
        key_col_length=18,
    )

    manager_map = {}
    if status.scalingManager.managedWorkers:
        for managed_worker in status.scalingManager.managedWorkers:
            manager_id_str = managed_worker.workerManagerID.decode()
            for worker_id in managed_worker.workerIDs:
                manager_map[worker_id.decode()] = manager_id_str

    # Include 'manager' as the first column for each worker; empty if not found
    worker_manager_table = __generate_worker_manager_table(
        [
            {
                "manager": manager_map.get(worker.workerId.decode(), ""),
                "worker": worker.workerId.decode(),
                "agt_cpu": worker.agent.cpu,
                "agt_rss": worker.agent.rss,
                "cpu": sum(p.resource.cpu for p in worker.processorStatuses),
                "rss": sum(p.resource.rss for p in worker.processorStatuses),
                "os_rss_free": worker.rssFree,
                "free": worker.free,
                "sent": worker.sent,
                "queued": worker.queued,
                "suspended": worker.suspended,
                "lag": worker.lagUS,
                "last": worker.lastS,
                "ITL": worker.itl,
            }
            for worker in status.workerManager.workers
        ],
        manager_length=10,
        worker_length=20,
    )

    table1 = __merge_tables(scheduler_table, object_manager, padding="|")
    table1 = __merge_tables(table1, task_manager_table, padding="|")
    table1 = __merge_tables(table1, sent_table, padding="|")
    table1 = __merge_tables(table1, received_table, padding="|")

    table3 = __merge_tables(worker_manager_table, client_table, padding="|")

    screen.clear()
    try:
        new_row, max_cols = __print_table(screen, 0, table1, padding=1)
    except curses.error:
        __print_too_small(screen)
        return

    try:
        screen.addstr(new_row, 0, "-" * max_cols)
        screen.addstr(new_row + 1, 0, "Shortcuts: " + " ".join([f"{v}[{chr(k)}]" for k, v in SORT_BY_OPTIONS.items()]))
        total_pending = sum(d.pendingWorkers for d in status.scalingManager.workerManagerDetails)
        pending_str = f", {total_pending} pending" if total_pending > 0 else ""
        screen.addstr(
            new_row + 3,
            0,
            f"Total {len(status.scalingManager.managedWorkers)} manager(s) "
            f"with {len(status.workerManager.workers)} worker(s){pending_str}",
        )
        _ = __print_table(screen, new_row + 4, table3)
    except curses.error:
        pass

    screen.refresh()


def __generate_keyword_data(title, data, key_col_length: int = 0, format_integer_flag: bool = False):
    table = [[title, ""]]

    def format_integer_func(value):
        if format_integer_flag:
            return format_integer(value)

        return value

    table.extend([[__truncate(k, key_col_length), format_integer_func(v)] for k, v in data.items()])
    return table


def __generate_worker_manager_table(wm_data: list[dict], manager_length: int, worker_length: int) -> list[list[str]]:
    if not wm_data:
        headers = [["No workers"]]
        return headers

    wm_data = sorted(
        wm_data, key=lambda item: item[SORT_BY_STATE["sort_by"]], reverse=bool(SORT_BY_STATE["sort_reverse"])
    )

    for row in wm_data:
        row["manager"] = __truncate(row["manager"], manager_length, how="left")
        row["worker"] = __truncate(row["worker"], worker_length, how="left")
        row["agt_cpu"] = format_percentage(row["agt_cpu"])
        row["agt_rss"] = format_bytes(row["agt_rss"])
        row["cpu"] = format_percentage(row["cpu"])
        row["rss"] = format_bytes(row["rss"])
        row["os_rss_free"] = format_bytes(row["os_rss_free"])

        last = row.pop("last")
        last = f"({format_seconds(last)}) " if last > 5 else ""
        row["lag"] = last + format_microseconds(row["lag"])

    worker_manager_table = [[f"[{v}]" if v == SORT_BY_STATE["sort_by"] else v for v in wm_data[0].keys()]]
    worker_manager_table.extend([list(worker.values()) for worker in wm_data])
    return worker_manager_table


def __print_table(screen, line_number, data, padding: int = 1):
    if not data:
        return

    col_widths = [max(len(str(row[i])) for row in data) for i in range(len(data[0]))]

    for i, header in enumerate(data[0]):
        screen.addstr(line_number, sum(col_widths[:i]) + (padding * i), str(header).rjust(col_widths[i]))

    for i, row in enumerate(data[1:], start=1):
        for j, cell in enumerate(row):
            screen.addstr(line_number + i, sum(col_widths[:j]) + (padding * j), str(cell).rjust(col_widths[j]))

    return line_number + len(data), sum(col_widths) + (padding * len(col_widths))


def __merge_tables(left: list[list], right: list[list], padding: str = "") -> list[list]:
    if not left:
        return right

    if not right:
        return left

    result = []
    for i in range(max(len(left), len(right))):
        if i < len(left):
            left_row = left[i]
        else:
            left_row = [""] * len(left[0])

        if i < len(right):
            right_row = right[i]
        else:
            right_row = [""] * len(right[0])

        if padding:
            padding_column = [padding]
            result.append(left_row + padding_column + right_row)
        else:
            result.append(left_row + right_row)

    return result


def __concat_tables(up: list[list], down: list[list], padding: int = 1) -> list[list]:
    max_cols = max([len(row) for row in up] + [len(row) for row in down])
    for row in up:
        row.extend([""] * (max_cols - len(row)))

    padding_rows = [[""] * max_cols] * padding

    for row in down:
        row.extend([""] * (max_cols - len(row)))

    return up + padding_rows + down


def __truncate(string: str, number: int, how: Literal["left", "right"] = "left") -> str:
    if number <= 0:
        return string

    if len(string) <= number:
        return string

    if how == "left":
        return f"{string[:number]}+"
    else:
        return f"+{string[-number:]}"


def __print_too_small(screen):
    screen.clear()
    screen.addstr(0, 0, "Your terminal is too small to show")
    screen.refresh()


def __change_option_state(option: int):
    if option not in SORT_BY_OPTIONS.keys():
        return

    SORT_BY_STATE["sort_by_previous"] = SORT_BY_STATE["sort_by"]
    SORT_BY_STATE["sort_by"] = SORT_BY_OPTIONS[option]
    if SORT_BY_STATE["sort_by"] != SORT_BY_STATE["sort_by_previous"]:
        SORT_BY_STATE["sort_reverse"] = True
        return

    SORT_BY_STATE["sort_reverse"] = not SORT_BY_STATE["sort_reverse"]
