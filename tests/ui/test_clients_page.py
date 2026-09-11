"""The Clients page, and the client each task is attributed to.

The scheduler reports one `ClientStatus` per connected client, and every `StateTask` names its client.
The GUI joins the two to say who a task belongs to and how much work each client has in flight.
"""

import unittest

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    Resource,
    ScalingManagerStatus,
    StateScheduler,
    StateTask,
    TaskManagerStatus,
    TaskState,
    WorkerManagerStatus,
)
from scaler.ui.app import WebGUIConfig, WebUIApp


def make_app() -> WebUIApp:
    return WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))


def make_status(*clients: ClientManagerStatus.ClientStatus) -> StateScheduler:
    return StateScheduler(
        binder=BinderStatus(received=[], sent=[]),
        scheduler=Resource(cpu=0, rss=0),
        rssFree=0,
        clientManager=ClientManagerStatus(clients=list(clients)),
        objectManager=ObjectManagerStatus(numberOfObjects=0),
        taskManager=TaskManagerStatus(stateToCount=[]),
        workerManager=WorkerManagerStatus(workers=[]),
        scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
    )


def make_task(**kwargs) -> StateTask:
    """A StateTask as the GUI receives it: capability reads need a deserialized struct."""
    return StateTask.from_bytes(StateTask(**kwargs).to_bytes())


def make_client(name: bytes, num_task: int = 0) -> ClientManagerStatus.ClientStatus:
    return ClientManagerStatus.ClientStatus(
        clientId=name,
        numTask=num_task,
        resource=Resource(cpu=125, rss=2_000_000),
        latencyUS=1500,
        lastSeenS=2,
        connectedS=90,
        hostname="box-1",
    )


class TestClientsPage(unittest.TestCase):
    def test_a_connected_client_becomes_a_row(self) -> None:
        app = make_app()
        app._process_clients(make_status(make_client(b"Client|one", num_task=3)))

        rows = app._clients_section()["clients"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["full_client"], "Client|one")
        self.assertEqual(rows[0]["host"], "box-1")
        self.assertEqual(rows[0]["tasks"], 3)
        self.assertEqual(rows[0]["cpu"], "12.5%")
        self.assertEqual(rows[0]["connected"], "1m30s")

    def test_a_departed_client_leaves_the_page_with_its_totals(self) -> None:
        app = make_app()
        app._process_clients(make_status(make_client(b"Client|one")))
        app._count_task_outcome("Client|one", TaskState.success)
        app._process_clients(make_status())

        self.assertEqual(app._clients_section()["clients"], [])
        self.assertEqual(app._client_task_totals, {})

    def test_outcomes_are_tallied_against_the_submitting_client(self) -> None:
        app = make_app()
        for state in (TaskState.success, TaskState.success, TaskState.failed, TaskState.failedWorkerDied):
            app._count_task_outcome("Client|one", state)
        app._process_clients(make_status(make_client(b"Client|one")))

        row = app._clients_section()["clients"][0]
        self.assertEqual((row["finished"], row["failed"]), (4, 2))


class TestTaskAttribution(unittest.TestCase):
    def test_a_task_row_names_its_client(self) -> None:
        app = make_app()
        entry = app._process_task_state(
            make_task(taskId=b"t1", functionName=b"square", state=TaskState.running, worker=b"w1", client=b"Client|one")
        )

        self.assertEqual(entry["full_client"], "Client|one")

    def test_a_result_without_the_client_keeps_the_one_the_task_had(self) -> None:
        app = make_app()
        app._process_task_state(
            make_task(taskId=b"t1", functionName=b"square", state=TaskState.running, worker=b"w1", client=b"Client|one")
        )
        entry = app._process_task_state(
            make_task(taskId=b"t1", functionName=b"square", state=TaskState.success, worker=b"w1", client=b"")
        )

        self.assertEqual(entry["full_client"], "Client|one")
        self.assertEqual(app._client_task_totals["Client|one"], {"finished": 1, "failed": 0})


if __name__ == "__main__":
    unittest.main()
