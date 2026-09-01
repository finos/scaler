"""Test that ``WebUIApp._process_worker_state`` exposes capabilities as a dict.

Downstream code in ``_process_scheduler`` calls ``.keys()`` on the stored capabilities,
so the handler must normalise the capnp ``list(TaskCapability)`` reader into a
``{name: value}`` dict keyed by capability name.
"""

import unittest
from unittest.mock import MagicMock

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import StateWorker, TaskCapability, WorkerState
from scaler.ui.app import WebGUIConfig, WebUIApp


class TestProcessWorkerStateExposesCapabilityKeys(unittest.TestCase):
    def test_connected_worker_capabilities_support_keys(self):
        config = WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380"))
        app = WebUIApp(config)
        # Stub the task stream so this test focuses solely on capability storage.
        app._task_stream = MagicMock()

        # Round-trip through capnp serialisation so the handler sees the same kind of
        # reader object it would receive off the wire (rather than a builder).
        wire_message = StateWorker.from_bytes(
            StateWorker(
                workerId=b"worker-1",
                state=WorkerState.connected,
                capabilities=[TaskCapability(name="gpu", value=4), TaskCapability(name="linux", value=-1)],
            ).to_bytes()
        )

        app._process_worker_state(wire_message)

        # _process_scheduler calls set(stored.keys()) on the stored capabilities, so the
        # stored value must be a mapping keyed by capability name.
        stored = app._worker_capabilities["worker-1"]
        self.assertEqual(set(stored.keys()), {"gpu", "linux"})


if __name__ == "__main__":
    unittest.main()
