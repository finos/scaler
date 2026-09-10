"""Integration test: end-to-end zero-copy pipeline through YMQ Bytes and Cap'n Proto lazy deserialization."""

import unittest

from scaler.io.utility import serialize
from scaler.io.ymq import Bytes
from scaler.protocol.capnp import Message, StateTask, TaskState


class TestZeroCopyEndToEnd(unittest.TestCase):
    def test_ymq_bytes_and_capnp_deserialization_are_zero_copy(self):
        """Demonstrates the full zero-copy pipeline between the Python Capnp and Python YMQ modules"""

        # serialize an inactive and success message
        inactive_wire = serialize(StateTask(state=TaskState.inactive, taskId=b"t", functionName=b"f", worker=b"w"))
        success_wire = serialize(StateTask(state=TaskState.success, taskId=b"t", functionName=b"f", worker=b"w"))
        self.assertEqual(len(inactive_wire), len(success_wire))

        # create a mutable byte array of the inactive msg
        buf = bytearray(inactive_wire)

        # create a YMQ Bytes from python - zero copy
        ymq_bytes = Bytes(buf)

        # deserialize into a capnp message - also zero copy
        msg = Message.from_bytes(ymq_bytes)

        # change the contents of the buffer to be the success message
        # if `msg` is reading from `buf` as it should in zero copy
        # then this should be reflected in the value of `msg.stateTask.state`
        buf[:] = success_wire
        self.assertEqual(msg.stateTask.state, TaskState.success)
