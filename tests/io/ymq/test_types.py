import array
import pickle
import unittest
from enum import IntEnum

import cloudpickle

from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import Address, AddressType, Bytes, ErrorCode, IOContext, Message, SysCallError, YMQException
from scaler.protocol.capnp import TaskCancel
from scaler.utility.identifiers import TaskID


class TestTypes(unittest.TestCase):
    def test_exception(self):
        # type checkers misidentify this as "unnecessary" due to the type hints file
        self.assertTrue(issubclass(YMQException, Exception))

        self.assertTrue(issubclass(SysCallError, YMQException))

        exc = SysCallError(ErrorCode.SysCallError, "oh no")
        self.assertEqual(exc.args, (ErrorCode.SysCallError, "oh no"))
        self.assertEqual(exc.code, ErrorCode.SysCallError)
        self.assertEqual(exc.message, "oh no")
        self.assertIsInstance(exc, YMQException)
        self.assertIsInstance(exc, SysCallError)

    def test_exception_pickle_round_trip(self):
        for exception_type in (YMQException, SysCallError):
            with self.subTest(exception_type=exception_type):
                original = exception_type(ErrorCode.SysCallError, "oh no")

                restored = pickle.loads(pickle.dumps(original))

                self.assertIs(type(restored), exception_type)
                self.assertEqual(restored.args, (ErrorCode.SysCallError, "oh no"))
                self.assertEqual(restored.code, ErrorCode.SysCallError)
                self.assertEqual(restored.message, "oh no")

    def test_exception_cloudpickle_round_trip(self):
        for exception_type in (YMQException, SysCallError):
            with self.subTest(exception_type=exception_type):
                original = exception_type(ErrorCode.SysCallError, "oh no")

                restored = cloudpickle.loads(cloudpickle.dumps(original))

                self.assertIs(type(restored), exception_type)
                self.assertEqual(restored.args, (ErrorCode.SysCallError, "oh no"))
                self.assertEqual(restored.code, ErrorCode.SysCallError)
                self.assertEqual(restored.message, "oh no")

    def test_error_code(self):
        self.assertTrue(issubclass(ErrorCode, IntEnum))
        self.assertEqual(
            ErrorCode.InvalidAddressFormat.explanation(),
            'Invalid address format, example input "tcp://127.0.0.1:2345" or "ipc:///tmp/domain_socket_name.sock"',
        )

    def test_bytes(self):
        b = Bytes(b"data")
        self.assertEqual(b.len, len(b))
        self.assertEqual(b.len, 4)
        self.assertEqual(b.data, b"data")

        # would raise an exception if Bytes didn't support the buffer interface
        m = memoryview(b)
        self.assertTrue(m.obj is b)
        self.assertEqual(m.tobytes(), b"data")

        b = Bytes()
        self.assertEqual(b.len, 0)
        self.assertTrue(b.data is None)

        b = Bytes(b"")
        self.assertEqual(b.len, 0)
        self.assertEqual(b.data, b"")

        b = Bytes(array.array("B", [115, 99, 97, 108, 101, 114]))
        assert b.len == 6
        assert b.data == b"scaler"

    def test_message(self):
        m = Message(b"address", b"payload")
        assert m.address is not None
        self.assertEqual(m.address.data, b"address")
        self.assertEqual(m.payload.data, b"payload")

        m = Message(address=None, payload=Bytes(b"scaler"))
        self.assertTrue(m.address is None)
        self.assertEqual(m.payload.data, b"scaler")

        m = Message(b"address", payload=b"payload")
        assert m.address is not None
        self.assertEqual(m.address.data, b"address")
        self.assertEqual(m.payload.data, b"payload")

    def test_address(self):
        addr = Address("tcp://127.0.0.1:9000")
        self.assertEqual(addr.type, AddressType.TCP)
        self.assertEqual(repr(addr), "tcp://127.0.0.1:9000")

        addr = Address("tcp://::1:9000")
        self.assertEqual(addr.type, AddressType.TCP)
        self.assertEqual(repr(addr), "tcp://::1:9000")

        addr = Address("ipc://my_socket")
        self.assertEqual(addr.type, AddressType.IPC)
        self.assertEqual(str(addr), "ipc://my_socket")

        with self.assertRaises(YMQException):
            Address("invalid://address")

    def test_io_context(self):
        ctx = IOContext()
        self.assertEqual(ctx.num_threads, 1)

        ctx = IOContext(num_threads=3)
        self.assertEqual(ctx.num_threads, 3)

    def test_buffer_interface(self):
        msg = TaskCancel(taskId=TaskID.generate_task_id())
        data = serialize(msg)

        # verify that capnp can deserialize this data
        _ = deserialize(data)

        # this creates a copy of the data
        copy = Bytes(data)

        # this should deserialize without creating a copy
        # because Bytes uses the buffer protocol
        deserialized: TaskCancel = deserialize(copy)  # type: ignore
        self.assertEqual(deserialized.taskId, msg.taskId)
