import enum
import unittest

from scaler.protocol.capnp import StateTask, TaskState


class TestEnumTyping(unittest.TestCase):
    """Regression coverage for issues #749 and #761: capnp.pyi enum classes are
    proper IntEnum subclasses, deserialised fields are real IntEnum members,
    and unknown ordinals raise instead of silently aliasing."""

    def test_enum_member_name_value(self):
        self.assertEqual(TaskState.inactive.name, "inactive")
        self.assertEqual(TaskState.inactive.value, 0)
        self.assertEqual(TaskState.success.name, "success")
        self.assertEqual(TaskState.success.value, 4)

    def test_construct_from_int(self):
        self.assertEqual(TaskState(0).name, "inactive")
        self.assertEqual(TaskState(4).name, "success")
        self.assertEqual(TaskState(4).value, 4)

    def test_dict_key(self):
        mapping: dict[TaskState, str] = {
            TaskState.inactive: "init",
            TaskState.running: "run",
            TaskState.success: "done",
        }
        self.assertEqual(mapping[TaskState.inactive], "init")
        self.assertEqual(mapping[TaskState.success], "done")

    def test_is_real_enum(self):
        self.assertTrue(issubclass(TaskState, enum.Enum))
        self.assertTrue(issubclass(TaskState, enum.IntEnum))
        self.assertIsInstance(TaskState.inactive, enum.Enum)

    def test_intenum_int_compatibility(self):
        # IntEnum members are int subclasses
        self.assertEqual(TaskState.success, 4)
        self.assertEqual(int(TaskState.success), 4)

    def test_unknown_ordinal_raises(self):
        with self.assertRaises(ValueError):
            TaskState(999)

    def test_deserialized_field_name_value(self):
        st = StateTask.from_bytes(
            StateTask(state=TaskState.success, taskId=b"t1", functionName=b"fn", worker=b"w1").to_bytes()
        )
        self.assertEqual(st.state.name, "success")
        self.assertEqual(st.state.value, 4)
        self.assertEqual(st.state, TaskState.success)
        # Round-tripped value is the actual IntEnum member, not a wrapper
        self.assertIs(st.state, TaskState.success)


if __name__ == "__main__":
    unittest.main()
