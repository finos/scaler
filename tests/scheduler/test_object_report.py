"""What the scheduler reports about the objects it tracks.

The store knows object IDs and never sees a task, so the join between an object and its tasks exists only here.
"""

import unittest

from scaler.config import defaults
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import ObjectMetadata, Task
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.utility.identifiers import ClientID, ObjectID, TaskID


def make_config_controller() -> VanillaConfigController:
    return VanillaConfigController(
        SchedulerConfig(
            bind_address=AddressConfig.from_string("tcp://127.0.0.1:6378"),
            object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:6379"),
        )
    )


def make_object_controller(sizes) -> VanillaObjectController:
    controller = VanillaObjectController(config_controller=make_config_controller())
    client_id = ClientID.generate_client_id()
    for name, object_id, size in sizes:
        controller.on_add_object(client_id, object_id, ObjectMetadata.ObjectContentType.object, name)
        controller._object_sizes[bytes(object_id)] = size
    return controller


def make_task(client_id: ClientID, function_object_id: ObjectID, argument_object_ids) -> Task:
    return Task(
        taskId=TaskID.generate_task_id(),
        source=client_id,
        metadata=b"",
        funcObjectId=function_object_id,
        functionArgs=[
            Task.Argument(type=Task.Argument.ArgumentType.objectID, data=object_id) for object_id in argument_object_ids
        ],
        capabilities={},
    )


class TestLargestObjects(unittest.TestCase):
    def test_the_biggest_objects_come_first_and_the_list_is_bounded(self) -> None:
        client_id = ClientID.generate_client_id()
        ids = [ObjectID.generate_object_id(client_id) for _ in range(4)]
        controller = make_object_controller(
            [(b"small", ids[0], 10), (b"huge", ids[1], 4000), (b"medium", ids[2], 500), (b"tiny", ids[3], 1)]
        )

        largest = controller.get_largest_objects(2)

        self.assertEqual([detail.name for detail in largest], [b"huge", b"medium"])
        self.assertEqual([detail.size for detail in largest], [4000, 500])
        self.assertEqual(controller.object_count(), 4)

    def test_an_object_whose_client_reports_no_size_still_appears(self) -> None:
        client_id = ClientID.generate_client_id()
        object_id = ObjectID.generate_object_id(client_id)
        controller = VanillaObjectController(config_controller=make_config_controller())
        controller.on_add_object(client_id, object_id, ObjectMetadata.ObjectContentType.object, b"unsized")

        largest = controller.get_largest_objects(10)

        self.assertEqual([detail.name for detail in largest], [b"unsized"])
        self.assertEqual(largest[0].size, 0)


class TestObjectReportLimit(unittest.TestCase):
    def test_the_report_carries_what_the_scheduler_is_configured_to_send(self) -> None:
        """The limit is what a monitor can page through, and what each report costs."""
        controller = make_config_controller()
        self.assertEqual(controller.get_config("object_report_limit"), defaults.OBJECT_REPORT_LIMIT)

        controller.update_config("object_report_limit", 25)
        self.assertEqual(controller.get_config("object_report_limit"), 25)


class TestTaskIdsByObject(unittest.TestCase):
    def test_a_task_is_listed_against_its_function_and_its_arguments(self) -> None:
        client_id = ClientID.generate_client_id()
        function_id = ObjectID.generate_object_id(client_id)
        argument_id = ObjectID.generate_object_id(client_id)
        unused_id = ObjectID.generate_object_id(client_id)

        controller = VanillaTaskController(config_controller=make_config_controller())
        task = make_task(client_id, function_id, [argument_id])
        controller._task_id_to_task[task.taskId] = task

        by_object = controller.get_task_ids_by_object({function_id, argument_id, unused_id})

        self.assertEqual(by_object[function_id], [task.taskId])
        self.assertEqual(by_object[argument_id], [task.taskId])
        self.assertEqual(by_object[unused_id], [])

    def test_one_object_shared_by_two_tasks_lists_both(self) -> None:
        client_id = ClientID.generate_client_id()
        shared_id = ObjectID.generate_object_id(client_id)

        controller = VanillaTaskController(config_controller=make_config_controller())
        tasks = [make_task(client_id, ObjectID.generate_object_id(client_id), [shared_id]) for _ in range(2)]
        for task in tasks:
            controller._task_id_to_task[task.taskId] = task

        by_object = controller.get_task_ids_by_object({shared_id})

        self.assertEqual(sorted(by_object[shared_id]), sorted(task.taskId for task in tasks))


if __name__ == "__main__":
    unittest.main()
