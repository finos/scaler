import abc
from typing import Callable, Generator, Generic, Optional, TypeVar

from scaler.utility.many_to_many_dict import ManyToManyDict

BlockType = TypeVar("BlockType")
ObjectKeyType = TypeVar("ObjectKeyType")


class ObjectUsage(Generic[ObjectKeyType], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_object_key(self) -> ObjectKeyType:
        raise NotImplementedError()


ObjectType = TypeVar("ObjectType", bound=ObjectUsage)


class ObjectTracker(Generic[BlockType, ObjectKeyType, ObjectType]):
    def __init__(self, prefix: str, callback: Callable[[ObjectType], None]):
        self._prefix = prefix
        self._callback = callback

        self._current_blocks: set[BlockType] = set()
        self._object_key_to_block: ManyToManyDict[ObjectKeyType, BlockType] = ManyToManyDict()
        self._object_key_to_object: dict[ObjectKeyType, ObjectType] = dict()

    def object_count(self):
        return len(self._object_key_to_object)

    def items(self):
        return self._object_key_to_object.items()

    def get_all_object_keys(self) -> set[ObjectKeyType]:
        return set(self._object_key_to_object.keys())

    def has_object(self, key: ObjectKeyType) -> bool:
        return key in self._object_key_to_object

    def get_object(self, key: ObjectKeyType) -> ObjectType:
        return self._object_key_to_object[key]

    def add_object(self, obj: ObjectType):
        self._object_key_to_object[obj.get_object_key()] = obj

    def get_object_block_pairs(self, blocks: set[BlockType]) -> Generator[tuple[ObjectKeyType, BlockType], None, None]:
        for block in blocks:
            if not self._object_key_to_block.has_right_key(block):
                continue

            for object_key in self._object_key_to_block.get_left_items(block):
                yield object_key, block

    def add_blocks_for_one_object(self, object_key: ObjectKeyType, blocks: set[BlockType]):
        if object_key not in self._object_key_to_object:
            raise KeyError(f"cannot find key={object_key} in ObjectTracker")

        for block in blocks:
            self._object_key_to_block.add(object_key, block)

        self._current_blocks.update(blocks)

    def remove_blocks_for_one_object(self, object_key: ObjectKeyType, blocks: set[BlockType]):
        ready_objects = []
        for block in blocks:
            obj = self.__remove_block_for_object(object_key, block)
            if obj is None:
                continue

            ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def add_one_block_for_objects(self, object_keys: set[ObjectKeyType], block: BlockType):
        for object_key in object_keys:
            if object_key not in self._object_key_to_object:
                raise KeyError(f"cannot find key={object_key} in ObjectTracker")

            self._object_key_to_block.add(object_key, block)

        self._current_blocks.add(block)

    def remove_one_block_for_objects(self, object_keys: set[ObjectKeyType], block: BlockType):
        ready_objects = []
        for object_key in object_keys:
            obj = self.__remove_block_for_object(object_key, block)
            if obj is None:
                continue

            ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def remove_blocks(self, blocks: set[BlockType]):
        ready_objects = []
        for block in blocks:
            if not self._object_key_to_block.has_right_key(block):
                continue

            object_keys = self._object_key_to_block.get_left_items(block).copy()
            for object_key in object_keys:
                obj = self.__remove_block_for_object(object_key, block)
                if obj is None:
                    continue

                ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def __remove_block_for_object(self, object_key: ObjectKeyType, block: BlockType) -> Optional[ObjectType]:
        if block not in self._current_blocks:
            return None

        if object_key not in self._object_key_to_object:
            return None

        if not self._object_key_to_block.has_key_pair(object_key, block):
            return None

        self._object_key_to_block.remove(object_key, block)

        if not self._object_key_to_block.has_right_key(block):
            self._current_blocks.remove(block)

        if self._object_key_to_block.has_left_key(object_key):
            return None

        return self._object_key_to_object.pop(object_key)
