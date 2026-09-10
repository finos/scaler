import enum


class ContainerInstanceLifecycleState(str, enum.Enum):
    CREATING = "CREATING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    FAILED = "FAILED"
