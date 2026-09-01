import dataclasses
import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

from scaler.config.mixins import ConfigType


@dataclasses.dataclass
class WorkerNames(ConfigType):
    """Parses a comma-separated string of worker names into a list."""

    names: list[str] = dataclasses.field(default_factory=list)

    @classmethod
    def from_string(cls, value: str) -> Self:
        if not value:
            return cls([])
        names = [name.strip() for name in value.split(",")]
        return cls(names)

    def __str__(self) -> str:
        if self.names:
            return ",".join(self.names)
        return "<empty>"

    def __len__(self) -> int:
        return len(self.names)


@dataclasses.dataclass
class WorkerCapabilities(ConfigType):
    """Parses a string of worker capabilities."""

    capabilities: dict[str, int] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_string(cls, value: str) -> Self:
        capabilities: dict[str, int] = {}
        if not value:
            return cls(capabilities)
        for item in value.split(","):
            name, separator, capability_value = item.partition("=")
            name = name.strip()
            if name == "":
                raise ValueError(f"Invalid capability '{item}': capability name cannot be an empty string.")
            if separator == "":
                capabilities[name] = -1
            else:
                try:
                    capabilities[name] = int(capability_value)
                except ValueError:
                    raise ValueError(
                        f"Invalid capability value for '{name}'. Expected an integer, but got '{capability_value}'."
                    )
        return cls(capabilities)

    def __str__(self) -> str:
        if not self.capabilities:
            return "<empty>"

        items = []
        for name, cap in self.capabilities.items():
            if cap == -1:
                items.append(name)
            else:
                items.append(f"{name}={cap}")
        return ",".join(items)
