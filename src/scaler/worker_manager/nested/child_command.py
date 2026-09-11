import os
from typing import Dict


def load_requirements_content(requirements_txt: str) -> str:
    """Return requirements file content, reading from disk if requirements_txt is a file path."""
    if os.path.isfile(requirements_txt):
        with open(requirements_txt) as f:
            return f.read()
    return requirements_txt


def format_capabilities(capabilities: Dict[str, int]) -> str:
    """
    Reverse of `parse_capabilities`: convert a capabilities dict into a
    comma-separated capability string (e.g. "linux,cpu=4").
    Values equal to -1 are emitted as flag-style entries (no `=value`).
    """
    parts = []
    for name, value in capabilities.items():
        if value == -1:
            parts.append(name)
        else:
            parts.append(f"{name}={value}")
    return ",".join(parts)
