from typing import Any


class ORBAWSEC2Exception(Exception):
    """Exception raised for errors in ORB AWS EC2 operations."""

    def __init__(self, data: Any):
        self.data = data
        super().__init__(f"ORB AWS EC2 Exception: {data}")
