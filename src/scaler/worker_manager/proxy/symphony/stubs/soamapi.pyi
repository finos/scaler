"""Type stubs for the parts of the IBM Spectrum Symphony Python API that this package annotates.

``soamapi`` ships with the Symphony product as bytecode with no source and no stubs, one
``pythonapi_<python-version>`` directory per interpreter Symphony supports, so it cannot be introspected or
imported on a development or CI host, and nothing here can be checked against the real library by CI.
It therefore declares only the four types that appear in annotations, and only the members the code calls: those
are assumptions the code already makes, so a drift shows up as a bug in our own call sites. The rest of the API
is reached through ``load_soamapi()``, which returns an untyped module, and would gain nothing from being
declared here.

Transcribed from the Symphony 7.3.2 Python API reference, and checked by introspection against the
``soamapi`` of Symphony 7.3.2 build 603035: every type, method and signature below matches.
"""

import array
from typing import Any, Optional

class SoamException(Exception):
    def get_embedded_exception(self) -> Optional[BaseException]: ...

class OutputStream:
    def write_byte_array(self, byte_array: array.array, offset: int, length: int) -> None: ...

class InputStream:
    def read_byte_array(self, type_code: str = ...) -> array.array: ...

class Message:
    def on_serialize(self, stream: OutputStream) -> None: ...
    def on_deserialize(self, stream: InputStream) -> None: ...

class TaskOutputHandle:
    def get_id(self) -> str: ...
    def is_successful(self) -> bool: ...
    def populate_task_output(self, task_output_message: Message) -> None: ...
    def get_exception(self) -> SoamException: ...

class SessionCallback:
    def on_response(self, task_output_handle: TaskOutputHandle) -> None: ...
    def on_exception(self, exception: SoamException) -> None: ...
