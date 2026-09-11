import array

try:
    import soamapi
except ImportError:
    raise ImportError("IBM Spectrum Symphony API not found, please install it with 'pip install soamapi'.")


class SoamMessage(soamapi.Message):
    def __init__(self, payload: bytes = b""):
        self.__payload = payload

    def set_payload(self, payload: bytes):
        self.__payload = payload

    def get_payload(self) -> bytes:
        return self.__payload

    def on_serialize(self, stream):
        payload_array = array.array("b", self.get_payload())
        stream.write_byte_array(payload_array, 0, len(payload_array))

    def on_deserialize(self, stream):
        self.set_payload(stream.read_byte_array("b"))
