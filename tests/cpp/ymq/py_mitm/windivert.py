import socket
from typing import Any

import pydivert
from scapy.all import IP, Packet

from tests.cpp.ymq.py_mitm.mitm_types import MITMInterface


class WindivertMITMInterface(MITMInterface):
    _windivert: pydivert.WinDivert
    _binder: socket.socket

    __interface: Any
    __direction: pydivert.Direction

    def __init__(self, local_ip: str, local_port: int, remote_ip: str, server_port: int):
        self._windivert = pydivert.WinDivert(f"tcp.DstPort == {local_port} or tcp.SrcPort == {server_port}")
        self._windivert.open()

        self._binder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._binder.bind((local_ip, local_port))
        self._binder.listen(16)

    def recv(self) -> Packet:
        windivert_packet = self._windivert.recv()

        # save these for later when we need to re-inject
        self.__interface = windivert_packet.interface
        self.__direction = windivert_packet.direction

        scapy_packet = IP(bytes(windivert_packet.raw))
        return scapy_packet

    def send(self, pkt: Packet) -> None:
        self._windivert.send(pydivert.Packet(bytes(pkt), self.__interface, self.__direction))
