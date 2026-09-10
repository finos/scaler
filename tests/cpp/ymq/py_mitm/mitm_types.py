"""
This is the common code for implementing man in the middle in Python
"""

import dataclasses
from abc import ABC, abstractmethod
from typing import Optional

from scapy.all import IP, TCP, Packet


@dataclasses.dataclass
class TCPConnection:
    """
    Represents a TCP connection over the TUNTAP interface
    local_ip and local_port are the mitm's ip and port, and
    remote_ip and remote_port are the port for the remote peer
    """

    local_ip: str
    local_port: int
    remote_ip: str
    remote_port: int

    def rewrite(self, pkt: IP, ack: Optional[int] = None, data=None):
        """
        Rewrite a TCP/IP packet as a packet originating
        from (local_ip, local_port) and going to (remote_ip, remote_port)
        This function is useful for taking a packet received from one connection, and redirecting it to another

        Args:
            pkt: A scapy TCP/IP packet to rewrite
            ack: An optional ack number to use instead of the one found in `pkt`
            data: An optional payload to use instead of the one found int `pkt`

        Returns:
            The rewritten packet, suitable for sending over TUNTAP
        """
        ip = pkt[IP]
        tcp = pkt[TCP]

        return (
            IP(src=self.local_ip or ip.src, dst=self.remote_ip)
            / TCP(
                sport=self.local_port or tcp.sport,
                dport=self.remote_port,
                flags=tcp.flags,
                seq=tcp.seq,
                ack=ack or tcp.ack,
            )
            / bytes(data or tcp.payload)
        )


class MITMInterface(ABC):
    @abstractmethod
    def recv(self) -> Packet: ...

    @abstractmethod
    def send(self, pkt: Packet) -> None: ...


class MITM(ABC):
    @abstractmethod
    def proxy(
        self,
        interface: MITMInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool: ...
