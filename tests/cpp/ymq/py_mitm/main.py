# flake8: noqa: E402

"""
This script provides a framework for running MITM test cases
"""

from scapy.config import conf

# only load the scapy layers that we need
conf.load_layers = ["inet"]

import argparse
import os
import platform
import signal

from scapy.all import IP, TCP

from tests.cpp.ymq.py_mitm import passthrough, randomly_drop_packets, send_rst_to_client
from tests.cpp.ymq.py_mitm.mitm_types import MITM, MITMInterface, TCPConnection


def main(pid: int, mitm_ip: str, mitm_port: int, remote_ip: str, server_port: int, mitm: MITM) -> None:
    """
    This function serves as a framework for man in the middle implementations
    A client connects to the MITM, then the MITM connects to a remote server
    The MITM sits inbetween the client and the server, manipulating the packets sent depending on the test case
    This function:
        1. creates an interface and prepares it for MITM
        2. handles connecting clients and handling connection closes
        3. delegates additional logic to a pluggable callable, `mitm`
        4. returns when both connections have terminated

    Args:
        pid: this is the pid of the test process, used for signaling readiness \
        we send SIGUSR1 to this process when the mitm is ready
        mitm_ip: The desired ip address of the mitm server \
        Windows: This parameter is ignored
        mitm_port: The desired port of the mitm server. \
        Linux: This is the port used to connect to the server, but the client is free to connect on any port \
        Windows: This parameter is ignored
        remote_ip: The remote ip for the that the remote server is bound to
        server_port: The port that the remote server is bound to
        mitm: The core logic for a MITM test case. This callable may maintain its own state and is responsible \
        for sending packets over the interface (if it doesn't, nothing will happen)
    """
    interface = get_interface(mitm_ip, mitm_port, remote_ip, server_port)

    signal_ready(pid)

    # these track information about our connections
    # we already know what to expect for the server connection, we are the connector
    client_conn = None

    # the port that the mitm uses to connect to the server
    # we increment the port for each new connection to avoid collisions
    next_server_port = mitm_port
    server_conn = TCPConnection(mitm_ip, next_server_port, remote_ip, server_port)

    # tracks the state of each connection
    client_closed = False
    server_closed = False

    while True:
        pkt = interface.recv()

        if not pkt.haslayer(IP) or not pkt.haslayer(TCP):
            continue

        active_ports = {mitm_port, server_conn.local_port}
        if pkt.sport not in active_ports and pkt.dport not in active_ports:
            continue

        ip = pkt[IP]
        tcp = pkt[TCP]

        # for a received packet, the destination ip and port are our local ip and port
        # and the source ip and port will be the remote ip and port
        sender = TCPConnection(pkt.dst, pkt.dport, pkt.src, pkt.sport)

        if tcp.flags.S and not tcp.flags.A:  # SYN from client
            if sender != client_conn or client_conn is None:
                print(f"[*] Client {ip.src}:{tcp.sport} connecting to {ip.dst}:{tcp.dport}")
                client_conn = sender

                next_server_port += 1
                server_conn = TCPConnection(mitm_ip, next_server_port, remote_ip, server_port)

        if tcp.flags.S and tcp.flags.A:  # SYN-ACK from server
            if sender == server_conn:
                print(f"[*] Server {ip.src}:{tcp.sport} accepted connection from {ip.dst}:{tcp.dport}")

        if tcp.flags.R or tcp.flags.F:  # RST or FIN
            if sender == client_conn:
                client_closed = True
            if sender == server_conn:
                server_closed = True

        pretty = f"[{tcp.flags}]{(': ' + str(bytes(tcp.payload))) if tcp.payload else ''}"

        if not mitm.proxy(interface, pkt, sender, client_conn, server_conn):
            if sender == client_conn:
                print(f"[DROPPED]: -> {pretty}")
            elif sender == server_conn:
                print(f"[DROPPED]: <- {pretty}")
            else:
                print(f"[DROPPED]: ?? {pretty}")

            continue  # the segment was not proxied, so we can't update our internal state

        if sender == client_conn:
            print(f"-> {pretty}")
        elif sender == server_conn:
            print(f"<- {pretty}")
        else:
            print(f"?? {pretty}")

        if client_closed and server_closed:
            print("[*] Both connections closed")
            return


def get_interface(mitm_ip: str, mitm_port: int, remote_ip: str, server_port: int) -> MITMInterface:
    """get the platform-specific mitm interface"""

    system = platform.system()
    if system == "Windows":
        from tests.cpp.ymq.py_mitm.windivert import WindivertMITMInterface

        return WindivertMITMInterface(mitm_ip, mitm_port, remote_ip, server_port)
    elif system in ("Linux", "Darwin"):
        from tests.cpp.ymq.py_mitm.tuntap import create_tuntap_interface

        return create_tuntap_interface("tun0", mitm_ip, remote_ip)

    raise RuntimeError("unsupported platform")


def signal_ready(pid: int) -> None:
    """signal to the caller that the mitm is ready"""

    system = platform.system()
    if system == "Windows":
        import win32api
        import win32event

        handle = win32event.OpenEvent(win32event.EVENT_MODIFY_STATE, False, "Global\\PythonSignal")
        win32event.SetEvent(handle)
        win32api.CloseHandle(handle)
    elif system in ("Linux", "Darwin"):
        if pid > 0:
            os.kill(pid, signal.SIGUSR1)  # type: ignore[attr-defined, unused-ignore]


TESTCASES = {
    "passthrough": passthrough.PassthroughMITM,
    "randomly_drop_packets": randomly_drop_packets.RandomlyDropPacketsMITM,
    "send_rst_to_client": send_rst_to_client.SendRSTToClientMITM,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Man in the middle test framework")
    parser.add_argument("pid", type=int, help="The pid of the test process, used for signaling")
    parser.add_argument("testcase", type=str, choices=TESTCASES.keys(), help="The MITM test case module name")
    parser.add_argument("mitm_ip", type=str, help="The desired ip address of the mitm server")
    parser.add_argument("mitm_port", type=int, help="The desired port of the mitm server")
    parser.add_argument("remote_ip", type=str, help="The desired remote ip for the TUNTAP interface")
    parser.add_argument("server_port", type=int, help="The port that the remote server is bound to")

    args, extra = parser.parse_known_args()

    mitm = TESTCASES[args.testcase]

    main(args.pid, args.mitm_ip, args.mitm_port, args.remote_ip, args.server_port, mitm(*extra))
