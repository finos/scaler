import subprocess
from typing import List

from scapy.all import TunTapInterface


def echo_call(cmd: List[str]):
    print(f"+ {' '.join(cmd)}")
    subprocess.check_call(cmd)


def create_tuntap_interface(iface_name: str, mitm_ip: str, remote_ip: str) -> TunTapInterface:
    """
    Creates a TUNTAP interface and sets brings it up and adds ips using the `ip` program

    Args:
        iface_name: The name of the TUNTAP interface, usually like `tun0`, `tun1`, etc.
        mitm_ip: The desired ip address of the mitm. This is the ip that clients can use to connect to the mitm
        remote_ip: The ip that routes to/from the tuntap interface.
        packets sent to `mitm_ip` will appear to come from `remote_ip`,\
        and conversely the tuntap interface can connect/send packets
        to `remote_ip`, making it a suitable ip for binding a server

    Returns:
        The TUNTAP interface
    """
    iface = TunTapInterface(iface_name, mode="tun")

    try:
        echo_call(["sudo", "ip", "link", "set", iface_name, "up"])
        echo_call(["sudo", "ip", "addr", "add", remote_ip, "peer", mitm_ip, "dev", iface_name])
        print(f"[+] Interface {iface_name} up with IP {mitm_ip}")
    except subprocess.CalledProcessError:
        print("[!] Could not bring up interface. Run as root or set manually.")
        raise

    return iface
