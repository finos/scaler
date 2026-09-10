"""One test class per topology. A topology is an ordered list of worker managers: one runs the vanilla
scaling policy, several run ``waterfall_v1`` at priority = position, possibly across backends.

Exactly one topology runs per invocation, named by ``SCALER_E2E`` (see README.md); the rest skip. Any
scenario can run on any topology, so what each class lists below is a curated matrix, not a limit -- only
``waterfall_spills`` has a hard requirement (>= 2 managers).
"""

from __future__ import annotations

import unittest

from tests.integration import E2E_TOPOLOGY
from tests.integration.backends import ContainerBackend, FlociEc2Backend, FlociEcsBackend
from tests.integration.framework import ManagerSpec, deploy
from tests.integration.scenarios import burst_and_drain, rising_load, steady_load_stable, waterfall_spills


def _topology(name: str):
    """Enable only the topology SCALER_E2E names. Each needs a Docker daemon and takes minutes, and the
    heavier ones need a prebuilt wheel, so opting in is explicit and one at a time. Deliberately no
    daemon/wheel probe: if you asked for a topology whose prerequisites are missing, it fails loudly
    rather than passing as a silent skip."""
    return unittest.skipUnless(E2E_TOPOLOGY == name, f"set SCALER_E2E={name} to run this e2e")


@_topology("container")
class TestContainerScaling(unittest.TestCase):
    """The scaling floor: no cloud and no emulator, just container machines."""

    def setUp(self) -> None:
        self.deployment = deploy(self, "container", [ManagerSpec(ContainerBackend())])

    def tearDown(self) -> None:
        self.deployment.assert_healthy(self)

    def test_burst_and_drain(self) -> None:
        burst_and_drain(self, self.deployment)

    def test_rising_load(self) -> None:
        rising_load(self, self.deployment)

    def test_steady_load_stable(self) -> None:
        steady_load_stable(self, self.deployment)


@_topology("container_waterfall")
class TestContainerWaterfall(unittest.TestCase):
    """Two container managers at different waterfall priorities; the top one capped so work must spill."""

    def setUp(self) -> None:
        self.deployment = deploy(
            self,
            "container_waterfall",
            [ManagerSpec(ContainerBackend("scaler-it-a"), cap=2), ManagerSpec(ContainerBackend("scaler-it-b"))],
        )

    def tearDown(self) -> None:
        self.deployment.assert_healthy(self)

    def test_waterfall_spills(self) -> None:
        waterfall_spills(self, self.deployment)


@_topology("ecs")
class TestECSScaling(unittest.TestCase):
    """The shipped ECS worker manager, launching real ECS task containers through floci."""

    def setUp(self) -> None:
        self.deployment = deploy(self, "ecs", [ManagerSpec(FlociEcsBackend())])

    def tearDown(self) -> None:
        self.deployment.assert_healthy(self)

    def test_burst_and_drain(self) -> None:
        burst_and_drain(self, self.deployment)

    def test_rising_load(self) -> None:
        rising_load(self, self.deployment)

    def test_steady_load_stable(self) -> None:
        steady_load_stable(self, self.deployment)


@_topology("ec2")
class TestEC2Scaling(unittest.TestCase):
    """The shipped ORB/EC2 worker manager, launching real Amazon Linux 2023 instances through floci."""

    def setUp(self) -> None:
        self.deployment = deploy(self, "ec2", [ManagerSpec(FlociEc2Backend())])

    def tearDown(self) -> None:
        self.deployment.assert_healthy(self)

    def test_burst_and_drain(self) -> None:
        burst_and_drain(self, self.deployment)

    def test_rising_load(self) -> None:
        rising_load(self, self.deployment)

    def test_steady_load_stable(self) -> None:
        steady_load_stable(self, self.deployment)


@_topology("ecs_ec2")
class TestCrossBackendWaterfall(unittest.TestCase):
    """Both shipped AWS managers on ONE scheduler: work fills the fast ECS pool, then spills onto EC2."""

    def setUp(self) -> None:
        self.deployment = deploy(
            self, "ecs_ec2", [ManagerSpec(FlociEcsBackend(), cap=2), ManagerSpec(FlociEc2Backend())]
        )

    def tearDown(self) -> None:
        self.deployment.assert_healthy(self)

    def test_waterfall_spills(self) -> None:
        waterfall_spills(self, self.deployment)


if __name__ == "__main__":
    unittest.main()
