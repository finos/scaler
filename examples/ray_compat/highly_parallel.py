"""
This example was copied from https://docs.ray.io/en/latest/ray-core/examples/highly_parallel.html

Only one or two changes are needed to make this example work on Scaler.
First is to import the compatibility layer, this patches Ray Core's API.
The second is to call `ray.shutdown()`, necessary only if using a local cluster.
"""

import random
import time
from fractions import Fraction

import ray

# this is one of only two changed lines
import scaler.compat.ray  # noqa: F401

# Let's start Ray
ray.init(address="auto")


def main():
    @ray.remote
    def pi4_sample(sample_count):
        """pi4_sample runs sample_count experiments, and returns the
        fraction of time it was inside the circle.
        """
        in_count = 0
        for i in range(sample_count):
            x = random.random()
            y = random.random()
            if x * x + y * y <= 1:
                in_count += 1
        return Fraction(in_count, sample_count)

    SAMPLE_COUNT = 1000 * 1000
    start = time.time()
    future = pi4_sample.remote(sample_count=SAMPLE_COUNT)
    pi4 = ray.get(future)  # noqa: F841
    end = time.time()
    dur = end - start
    print(f"Running {SAMPLE_COUNT} tests took {dur} seconds")


if __name__ == "__main__":
    main()

    # this is the second changed line
    # we need to explicitly shut down the implicit cluster
    ray.shutdown()
