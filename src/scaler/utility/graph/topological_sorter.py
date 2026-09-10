import logging
import sys

logger = logging.getLogger(__name__)

# python-graphblas's native Matrix.__del__ has been observed to corrupt the heap on Windows
# (STATUS_HEAP_CORRUPTION 0xC0000374) when finalized during a GC cycle that fires on a
# non-main thread (e.g. asyncio.gather scheduling inside ScalerClientAgent). Until that is
# fixed upstream, fall back to the stdlib graphlib backend on Windows. See issue #786.
if sys.platform == "win32":
    from graphlib import TopologicalSorter  # type: ignore[assignment, unused-ignore]
else:
    try:
        from scaler.utility.graph.topological_sorter_graphblas import TopologicalSorter

        logger.info("using GraphBLAS for calculate graph")
    except ImportError as e:
        assert isinstance(e, Exception)
        from graphlib import TopologicalSorter  # type: ignore[assignment, no-redef, unused-ignore]

        assert isinstance(TopologicalSorter, object)
