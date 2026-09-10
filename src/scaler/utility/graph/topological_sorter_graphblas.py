import collections
import graphlib
import itertools
from typing import Generic, Hashable, Iterable, List, Mapping, Optional, Tuple, TypeVar

from bidict import bidict

try:
    import graphblas as gb
    import numpy as np  # noqa
except ImportError:
    raise ImportError("Please use 'pip install python-graphblas' to have graph blas support")

GraphKeyType = TypeVar("GraphKeyType", bound=Hashable)


class TopologicalSorter(Generic[GraphKeyType]):
    """
    Implements graphlib's TopologicalSorter, but the graph handling is backed by GraphBLAS
    Reference: https://github.com/python/cpython/blob/4a3ea1fdd890e5e2ec26540dc3c958a52fba6556/Lib/graphlib.py
    """

    def __init__(self, graph: Optional[Mapping[GraphKeyType, Iterable[GraphKeyType]]] = None):
        # the layout of the matrix is (in-vertex, out-vertex)
        self._matrix = gb.Matrix(gb.dtypes.BOOL)
        self._key_to_id: bidict[GraphKeyType, int] = bidict()

        self._graph_matrix_mask: Optional[np.ndarray] = None
        self._visited_vertices_mask: Optional[np.ndarray] = None
        self._ready_nodes: Optional[List[GraphKeyType]] = None

        self._n_done = 0
        self._n_visited = 0

        if graph is not None:
            self.merge_graph(graph)

    def add(self, node: GraphKeyType, *predecessors: GraphKeyType) -> None:
        self.merge_graph({node: predecessors})

    def merge_graph(self, graph: Mapping[GraphKeyType, Iterable[GraphKeyType]]) -> None:
        if self._ready_nodes is not None:
            raise ValueError("nodes cannot be added after a call to prepare()")

        # cache old dim to compare later when resizing matrix
        old_dim = len(self._key_to_id)

        # maintain iterable copies for iterable predecessors
        graph_iterable_copy = {}

        # update key to id mappings
        for node, predecessors in graph.items():
            if node not in self._key_to_id:
                self._key_to_id[node] = len(self._key_to_id)

            # copy iterator if predecessors is an iterable
            if isinstance(predecessors, collections.abc.Iterable):
                predecessors, graph_iterable_copy[node] = itertools.tee(predecessors)

            for pred in predecessors:
                if pred not in self._key_to_id:
                    self._key_to_id[pred] = len(self._key_to_id)

        # resize at once as it is faster
        if old_dim != len(self._key_to_id):
            self._matrix.resize(len(self._key_to_id), len(self._key_to_id))

        # update matrix
        for node, predecessors in graph.items():
            if node in graph_iterable_copy:
                predecessors = graph_iterable_copy[node]

            for pred in predecessors:
                self._matrix[self._key_to_id[node], self._key_to_id[pred]] = True

    def prepare(self) -> None:
        if self._ready_nodes is not None:
            raise ValueError("cannot prepare() more than once")

        self._graph_matrix_mask = np.ones(len(self._key_to_id), bool)
        self._visited_vertices_mask = np.zeros(len(self._key_to_id), bool)

        self._ready_nodes = self._get_zero_degree_keys()
        for node in self._ready_nodes:
            self._visited_vertices_mask[self._key_to_id[node]] = True
        self._n_visited += len(self._ready_nodes)

        if self._has_cycle():
            raise graphlib.CycleError("cycle detected")

    def get_ready(self) -> Tuple[GraphKeyType, ...]:
        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")

        result = tuple(self._ready_nodes)
        self._ready_nodes.clear()
        return result

    def is_active(self) -> bool:
        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")
        return self._n_done < self._n_visited or bool(self._ready_nodes)

    def __bool__(self) -> bool:
        return self.is_active()

    def done(self, *nodes: GraphKeyType) -> None:
        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")

        for node in nodes:
            if node not in self._key_to_id:
                raise ValueError(f"node {node!r} was not added using add()")

            _id = self._key_to_id[node]

            if not self._visited_vertices_mask[_id]:
                raise ValueError(f"node {node!r} is not ready")

            if not self._graph_matrix_mask[_id]:
                raise ValueError(f"node {node!r} is already done")

            self._graph_matrix_mask[_id] = False
        self._n_done += len(nodes)

        new_ready_nodes = self._get_zero_degree_keys()
        for node in new_ready_nodes:
            self._visited_vertices_mask[self._key_to_id[node]] = True
        self._ready_nodes.extend(new_ready_nodes)
        self._n_visited += len(new_ready_nodes)

    def static_order(self) -> Iterable[GraphKeyType]:
        self.prepare()
        while self.is_active():
            node_group = self.get_ready()
            yield from node_group
            self.done(*node_group)

    def _has_cycle(self) -> bool:
        """
        Detect cycle using trace(A^n) != 0.
        https://arxiv.org/pdf/1610.01200.pdf

        :return: True if cycle is found, otherwise False
        """
        matrix_n = gb.Vector.from_dense(np.ones(len(self._key_to_id), bool), missing_value=False).diag()
        for _ in range(len(self._key_to_id)):
            # use LOR_PAIR to compute matrix multiplication over boolean matrices
            matrix_n << gb.semiring.lor_pair(matrix_n @ self._matrix)
            # check diagonal for any truthy values
            if matrix_n.diag().reduce(gb.monoid.lor):
                return True
        return False

    def _get_zero_degree_keys(self) -> List[GraphKeyType]:
        ids = self._get_mask_diff(self._visited_vertices_mask, self._get_zero_degree_mask(self._get_masked_matrix()))
        return [self._key_to_id.inverse[_id] for _id in ids]

    def _get_masked_matrix(self) -> gb.Matrix:
        # convert vector mask to matrix diagonal and then perform matrix multiplication to mask matrix
        # https://github.com/DrTimothyAldenDavis/GraphBLAS/issues/48#issuecomment-858596341
        return gb.semiring.lor_pair(
            self._matrix @ gb.Vector.from_dense(self._graph_matrix_mask, missing_value=False).diag()
        )

    @classmethod
    def _get_zero_degree_mask(cls, masked_matrix: gb.Matrix) -> np.ndarray:
        degrees = masked_matrix.reduce_rowwise(gb.monoid.lor)
        indices, _ = degrees.to_coo(indices=True, values=False, sort=False)
        return np.logical_not(np.in1d(np.arange(masked_matrix.nrows), indices))

    @staticmethod
    def _get_mask_diff(old_mask: np.ndarray, new_mask: np.ndarray) -> List[int]:
        return np.argwhere(old_mask != new_mask).ravel().tolist()
