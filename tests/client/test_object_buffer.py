"""Unit tests for ``ObjectBuffer`` dedup (identity- and content-based).

These tests use stub connectors so they exercise the buffering / dedup logic
without spinning up a scheduler or object-storage server.
"""

import asyncio
import gc
import unittest
from concurrent.futures import Future
from typing import Any, Coroutine, TypeVar

import numpy as np

from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.default import DefaultSerializer
from scaler.protocol.capnp import BaseMessage
from scaler.utility.identifiers import ClientID, ObjectID

T = TypeVar("T")


class _FakeAgentConnector:
    """Minimal SyncConnector stub that records sent BaseMessages."""

    def __init__(self) -> None:
        self.sent: list[BaseMessage] = []

    def send(self, message: BaseMessage) -> None:
        self.sent.append(message)


class _FakeStorageConnector:
    """Minimal AsyncObjectStorageConnector stub that records set_object calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[ObjectID, int]] = []  # (object_id, payload_size)

    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        self.calls.append((object_id, len(payload)))


class _FakeAgentBridge:
    """Minimal ClientAgentBridge stub that runs the object storage accesses synchronously."""

    def __init__(self, connector_storage: _FakeStorageConnector) -> None:
        self._connector_storage = connector_storage

    def run_in_agent(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        result: Future[T] = Future()
        result.set_result(asyncio.run(coroutine))
        return result

    async def object_storage_connector(self) -> _FakeStorageConnector:
        return self._connector_storage


def _make_buffer() -> tuple[ObjectBuffer, _FakeAgentConnector, _FakeStorageConnector]:
    agent = _FakeAgentConnector()
    storage = _FakeStorageConnector()
    buf = ObjectBuffer(
        identity=ClientID.generate_client_id("test"),
        serializer=DefaultSerializer(),
        connector_agent=agent,  # type: ignore[arg-type]
        bridge=_FakeAgentBridge(storage),  # type: ignore[arg-type]
    )
    # The constructor uploads the serializer object eagerly; clear those so the
    # tests below only see the calls they themselves trigger.
    agent.sent.clear()
    storage.calls.clear()
    return buf, agent, storage


class TestObjectBufferDedup(unittest.TestCase):
    def test_same_object_uploaded_only_once_within_batch(self) -> None:
        """Buffer the same Python object N times in one cycle, commit once -> 1 upload."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(10_000, dtype=np.float64)  # weakreffable, non-trivial payload

        caches = [buf.buffer_send_object(shared, None, reserialize=False, dedup=True) for _ in range(5)]
        buf.commit_send_objects()

        # All returned caches should be the SAME entry.
        first_id = caches[0].object_id
        for c in caches[1:]:
            self.assertEqual(c.object_id, first_id)
            self.assertIs(c, caches[0])

        # Storage should have seen exactly one set_object call.
        self.assertEqual(len(storage.calls), 1)
        self.assertEqual(storage.calls[0][0], first_id)

    def test_distinct_content_gets_distinct_uploads(self) -> None:
        buf, _agent, storage = _make_buffer()

        a = np.zeros(100, dtype=np.float64)
        b = np.ones(100, dtype=np.float64)  # different content

        ca = buf.buffer_send_object(a, None, reserialize=False, dedup=True)
        cb = buf.buffer_send_object(b, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        self.assertNotEqual(ca.object_id, cb.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_equal_content_distinct_objects_share_one_upload(self) -> None:
        """Identical serialized content uploads only once: two *distinct* Python objects
        with identical bytes share one object ID and one upload -- a win the identity
        cache (keyed on id(obj)) cannot capture on its own."""
        buf, _agent, storage = _make_buffer()

        a = np.zeros(100, dtype=np.float64)
        b = np.zeros(100, dtype=np.float64)  # equal contents, different identity

        ca = buf.buffer_send_object(a, None, reserialize=False, dedup=True)
        cb = buf.buffer_send_object(b, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        self.assertEqual(ca.object_id, cb.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_equal_content_reuses_one_object_id(self) -> None:
        """Equal payloads reuse one object ID; changed payload -> different ID. (Hold
        references like real callers do, so temporaries can't be GC'd and id-recycled.)"""
        buf, _agent, _storage = _make_buffer()

        a1 = np.zeros(64, dtype=np.uint8)
        a2 = np.zeros(64, dtype=np.uint8)
        b = np.ones(64, dtype=np.uint8)

        c_a1 = buf.buffer_send_object(a1, None, reserialize=False, dedup=True)
        c_a2 = buf.buffer_send_object(a2, None, reserialize=False, dedup=True)
        c_b = buf.buffer_send_object(b, None, reserialize=False, dedup=True)

        self.assertEqual(c_a1.object_id, c_a2.object_id)
        self.assertNotEqual(c_a1.object_id, c_b.object_id)

    def test_function_dedup(self) -> None:
        buf, _agent, storage = _make_buffer()

        def fn(x):  # noqa: D401
            return x

        c1 = buf.buffer_send_function(fn)
        c2 = buf.buffer_send_function(fn)
        buf.commit_send_objects()

        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_non_weakreffable_arg_deduped_within_batch(self) -> None:
        """Non-weakreffable args (list / dict / tuple) dedup within a batch via the
        per-cycle cache. The per-cycle serialize-cache resets on commit, but the
        upload stays deduped across the commit because the content dedup map still
        points the identical bytes at the already-uploaded ID the server holds."""
        buf, _agent, storage = _make_buffer()

        shared_list = list(range(1000))

        c1 = buf.buffer_send_object(shared_list, None, reserialize=False, dedup=True)
        c2 = buf.buffer_send_object(shared_list, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # One upload within the batch.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

        # After the commit the per-cycle cache is dropped (so the list is
        # re-serialized), but the content dedup map still maps these bytes to the
        # already-uploaded ID known to the server, so no second upload happens.
        c3 = buf.buffer_send_object(shared_list, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(c1.object_id, c3.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_dedup_survives_commit(self) -> None:
        """A weakref-able object reused across separate commit cycles is uploaded
        only once: the persistent identity cache is not dropped on commit."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        c2 = buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # Same cached upload reused across the intervening commit.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_mutation_without_reserialize_serves_cached_snapshot(self) -> None:
        """By default the persistent cache is reused across commits, so mutating
        an object in place and re-buffering it returns the cached pre-mutation
        snapshot. Callers who mutate must pass reserialize=True (see below)."""
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box([1, 2, 3])
        c1 = buf.buffer_send_object(obj, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        obj.v.append(4)  # in-place mutation after the upload
        c2 = buf.buffer_send_object(obj, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # Cache hit: same object_id, no second upload, stale snapshot served.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_reserialize_reuploads_mutated_object_and_refreshes_cache(self) -> None:
        """reserialize=True re-serializes a mutated object, uploads the new
        contents, and refreshes the persistent cache so a later default call
        reuses the new snapshot (not the original)."""
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box([1, 2, 3])
        c1 = buf.buffer_send_object(obj, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        obj.v.append(4)  # in-place mutation after the upload
        c2 = buf.buffer_send_object(obj, None, reserialize=True, dedup=True)
        buf.commit_send_objects()

        # Re-uploaded with the mutated contents.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertNotEqual(c1.object_payload, c2.object_payload)
        self.assertEqual(len(storage.calls), 2)

        # The cache now holds the refreshed snapshot: a later default call hits
        # c2, not c1, and does not re-upload.
        c3 = buf.buffer_send_object(obj, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(c3.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_reserialize_dedups_repeats_within_one_cycle(self) -> None:
        """A shared, mutated object passed many times in one reserialize call is
        re-serialized and re-uploaded once, then deduped within that commit cycle
        -- not re-uploaded per task."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(10_000, dtype=np.float64)

        # Prime the persistent cache in an earlier cycle.
        buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        shared[0] = 1.0  # mutate so the refresh has genuinely new content

        # Now reserialize the same object across five tasks in one cycle.
        caches = [buf.buffer_send_object(shared, None, reserialize=True, dedup=True) for _ in range(5)]
        buf.commit_send_objects()

        # Exactly one extra upload (the refresh), all five share it.
        first_id = caches[0].object_id
        for cache in caches[1:]:
            self.assertEqual(cache.object_id, first_id)
        self.assertNotEqual(first_id, storage.calls[0][0])  # different from the pre-mutation upload
        self.assertEqual(len(storage.calls), 2)

    def test_reserialize_unchanged_content_skips_reupload(self) -> None:
        """The content-dedup payoff: reserialize=True on an object that turns out not
        to have changed re-serializes it but does NOT re-upload, because the unchanged
        bytes still map to the object ID the server already holds."""
        buf, _agent, storage = _make_buffer()

        shared = np.zeros(10_000, dtype=np.float64)

        c1 = buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        # No mutation; force a re-serialize anyway.
        c2 = buf.buffer_send_object(shared, None, reserialize=True, dedup=True)
        buf.commit_send_objects()

        # Same content -> same dedup-mapped ID -> no second upload.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_send_object_path_bypasses_identity_cache(self) -> None:
        """The send_object() path (dedup=False) opts out of the identity cache: it
        never records the object, so it can't serialize-dedup against earlier calls
        and can't serve a stale snapshot to a later submit(). The upload itself is
        still content-deduped, so identical bytes upload only once."""
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        obj = _Box(b"data")
        c1 = buf.buffer_send_object(obj, None, reserialize=False, dedup=False)
        c2 = buf.buffer_send_object(obj, None, reserialize=False, dedup=False)
        buf.commit_send_objects()

        # Opted out of the identity cache: nothing remembered under id(obj).
        self.assertNotIn(id(obj), buf._dedup_cache)
        self.assertNotIn(id(obj), buf._cycle_dedup_cache)

        # But the upload is content-deduped, so the identical bytes upload once.
        self.assertEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 1)

    def test_clear_invalidates_dedup_cache(self) -> None:
        """After clear(), the same object must be re-uploaded because the server has
        discarded its prior copy. clear() drops valid_object_ids AND the content dedup
        map, so the re-upload mints a brand-new object ID (it cannot reuse, or collide
        with, the cleared one)."""
        buf, _agent, storage = _make_buffer()

        shared = np.ones(1024, dtype=np.uint8)

        c1 = buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        buf.clear()

        c2 = buf.buffer_send_object(shared, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # clear() reset the content dedup map, so the same bytes get a brand-new object ID
        # (never the cleared one) and a genuine re-upload.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_id_recycled_after_gc_does_not_serve_stale_cache(self) -> None:
        """An object GC'd after its commit, whose id is later reused by a
        different object, must miss: the persistent cache survives the commit,
        but the parallel weakref guard detects that id() now names a different
        object and drops the stale entry."""
        buf, _agent, storage = _make_buffer()

        class _Box:
            def __init__(self, v):
                self.v = v

        first = _Box(b"a" * 256)
        c1 = buf.buffer_send_object(first, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        first_id = id(first)

        del first
        gc.collect()

        # Try to place another object such that its id() collides with the freed
        # one.  Allocate until we get a match, with a small budget.
        second = None
        for _ in range(10_000):
            candidate = _Box(b"b" * 256)
            if id(candidate) == first_id:
                second = candidate
                break
        if second is None:
            self.skipTest("could not provoke id recycling on this interpreter")

        c2 = buf.buffer_send_object(second, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # Must NOT have returned the stale cache entry.
        self.assertNotEqual(c1.object_id, c2.object_id)
        self.assertEqual(len(storage.calls), 2)

    def test_weakref_guard_rejects_recycled_id(self) -> None:
        """Deterministic counterpart to the id-recycling test: when id(obj) collides
        with a stale persistent entry left by a different, now-collected object, the
        weakref guard rejects the stale cache so the new object is serialized under
        its own object ID instead of being served the other's snapshot."""
        buf, _agent, storage = _make_buffer()

        first = np.zeros(1024, dtype=np.uint8)
        c1 = buf.buffer_send_object(first, None, reserialize=False, dedup=True)
        buf.commit_send_objects()
        self.assertEqual(len(storage.calls), 1)

        # Simulate id recycling: a different object `second` (different content)
        # whose id() matches the still-cached entry for `first`, while the weakref
        # for that id no longer resolves to a live object.
        second = np.ones(1024, dtype=np.uint8)
        stale_key = id(second)
        buf._dedup_cache[stale_key] = c1  # stale entry -> first's cache
        buf._dedup_alive.pop(stale_key, None)  # weakref does not resolve to `second`

        c2 = buf.buffer_send_object(second, None, reserialize=False, dedup=True)
        buf.commit_send_objects()

        # Guard rejected the stale entry: `second` got its own object ID
        # and a real upload, NOT first's snapshot.
        self.assertNotEqual(c2.object_id, c1.object_id)
        self.assertEqual(len(storage.calls), 2)


if __name__ == "__main__":
    unittest.main()
