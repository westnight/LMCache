# SPDX-License-Identifier: Apache-2.0
"""Tests for the facts-based IPC event lifecycle (ACK, not count windows).

Covers the ported upstream behaviour:

* ``RELEASE_EVENT`` exists in the transport-neutral RPC contract;
* the server holds an exported completion event until the worker releases it,
  then returns it to the owning context's pool;
* unknown handles (e.g. from an unregistered context) are ignored;
* unregistering a context forgets the events of its pool.
"""

# Standard
from collections import deque
from types import SimpleNamespace

# Third Party
import pytest

# First Party
from lmcache.v1.multiprocess.ipc_event_registry import IPCEventRegistry
from lmcache.v1.multiprocess.modules.lmcache_driven_transfer import ContextEntry
from lmcache.v1.multiprocess.rpc import get_rpc_spec, get_rpc_specs


class _FakeBackend:
    """Minimal EventIPCBackend stand-in; handles are stable per event."""

    device_type = "fake"

    def __init__(self) -> None:
        self.created = 0
        self.recorded: list[object] = []

    def create_event(self, device):
        self.created += 1
        return object()

    def record_event(self, event, stream):
        self.recorded.append(event)

    def export_event(self, event, device) -> bytes:
        return f"handle-{id(event):x}".encode()

    def import_event(self, handle, device):
        return ("imported", handle)

    def wait_event(self, event, stream):
        return None


def _entry(backend, registry, pool):
    return ContextEntry(
        cache_context=SimpleNamespace(device="fake"),
        model_name="m",
        world_size=1,
        event_backend=backend,
        ipc_events=registry,
        completion_event_pool=pool,
    )


def test_release_event_is_part_of_the_rpc_contract() -> None:
    specs = get_rpc_specs()
    assert "release_event" in specs
    spec = get_rpc_spec("release_event")
    assert spec.payload_types == (bytes,)
    assert spec.response_type is type(None)


def test_exported_event_is_held_until_released_then_pooled() -> None:
    backend = _FakeBackend()
    registry = IPCEventRegistry()
    pool: deque[object] = deque()
    entry = _entry(backend, registry, pool)

    event = entry.next_completion_event()
    assert backend.created == 1

    handle = entry.export_completion_event(event)
    assert registry.exported_count == 1
    assert len(pool) == 0  # still owned by the registry, not idle

    registry.release_exported(handle)
    assert registry.exported_count == 0
    assert list(pool) == [event]  # returned to the pool for reuse

    # The next transfer reuses the pooled event instead of creating another.
    assert entry.next_completion_event() is event
    assert backend.created == 1


def test_unknown_handle_release_is_ignored() -> None:
    registry = IPCEventRegistry()
    pool: deque[object] = deque()
    registry.release_exported(b"never-exported")
    assert registry.exported_count == 0
    assert len(pool) == 0


def test_forget_pool_drops_unreleased_events_of_that_context() -> None:
    backend = _FakeBackend()
    registry = IPCEventRegistry()
    pool_a: deque[object] = deque()
    pool_b: deque[object] = deque()
    entry_a = _entry(backend, registry, pool_a)
    entry_b = _entry(backend, registry, pool_b)

    handle_a = entry_a.export_completion_event(entry_a.next_completion_event())
    handle_b = entry_b.export_completion_event(entry_b.next_completion_event())
    assert registry.exported_count == 2

    registry.forget_pool(pool_a)
    assert registry.exported_count == 1

    registry.release_exported(handle_a)  # already forgotten: must not resurrect
    assert registry.exported_count == 1
    registry.release_exported(handle_b)
    assert registry.exported_count == 0
    assert len(pool_b) == 1


def test_context_entry_requires_backend_and_registry() -> None:
    entry = ContextEntry(
        cache_context=SimpleNamespace(device="fake"),
        model_name="m",
        world_size=1,
    )
    with pytest.raises(RuntimeError):
        entry.next_completion_event()


def test_producer_event_records_on_the_explicit_stream() -> None:
    """The producer event must be recorded on the stream that produced the KV.

    The connector passes it explicitly so a hook that moved to another
    thread/stream cannot silently record the ordering event elsewhere.
    """
    # First Party
    from lmcache.v1.multiprocess.transfer_context.worker_transfer import (
        LMCacheDrivenTransferContext,
    )

    class _RecordingBackend(_FakeBackend):
        def __init__(self) -> None:
            super().__init__()
            self.recorded_streams: list[object] = []

        def record_event(self, event, stream):
            self.recorded_streams.append(stream)

    backend = _RecordingBackend()
    ctx = LMCacheDrivenTransferContext.__new__(LMCacheDrivenTransferContext)
    ctx._device = "fake"
    ctx._event_backend = backend

    ctx.create_recorded_event(stream="explicit-stream")
    assert backend.recorded_streams == ["explicit-stream"]
