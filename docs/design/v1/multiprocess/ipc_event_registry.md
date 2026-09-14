# IPC event registry (MP server side)

`lmcache/v1/multiprocess/ipc_event_registry.py`

## Why

A cross-process CUDA event is only valid while the **exporter** keeps the
underlying object alive, and an imported event must outlive every stream wait
queued on it. A count window ("keep the last N exported events", FIFO or ring)
approximates that with "the peer has probably imported it by now". The window
is `N / event-rate`: it shrinks exactly when load grows, while the peer's
import delay grows. When the approximation is wrong the peer either fails to
open the handle (`invalid argument`) or a queued wait is left without its
record -- a hang that is much harder to attribute than an error.

So both directions are released on **facts**:

```
worker                         server
  STORE/RETRIEVE  ----------->  next_completion_event()   (pool)
                                record + export
                  <-----------  handle (registry.track_exported)
  ...device done...
  RELEASE_EVENT(handle) ----->  registry.release_exported(handle)
                                -> event returns to the context's pool
```

The reply to `STORE` / `RETRIEVE` is only sent after the server has called
`import_event` on the worker's producer event, so the worker's RPC future is
itself the import ACK. The worker pins the producer event to that future with
`MessagingFuture.retain_reference()`; the transport holds the future until the
reply.

## Semantics

* `track_exported(handle, event, pool)` -- hold `event` until
  `release_exported(handle)`; then append it to `pool`.
* `release_exported(handle)` -- unknown handles (an already unregistered
  context, or a duplicate release) are ignored, so a late `RELEASE_EVENT`
  cannot resurrect or corrupt a pool.
* `forget_pool(pool)` -- called when a context is unregistered; drops every
  unreleased export that belongs to that pool.
* `exported_count` -- number of handles still owned by the registry; a value
  that keeps growing means a peer is not releasing (visible in
  `report_status` of `LMCacheDrivenTransferModule` as
  `exported_events_unreleased`).

## Counters

`LMCacheDrivenTransferModule.report_status()` reports:

* `exported_events_unreleased` -- handles the registry still holds;
* per context `idle_completion_events` -- size of the idle pool.

## Imported events

Imported worker events are currently held by the cache context
(`BaseCacheContext.hold_imported_event` + a stream-ordered host callback,
`release_imported_event`), which is the same "release after the wait was
consumed" contract. `IPCEventRegistry` keeps the imported half of the upstream
API (`track_imported` / `release_imported` / `imported_count`) so the two
mechanisms can be unified without changing the class, but the transfer module
does not call it yet.

## Failure modes this removes

* completion event dropped at handler return before the worker imported it;
* completion event recycled while the worker's stream wait was still queued;
* producer event garbage-collected at the worker before the server imported it;
* and, indirectly, the "keep the last N" retention that masked all three.
