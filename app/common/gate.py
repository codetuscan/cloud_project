"""Bounded-concurrency gate (backpressure) that also records in-flight / waiting / peak usage."""
import asyncio


class Busy(Exception):
    """Could not get a slot within the admission timeout."""


class Slot:
    def __init__(self, gate: "Gate"):
        self._gate, self._done = gate, False

    def release(self) -> None:  # idempotent
        if not self._done:
            self._done = True
            self._gate._release()


class Gate:
    def __init__(self, limit: int, name: str = ""):
        self.limit, self.name = max(1, limit), name
        self._sem = asyncio.Semaphore(self.limit)
        self.inflight = self.peak = self.waiting = 0
        self.admitted = self.rejected = 0

    async def enter(self, timeout: float | None = None) -> Slot:
        self.waiting += 1
        try:
            if timeout is None:
                await self._sem.acquire()
            else:
                await asyncio.wait_for(self._sem.acquire(), timeout)
        except asyncio.TimeoutError:
            self.rejected += 1
            raise Busy(self.name)
        finally:
            self.waiting -= 1
        self.inflight += 1
        self.admitted += 1
        self.peak = max(self.peak, self.inflight)
        return Slot(self)

    def _release(self) -> None:
        self.inflight -= 1
        self._sem.release()

    # `async with gate:` for the simple "wait as long as needed" case
    async def __aenter__(self):
        self._ctx = getattr(self, "_ctx", [])
        self._ctx.append(await self.enter())
        return self

    async def __aexit__(self, *exc):
        self._ctx.pop().release()

    def stats(self) -> dict:
        return {"limit": self.limit, "inflight": self.inflight, "waiting": self.waiting,
                "peak_inflight": self.peak, "admitted": self.admitted, "rejected": self.rejected}
