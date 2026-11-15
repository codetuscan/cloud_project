import asyncio

import pytest

from app.common.gate import Busy, Gate


async def test_limit_is_never_exceeded():
    g = Gate(3, "t")
    live = peak = 0

    async def work():
        nonlocal live, peak
        async with g:
            live += 1
            peak = max(peak, live)
            await asyncio.sleep(0.02)
            live -= 1

    await asyncio.gather(*[work() for _ in range(20)])
    assert peak == 3 and g.stats()["peak_inflight"] == 3 and g.inflight == 0


async def test_admission_timeout_raises_busy_and_counts():
    g = Gate(1, "uploads")
    slot = await g.enter()
    with pytest.raises(Busy):
        await g.enter(timeout=0.05)
    assert g.stats()["rejected"] == 1 and g.waiting == 0
    slot.release()
    (await g.enter(timeout=0.05)).release()               # free again


async def test_slot_release_is_idempotent():
    g = Gate(1)
    s = await g.enter()
    s.release(); s.release()
    assert g.inflight == 0
    await g.enter(timeout=0.05)
    with pytest.raises(Busy):
        await g.enter(timeout=0.05)                        # a double release must not create a second permit
