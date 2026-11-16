"""Controller start-up probe: refresh workers that answer, leave the unreachable ones to the monitor."""
import httpx

from app.controller import health


class FakePool:
    def __init__(self, rows):
        self.rows, self.updated = rows, None

    async def fetch(self, sql, *args):
        assert "state <> 'DEAD'" in sql           # workers already DEAD are revived by their own heartbeat
        return self.rows

    async def execute(self, sql, ids):
        self.updated = list(ids)


class FakeHttp:
    def __init__(self, behaviour):
        self.behaviour, self.urls = behaviour, []

    async def get(self, url, timeout=None):
        self.urls.append(url)
        out = self.behaviour[url]
        if isinstance(out, Exception):
            raise out
        return httpx.Response(out, request=httpx.Request("GET", url))


def rows(*ids):
    return [{"worker_id": i, "address": f"http://{i}:9000"} for i in ids]


async def test_answering_workers_are_refreshed_and_dead_ones_are_not():
    pool = FakePool(rows("worker1", "worker2", "worker3"))
    http = FakeHttp({"http://worker1:9000/healthz": 200,
                     "http://worker2:9000/healthz": httpx.ConnectError("down"),
                     "http://worker3:9000/healthz": 200})
    assert await health.probe_workers(pool, http) == (2, 1)
    assert sorted(pool.updated) == ["worker1", "worker3"]


async def test_a_non_200_answer_does_not_count_as_alive():
    pool = FakePool(rows("worker1"))
    assert await health.probe_workers(pool, FakeHttp({"http://worker1:9000/healthz": 503})) == (0, 1)
    assert pool.updated is None


async def test_no_workers_known_is_fine():
    pool = FakePool([])
    assert await health.probe_workers(pool, FakeHttp({})) == (0, 0)
    assert pool.updated is None
