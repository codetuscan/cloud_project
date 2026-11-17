"""Rebalancer: the move plan, and the safety rules of a single move (never fewer than 2 copies)."""
from types import SimpleNamespace

import pytest

from app.common.gate import Gate
from app.controller import rebalance


# ------------------------------------------------------------------ plan_moves (pure)
def test_new_empty_worker_receives_until_balanced():
    loads = {"worker1": 60, "worker2": 60, "worker3": 60, "worker4": 60, "worker5": 0}
    plan = rebalance.plan_moves(loads, tolerance=2, limit=1000)
    assert all(r == "worker5" for _, r in plan)
    after = dict(loads)
    for d, r in plan:
        after[d] -= 1
        after[r] += 1
    assert max(after.values()) - min(after.values()) <= 2
    assert sum(after.values()) == sum(loads.values())     # moves, never copies or deletions
    assert 46 <= after["worker5"] <= 48                    # 240 replicas / 5 workers = 48, within tolerance


def test_already_balanced_cluster_plans_nothing():
    assert rebalance.plan_moves({"a": 10, "b": 11, "c": 12}, tolerance=2, limit=50) == []


def test_tolerance_zero_does_not_oscillate():
    # a spread of 1 can never be improved by moving one replica; it must not ping-pong
    assert rebalance.plan_moves({"a": 5, "b": 4}, tolerance=0, limit=50) == []


def test_limit_caps_one_round():
    plan = rebalance.plan_moves({"a": 100, "b": 0}, tolerance=2, limit=16)
    assert plan == [("a", "b")] * 16


def test_takes_from_the_fullest_workers_first():
    plan = rebalance.plan_moves({"a": 20, "b": 14, "c": 10, "d": 0}, tolerance=2, limit=6)
    donors = [d for d, _ in plan]
    assert donors[:6] == ["a", "a", "a", "a", "a", "a"]
    assert {r for _, r in plan} == {"d"}


def test_full_worker_never_receives():
    # "d" is empty but has no free space: nothing can be moved to it
    assert rebalance.plan_moves({"a": 30, "b": 30, "d": 0}, tolerance=2, limit=10, receivers={"a", "b"}) == []


def test_single_worker_plans_nothing():
    assert rebalance.plan_moves({"a": 30}, tolerance=2, limit=10) == []


# ------------------------------------------------------------------ move(): rollback and promotion rules
class FakeConn:
    def __init__(self, pool):
        self.pool = pool

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def fetchval(self, sql, *args):
        return await self.pool.fetchval(sql, *args)


class FakePool:
    """Just enough of asyncpg for move(): replica rows as {(chunk, worker): state}."""

    def __init__(self, rows, object_committed=True):
        self.rows, self.committed = dict(rows), object_committed

    def acquire(self):
        return FakeConn(self)

    async def fetchval(self, sql, *args):
        if sql.lstrip().startswith("INSERT INTO replicas"):
            cid, wid = args
            if not self.committed or (cid, wid) in self.rows:
                return None
            self.rows[(cid, wid)] = "PENDING"
            return 1
        if "SET state='HEALTHY'" in sql:
            cid, wid = args
            if self.rows.get((cid, wid)) == "PENDING":
                self.rows[(cid, wid)] = "HEALTHY"
                return 1
            return None
        if "SELECT count(*)" in sql:
            return sum(1 for (c, _), s in self.rows.items() if c == args[0] and s == "HEALTHY")
        if "SET state='DELETING'" in sql:
            cid, wid = args
            if self.rows.get((cid, wid)) == "HEALTHY":
                self.rows[(cid, wid)] = "DELETING"
                return 1
            return None
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        assert sql.startswith("DELETE FROM replicas")
        if self.rows.get(args) == "PENDING":
            del self.rows[args]


CH = {"chunk_id": "c1", "size": 4, "checksum": "sha256:" + "a" * 64}
DONOR = {"worker_id": "w1", "address": "http://w1:9000"}
OTHER = "w2"
RECIP = {"worker_id": "w5", "address": "http://w5:9000"}


@pytest.fixture
def copy_result(monkeypatch):
    def install(outcome, side_effect=None):
        async def fake_copy(state, src, tgt, chunk, counter="x"):
            if side_effect:
                side_effect()
            return outcome
        monkeypatch.setattr(rebalance, "_command_copy", fake_copy)
    return install


def _state(pool):
    return SimpleNamespace(pool=pool, http=None, transfer_sem=Gate(2, "t"))


async def test_successful_move_promotes_recipient_and_retires_donor(copy_result):
    copy_result("ok")
    pool = FakePool({("c1", "w1"): "HEALTHY", ("c1", OTHER): "HEALTHY"})
    assert await rebalance.move(_state(pool), CH, DONOR, RECIP) is True
    assert pool.rows == {("c1", "w1"): "DELETING", ("c1", OTHER): "HEALTHY", ("c1", "w5"): "HEALTHY"}


async def test_failed_copy_is_rolled_back_and_donor_kept(copy_result):
    copy_result("target_failed")
    pool = FakePool({("c1", "w1"): "HEALTHY", ("c1", OTHER): "HEALTHY"})
    assert await rebalance.move(_state(pool), CH, DONOR, RECIP) is False
    assert pool.rows == {("c1", "w1"): "HEALTHY", ("c1", OTHER): "HEALTHY"}


async def test_donor_kept_if_the_other_copy_went_bad_during_the_move(copy_result):
    pool = FakePool({("c1", "w1"): "HEALTHY", ("c1", OTHER): "HEALTHY"})
    copy_result("ok", side_effect=lambda: pool.rows.__setitem__(("c1", OTHER), "CORRUPT"))
    assert await rebalance.move(_state(pool), CH, DONOR, RECIP) is True
    # only 2 healthy copies exist after the move: the donor's copy must NOT be retired
    assert pool.rows[("c1", "w1")] == "HEALTHY" and pool.rows[("c1", "w5")] == "HEALTHY"


async def test_object_deleted_during_move_leaves_cleanup_to_the_reaper(copy_result):
    pool = FakePool({("c1", "w1"): "HEALTHY", ("c1", OTHER): "HEALTHY"})

    def delete_object():  # begin_delete moves every replica row of the object to DELETING
        for k in pool.rows:
            pool.rows[k] = "DELETING"
    copy_result("ok", side_effect=delete_object)
    assert await rebalance.move(_state(pool), CH, DONOR, RECIP) is False
    assert set(pool.rows.values()) == {"DELETING"}


async def test_recipient_that_already_has_a_row_is_skipped(copy_result):
    copy_result("ok")
    pool = FakePool({("c1", "w1"): "HEALTHY", ("c1", "w5"): "MISSING"})
    assert await rebalance.move(_state(pool), CH, DONOR, RECIP) is False
    assert pool.rows == {("c1", "w1"): "HEALTHY", ("c1", "w5"): "MISSING"}
