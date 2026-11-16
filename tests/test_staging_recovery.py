"""Start-up recovery of the staging directory: staging files and upload rows must agree after any crash."""
import uuid

from app.controller import upload


class FakePool:
    def __init__(self, uploads):
        self.uploads, self.deleted = uploads, []

    async def fetch(self, sql, *args):
        return [{"object_id": oid, "state": st} for oid, st in self.uploads]

    async def execute(self, sql, oid):
        assert sql.startswith("DELETE FROM objects")
        self.deleted.append(oid)


def ids(n):
    return [uuid.uuid4() for _ in range(n)]


async def test_crash_while_streaming_drops_row_and_partial_file(tmp_path):
    (a,) = ids(1)
    (tmp_path / f"{a}.uploading").write_bytes(b"partial")
    pool = FakePool([(a, "STREAMING")])
    out = await upload.recover_staging(pool, str(tmp_path))
    assert pool.deleted == [a] and not (tmp_path / f"{a}.uploading").exists()
    assert out == {"streaming_dropped": 1, "renames_finished": 0, "files_removed": 1}


async def test_crash_after_commit_point_finishes_the_rename(tmp_path):
    (a,) = ids(1)
    (tmp_path / f"{a}.uploading").write_bytes(b"complete file")
    out = await upload.recover_staging(FakePool([(a, "STAGED")]), str(tmp_path))
    assert (tmp_path / f"{a}.staged").read_bytes() == b"complete file"
    assert not (tmp_path / f"{a}.uploading").exists() and out["renames_finished"] == 1


async def test_staged_file_with_its_row_is_kept_for_the_resume_loop(tmp_path):
    (a,) = ids(1)
    (tmp_path / f"{a}.staged").write_bytes(b"x")
    out = await upload.recover_staging(FakePool([(a, "STAGED")]), str(tmp_path))
    assert (tmp_path / f"{a}.staged").exists() and out == {"streaming_dropped": 0, "renames_finished": 0,
                                                             "files_removed": 0}


async def test_files_without_rows_are_removed_and_others_ignored(tmp_path):
    a, b = ids(2)
    (tmp_path / f"{a}.staged").write_bytes(b"orphan: the old crash-between-rename-and-insert leak")
    (tmp_path / f"{b}.uploading").write_bytes(b"orphan partial")
    (tmp_path / "notes.txt").write_text("not ours")
    (tmp_path / "subdir.staged").mkdir()
    out = await upload.recover_staging(FakePool([]), str(tmp_path))
    assert out["files_removed"] == 2
    assert not (tmp_path / f"{a}.staged").exists() and not (tmp_path / f"{b}.uploading").exists()
    assert (tmp_path / "notes.txt").exists() and (tmp_path / "subdir.staged").is_dir()
