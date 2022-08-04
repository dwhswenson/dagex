import pytest

import tempfile
import pathlib

from lockfile import *

class TestLockFile(object):
    def setup(self):
        self.tmpdir = pathlib.Path(tempfile.mkdtemp())
        self.filename = self.tmpdir / "foo.bar"
        self.lockfilename = self.tmpdir / ".foo.bar.lock"
        self.lockfile = LockFile(self.filename, timeout=0.2, delay=0.05)

    def teardown(self):
        files = [self.filename, self.tmpdir / ".foo.bar.lock"]
        for path in [f for f in files if f.is_file()]:
            path.unlink()
        self.tmpdir.rmdir()

    def test_acquire_and_release(self):
        assert not self.lockfilename.is_file()
        self.lockfile.acquire()
        assert self.lockfilename.is_file()
        self.lockfile.validate_lock()
        self.lockfile.release()
        assert not self.lockfilename.is_file()

    def test_locked_timeout(self):
        existing_lock = open(self.lockfilename, mode='x+')
        with pytest.raises(TimeoutError):
            self.lockfile.acquire()

        existing_lock.close()
        self.lockfilename.unlink()
