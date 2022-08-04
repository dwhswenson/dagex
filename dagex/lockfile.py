import contextlib
import pathlib
import time

class TimeoutError(RuntimeError):
    pass

class LockFile(object):
    """
    We use a separate file for locking purposes. We don't even open the real
    file until we obtain the lock.
    """
    def __init__(self, filename, timeout, delay):
        self.filename = pathlib.Path(filename)
        self.timeout = timeout
        self.delay = delay

        basename = self.filename.name
        dirname = self.filename.parent
        self.lockfile = dirname / ("." + basename + ".lock")
        self.lock = None

    def acquire(self):
        acquired = False
        start_time = time.time()
        while acquired is False:
            try:
                lock = open(self.lockfile, mode='x+')
            except FileExistsError:
                now = time.time()
                if now - start_time > self.timeout:
                    raise TimeoutError("Couldn't acquire lock (%s sec)" %
                                       self.timeout)
                else:
                    time.sleep(self.delay)
            else:
                acquired = True
                self.lock = lock
                self.lock.write(repr(self))

    def release(self):
        self.lock.close()
        self.lockfile.unlink()

    def validate_lock(self):
        self.lock.seek(0)
        assert self.lock.read() == repr(self)
