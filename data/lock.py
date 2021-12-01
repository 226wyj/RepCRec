from enum import Enum
from collections import deque


class LockType(Enum):
    R = 0,
    W = 1


class Lock:
    def __init__(self, vid: str, tid: str, lock_type: LockType) -> None:
        self.vid = vid
        self.tid = tid
        self.lock_type = lock_type


class ReadLock(Lock):
    def __init__(self, vid: str, tid: str) -> None:
        super(ReadLock, self).__init__(vid, tid, LockType.R)


class WriteLock(Lock):
    def __init__(self, vid: str, tid: str) -> None:
        super(WriteLock, self).__init__(vid, tid, LockType.W)


class LockManager:
    """ Manage locks for a certain variable. """

    def __init__(self, vid: str) -> None:
        self.vid = vid
        self.current_lock = None
        self.lock_queue = deque()
        self.shared_read_lock = set()

    def promote_current_lock(self, write_lock: WriteLock) -> None:
        if not self.current_lock:
            raise "ERROR[0]: No lock on variable {}.".format(self.vid)
        if not self.current_lock.lock_type == LockType.R:
            raise "ERROR[1]: Current lock on variable {} is not a read lock can't promote.".format(self.vid)
        if len(self.shared_read_lock) != 1:
            raise "ERROR[2]: Other transactions are sharing the read lock on variable {}".format(self.vid)
        if write_lock.tid not in self.shared_read_lock:
            raise "ERROR[3]: Transaction {} is not holding " \
                  "the read lock of variable {}, can't promote.".format(write_lock.tid, self.vid)

        # remove current read lock from the shared read lock set, then promote it to a write lock
        self.shared_read_lock.remove(write_lock.tid)
        self.current_lock = write_lock

    def clear(self):
        self.current_lock = None
        self.lock_queue.clear()
        self.shared_read_lock.clear()

    def share_current_lock(self, tid: str):
        if self.current_lock.lock_type == LockType.R:
            self.shared_read_lock.add(tid)
        else:
            raise "ERROR[4]: Transaction {}'s current lock on variable {} " \
                  "is a write lock, which can not be shared."\
                .format(
                    self.current_lock.tid,
                    self.current_lock.vid
                )

    def release_current_lock(self, tid: str) -> None:
        if self.current_lock:
            if self.current_lock.lock_type == LockType.R:
                self.shared_read_lock.remove(tid)
            self.current_lock = None

    def add_lock_to_queue(self, lock) -> None:
        if lock in self.lock_queue:
            return
        for l in self.lock_queue:
            if l.tid == lock.tid:
                if l.lock_type == LockType.R:

        self.lock_queue.append(lock)