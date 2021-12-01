from enum import Enum
from collections import deque


class LockType(Enum):
    R = 0,
    W = 1


class Lock:
    def __init__(self, vid: str, lock_type: LockType) -> None:
        self.vid = vid
        self.lock_type = lock_type


class ReadLock(Lock):
    def __init__(self, vid: str, tid: str) -> None:
        super(ReadLock, self).__init__(vid, LockType.R)
        self.tid_set = set()
        self.tid_set.add(tid)


class WriteLock(Lock):
    def __init__(self, vid: str, tid: str) -> None:
        super(WriteLock, self).__init__(vid, LockType.W)
        self.tid = tid


class LockManager:
    """ Manage locks for a certain variable. """

    def __init__(self, vid: str) -> None:
        self.vid = vid
        self.current_lock = None
        self.lock_queue = deque()

    def promote_current_lock(self, write_lock: WriteLock) -> None:
        if not self.current_lock:
            raise "There is no lock on variable {}, please recheck.".format(self.vid)
        if not self.current_lock.lock_type == LockType.R:
            raise "Current lock on variable {} is not a read lock can't promote.".format(self.vid)
        if len(self.current_lock.tid_set) != 1:
            raise "Other transactions are sharing the read lock on variable {}".format(self.vid)
        if write_lock.tid not in self.current_lock.tid_set:
            raise "Transaction {} is not holding " \
                  "the read lock of variable {}, can't promote.".format(write_lock.tid, self.vid)
        self.current_lock = write_lock

    def clear(self):
        self.lock_queue.clear()
        self.current_lock = None

    def share_current_lock(self, tid: str):
        if self.current_lock.lock_type == LockType.R:
            self.current_lock.tid_set.add(tid)
        else:
            raise "Transaction {}'s current lock on variable {} " \
                  "is a write lock, which can not be shared."\
                .format(
                    self.current_lock.tid,
                    self.current_lock.vid
                )

    def release_lock(self, tid: str) -> None:
        if self.current_lock:
            if self.current_lock.lock_type == LockType.R:
                if tid in self.current_lock.tid_set:
                    self.current_lock.tid_set.remove(tid)
            else:
                if self.current_lock.lock_type == LockType.W:
                    self.current_lock = None
