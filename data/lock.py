from enum import Enum
from collections import deque


class LockType(Enum):
    R = 0,
    W = 1


class Lock:
    def __init__(self, vid, lock_type):
        self.vid = vid
        self.lock_type = lock_type


class ReadLock(Lock):
    def __init__(self, vid, tid):
        super(ReadLock, self).__init__(vid, LockType.R)
        self.tid_set = set()
        self.tid_set.add(tid)


class WriteLock(Lock):
    def __init__(self, vid, tid):
        super(WriteLock, self).__init__(vid, LockType.W)
        self.tid = tid


class LockManager:
    """ Manage locks for a certain variable. """

    def __init__(self, vid):
        self.vid = vid
        self.current_lock = None
        self.lock_queue = deque()

    def clear(self):
        self.lock_queue.clear()
        self.current_lock = None

    def share_current_lock(self, tid):
        if self.current_lock.lock_type == LockType.R:
            self.current_lock.tid_set.add(tid)
        else:
            raise "Transaction {}'s current lock on variable {} " \
                  "is a write lock, which can not be shared."\
                .format(
                    self.current_lock.tid,
                    self.current_lock.vid
                )
