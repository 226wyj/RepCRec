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
        self.shared_read_lock = deque()

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
        if self.current_lock.lock_type == LockType.R and tid not in self.shared_read_lock:
            self.shared_read_lock.append(tid)
        else:
            raise "ERROR[4]: Transaction {}'s current lock on variable {} " \
                  "is a write lock, which can not be shared."\
                .format(
                    self.current_lock.tid,
                    self.current_lock.vid
                )

    def release_current_lock(self, tid: str) -> None:
        """ Release current lock, and update shared lock lists if needed. """
        if self.current_lock and self.current_lock.tid == tid:
            if self.current_lock.lock_type == LockType.R:
                self.shared_read_lock.remove(tid)
                if len(self.shared_read_lock) == 0:
                    # If there is no shared read locks, then set current lock to None.
                    self.current_lock = None
                else:
                    # Otherwise, let the next shared read lock be the current lock
                    # according to the first-come-first-serve rule.
                    self.current_lock = ReadLock(self.vid, self.shared_read_lock[0])
            else:
                self.current_lock = None

    def add_lock_to_queue(self, lock) -> None:
        """ Only blocked locks are added to the queue. """
        all_queued_tid = [lock.tid for lock in self.lock_queue]
        if lock in self.lock_queue:
            # If the same kind of lock has already been in the queue, then return.
            return
        elif lock.tid in all_queued_tid and lock.lock_type == LockType.R:
            # If the same transaction has lock in queue, then there are two possibilities:
            # (1) R lock in queue and W new lock;
            # (2) W lock in queue and R new lock;
            # If the new lock is R type, then return.
            return
        else:
            self.lock_queue.append(lock)

    def remove_lock_from_queue(self, tid) -> None:
        """ Remove all the lock whose tid is equal to the given tid. """
        self.lock_queue = deque([lock for lock in self.lock_queue if lock.tid != tid])

    def set_current_lock(self, lock):
        if lock.lock_type == LockType.R:
            self.shared_read_lock.append(lock.tid)
        self.current_lock = lock

    def has_write_lock(self):
        for lock in self.lock_queue:
            if lock.lock_type == LockType.W:
                return True
        return False

    def has_other_write_lock(self, tid):
        for lock in self.lock_queue:
            if lock.lock_type == LockType.W:
                if lock.tid != tid:
                    return True
        return False
