from collections import deque
from enum import Enum

from errors import LockError


class LockType(Enum):
    R = 0,
    W = 1


class Lock:
    def __init__(self, tid: str, vid: str, lock_type: LockType) -> None:
        self.tid = tid  # transaction id
        self.vid = vid  # variable id
        self.lock_type = lock_type  # either R or W


class ReadLock(Lock):
    def __init__(self, tid: str, vid: str) -> None:
        super(ReadLock, self).__init__(tid, vid, LockType.R)

    def __repr__(self):
        return '(ReadLock {} {})'.format(self.tid, self.vid)


class WriteLock(Lock):
    def __init__(self, tid: str, vid: str) -> None:
        super(WriteLock, self).__init__(tid, vid, LockType.W)

    def __repr__(self):
        return '(WriteLock {} {})'.format(self.tid, self.vid)


class LockManager:
    """Manage locks for a certain variable.
    """

    def __init__(self, vid: str) -> None:
        self.vid = vid
        self.current_lock = None
        self.lock_queue = deque()
        self.shared_read_lock = deque()  # Stores all the tid that are sharing the read lock, including current lock.

    def promote_current_lock(self, write_lock: WriteLock) -> None:
        """Promote the current read lock to write lock if possible.
        """
        if not self.current_lock:
            raise LockError("ERROR[0]: No lock on variable {}.".format(self.vid))
        if not self.current_lock.lock_type == LockType.R:
            raise LockError("ERROR[1]: Current lock on variable {} is not a read lock can't promote.".format(self.vid))
        if len(self.shared_read_lock) != 1:
            raise LockError("ERROR[2]: Other transactions are sharing the read lock on variable {}".format(self.vid))
        if write_lock.tid not in self.shared_read_lock:
            raise LockError("ERROR[3]: Transaction {} is not holding ".format(write_lock.tid) +
                            "the read lock of variable {}, can't promote.".format(self.vid))

        # remove current read lock from the shared read lock set, then promote it to a write lock
        self.shared_read_lock.remove(write_lock.tid)
        self.current_lock = write_lock
        print("After promotion, current lock: ", self.current_lock)

    def clear(self):
        """
        Clear the lock manager's message, is used when a site wants to
        clear its lock table.
        """
        self.current_lock = None
        self.lock_queue.clear()
        self.shared_read_lock.clear()

    def share_current_lock(self, tid: str):
        """Share the current read lock with other transactions if possible.
        """
        if self.current_lock.lock_type == LockType.R and tid not in self.shared_read_lock:
            self.shared_read_lock.append(tid)
        else:
            raise LockError(
                ("ERROR[4]: Transaction {}'s current lock on variable {} is a "
                 "write lock, which can not be shared.".format(self.current_lock.tid, self.current_lock.vid))
            )

    def release_current_lock(self, tid: str) -> None:
        """Release current lock, and update shared lock lists if needed.
        """
        if self.current_lock:
            if self.current_lock.lock_type == LockType.R:
                if tid in self.shared_read_lock:
                    self.shared_read_lock.remove(tid)
                if len(self.shared_read_lock) == 0:
                    self.current_lock = None
            else:
                if self.current_lock.tid == tid:
                    self.current_lock = None

    def add_lock_to_queue(self, lock) -> None:
        """Only blocked locks are added to the queue.
        """
        for waited_lock in self.lock_queue:
            if waited_lock.tid == lock.tid:
                if waited_lock.lock_type == lock.lock_type or lock.lock_type == LockType.R:
                    return
        self.lock_queue.append(lock)

    def remove_lock_from_queue(self, tid) -> None:
        """Remove all the lock whose tid is equal to the given tid.
        """
        self.lock_queue = deque([lock for lock in self.lock_queue if lock.tid != tid])

    def set_current_lock(self, lock):
        if lock.lock_type == LockType.R:
            self.shared_read_lock.append(lock.tid)
        self.current_lock = lock

    def has_write_lock(self):
        """Check if there is a write lock waiting in queue.
        """
        for lock in self.lock_queue:
            if lock.lock_type == LockType.W:
                return True
        return False

    def has_other_write_lock(self, tid):
        """
        Check if there is a write lock waiting in queue apart from
        that of the same transaction.
        """
        for lock in self.lock_queue:
            if lock.lock_type == LockType.W:
                if lock.tid != tid:
                    return True
        return False


def is_conflict(lock1, lock2) -> bool:
    """Static method, to judge if lock1 conflicts with lock2.

    Principle:
        (1) R lock conflicts with W lock.
        (2) W lock conflicts with R lock and W lock.

    Besides, It is guaranteed that a lock won't be conflicted with itself.
    """
    if lock1.lock_type == LockType.R and lock2.lock_type == LockType.R:
        return False
    else:
        return True if lock1.tid != lock2.tid else False
