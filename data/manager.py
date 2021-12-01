from collections import defaultdict, deque
from lock import LockManager, LockType
from variable import Variable
from value import CommitValue, TemporaryValue, ReadResult


class DataManager:
    def __init__(self, sid: int) -> None:
        self.sid = sid
        self.is_up = True
        self.data = defaultdict()
        self.lock_table = defaultdict()
        self.fail_timestamp = []
        self.recover_timestamp = []

        for i in range(1, 21):
            vid = 'x{}'.format(i)
            init_val = i * 10
            if i % 2 == 0:
                # replicated value
                self.data[vid] = Variable(
                    vid=vid,
                    init=CommitValue(init_val, 0),
                    is_replicated=True
                )
                # create lock table
                self.lock_table[vid] = LockManager(vid)
            else:
                if i % 10 + 1 == self.sid:
                    # not replicated value
                    self.data[vid] = Variable(
                        vid=vid,
                        init=CommitValue(init_val, 0),
                        is_replicated=False
                    )
                # create lock table
                self.lock_table[vid] = LockManager(vid)

    def has_variable(self, vid: str) -> bool:
        return False if vid not in self.data else True

    def fail(self, timestamp: int) -> None:
        """ Set the `is_up` state to false and clear the lock table. """
        self.is_up = False
        self.fail_timestamp.append(timestamp)
        self.lock_table.clear()

    def recover(self, timestamp: int) -> None:
        """
        Record the recover timestamp, and set
        all the replicated variable's state to unreadable.
        """
        self.is_up = True
        self.recover_timestamp.append(timestamp)
        for v in self.data.values():
            if v.is_replicated:
                v.is_readable = False

    def snapshot_read(self, vid: str, timestamp: int) -> ReadResult:
        v: Variable = self.data[vid]
        if not v.is_readable:
            return ReadResult(None, False)
        else:
            for commit_value in v.commit_value_list:
                if commit_value.commit_time <= timestamp:
                    if v.is_replicated:
                        # If the site wasn't up all the time between
                        # the time when xi was committed and RO began,
                        # then this RO can abort.
                        for t in self.fail_timestamp:
                            if commit_value < t <= timestamp:
                                return ReadResult(None, False)
                    return ReadResult(commit_value.value, True)
            return ReadResult(None, False)

    def read(self, tid: str, vid: str) -> ReadResult:
        v: Variable = self.data[vid]
        if not v.is_readable:
            return ReadResult(None, False)
        else:
            lock_manager: LockManager = self.lock_table[vid]
            current_lock = lock_manager.current_lock
            if current_lock:
                if current_lock.lock_type == LockType.R:
                    if tid in current_lock.tid_set:
                        return ReadResult(v.get_last_commit_value(), True)
                    
