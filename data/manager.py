from collections import defaultdict, deque
from lock import LockManager
from value import Value, CommitValue, TemporaryValue, ReadResult


class Variable:
    def __init__(self, vid: str, init: Value, is_replicated):
        self.vid = vid
        self.commit_value_list = deque()
        self.init_value = init
        self.is_replicated = is_replicated
        self.is_readable = True

        self.commit_value_list.append(self.init_value)

    def get_last_commit_value(self):
        return self.commit_value_list[-1]

    def add_commit_value(self, v):
        self.commit_value_list.append(v)


class DataManager:
    def __init__(self, sid):
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

    def fail(self, sid):
        """ Set the `is_up` state to false and clear the lock table. """
        self.is_up = False
        self.lock_table.clear()

    def recover(self, timestamp):
        """
        Record the recover timestamp, and set
        all the replicated variable's state to unreadable.
        """
        self.is_up = True
        self.recover_timestamp.append(timestamp)
        for v in self.data.values():
            if v.is_replicated:
                v.is_readable = False

    def snapshot_read(self, vid, timestamp):
        v = self.data[vid]
        if v.is_readable:
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
