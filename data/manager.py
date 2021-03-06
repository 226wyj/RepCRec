from collections import defaultdict

from data.lock import ReadLock, WriteLock, LockManager, LockType, is_conflict
from data.value import CommitValue, TemporaryValue, ResultValue
from data.variable import Variable


class DataManager:
    """Manage all the data in a site
    """
    def __init__(self, sid: int) -> None:
        self.sid = sid  # site id
        self.is_up = True  # whether the site is down or not
        self.data = defaultdict()  # all the variables stored in the site
        self.lock_table = defaultdict()  # lock managers for each variable
        self.fail_timestamp = []  # record all the fail time of this site
        self.recover_timestamp = []  # record all the recover time of this site

        # initialize variables
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
        return True if vid in self.data else False

    def snapshot_read(self, vid: str, timestamp: int) -> ResultValue:
        """Return the snapshot value for read-only transactions.
        """
        v: Variable = self.data[vid]
        if not v.is_readable:
            return ResultValue(None, False)
        else:
            for commit_value in v.commit_value_list:
                if commit_value.commit_time <= timestamp:
                    if v.is_replicated:
                        # If the site wasn't up all the time between
                        # the time when xi was committed and RO began,
                        # then this RO can abort.
                        for t in self.fail_timestamp:
                            if commit_value < t <= timestamp:
                                return ResultValue(None, False)
                    return ResultValue(commit_value.value, True)
            return ResultValue(None, False)

    def read(self, tid: str, vid: str) -> ResultValue:
        """Return the value for normally-read transactions.
        """
        v: Variable = self.data[vid]
        if not v.is_readable:
            print('{} failed to read {}.{} [Site just recovered, not readable]'.format(tid, vid, self.sid))
            return ResultValue(None, False)
        else:
            lock_manager: LockManager = self.lock_table[vid]
            current_lock = lock_manager.current_lock

            # If there's no lock on the variable, set a read lock then read directly.
            if not current_lock:
                lock_manager.set_current_lock(ReadLock(tid, vid))
                return ResultValue(v.get_last_commit_value(), True)

            # There is a read lock on the variable.
            if current_lock.lock_type == LockType.R:
                # If the transaction shares the read lock, then it can read the variable.
                if tid in lock_manager.shared_read_lock:
                    return ResultValue(v.get_last_commit_value(), True)
                else:
                    # The transaction doesn't share the read lock, and there are other write
                    # locks waiting in front, so the read lock should wait in queue.
                    if lock_manager.has_write_lock():
                        lock_manager.add_lock_to_queue(ReadLock(tid, vid))
                        print('{} failed to read {}.{} [Exist write locks waiting in front]'.format(tid, vid, self.sid))
                        return ResultValue(None, False)
                    else:
                        # There is no other write locks waiting, then share the current read lock
                        # and return the read value.
                        lock_manager.share_current_lock(tid)
                        return ResultValue(v.get_last_commit_value(), True)

            # There is a write lock on the variable.
            else:
                # If current transaction has already held a write lock on variable, then it
                # will read the temporary value for the write has not been committed.
                if tid == current_lock.tid:
                    return ResultValue(v.get_temporary_value(), True)
                else:
                    lock_manager.add_lock_to_queue(ReadLock(tid, vid))
                    print('{} failed to read {}.{} [Lock conflict]'.format(tid, vid, self.sid))
                    return ResultValue(None, False)

    def get_write_lock(self, tid, vid) -> bool:
        """
        To judge whether a transaction can get the write
        lock of the certain variable.
        """
        lock_manager: LockManager = self.lock_table.get(vid)
        current_lock = lock_manager.current_lock
        # print(tid, current_lock)
        # There is no lock on the variable currently,
        # so set the current lock to write lock and return True.
        if not current_lock:
            # lock_manager.set_current_lock(WriteLock(tid, vid))
            return True
        else:
            if current_lock.lock_type == LockType.R:
                if len(lock_manager.shared_read_lock) != 1:
                    # There are multiple transactions holding the read lock of the variable,
                    # so the transaction can't get a write lock, and this request has to
                    # wait in queue.
                    lock_manager.add_lock_to_queue(WriteLock(tid, vid))
                    return False
                else:
                    if tid in lock_manager.shared_read_lock:
                        if not lock_manager.has_other_write_lock(tid):
                            # The transaction holds the read lock of the variable currently,
                            # and there is no other write transactions waiting in queue, can
                            # promote its read lock to write lock.
                            # lock_manager.promote_current_lock(WriteLock(tid, vid))
                            return True
                        else:
                            lock_manager.add_lock_to_queue(WriteLock(tid, vid))
                            return False
                    else:
                        # There are other transactions holding the read lock.
                        lock_manager.add_lock_to_queue(WriteLock(tid, vid))
                        return False
            else:
                # There are other transactions holding the write lock.
                if current_lock.tid == tid:
                    return True
                else:
                    lock_manager.add_lock_to_queue(WriteLock(tid, vid))
                    return False

    def write(self, tid, vid, value) -> None:
        """Add write lock to certain site and write its temporary value.

        This method will only be called after getting the write lock successfully,
        so there are no extra checking steps.
        """
        lock_manager: LockManager = self.lock_table.get(vid)
        v: Variable = self.data.get(vid)
        assert lock_manager is not None and v is not None
        current_lock = lock_manager.current_lock
        if current_lock:
            if current_lock.lock_type == LockType.R:
                # If current lock is a read lock, then it must be of the same transaction,
                # and there should be no other locks waiting in queue.
                assert len(lock_manager.shared_read_lock) == 1 and \
                       tid in lock_manager.shared_read_lock and \
                       not lock_manager.has_other_write_lock(tid)
                lock_manager.promote_current_lock(WriteLock(tid, vid))
                v.temporary_value = TemporaryValue(value, tid)
            else:
                # If current lock is a write lock, then it must of the same transaction.
                assert tid == current_lock.tid
                v.temporary_value = TemporaryValue(value, tid)
        else:
            lock_manager.set_current_lock(WriteLock(tid, vid))
            v.temporary_value = TemporaryValue(value, tid)

    def dump(self):
        """Show all the variables in the site.
        """
        site_status = 'up' if self.is_up else 'down'
        output = 'site {} [{}] - '.format(self.sid, site_status)
        for v in self.data.values():
            output += '{}: {}, '.format(v.vid, v.get_last_commit_value())
        print(output)

    def abort(self, tid):
        """Abort certain transactino.

        Side Effect:
            (1) All the locks that required by this transaction will be released.
            (2) All the operations of this transaction that are waiting in queue
                will be dropped.
        """
        for lock_manager in self.lock_table.values():
            lock_manager.release_current_lock(tid)
            lock_manager.remove_lock_from_queue(tid)
        self.update_lock_table()

    def commit(self, tid, commit_time):
        """Commit the given transaction.

        Side Effect:
            (1) If the transaction writes temporary value on the site,
                then save it as the commit value.
            (2) Release all the locks that are required by the transaction.
        """
        # Release locks.
        for lock_manager in self.lock_table.values():
            lock_manager.release_current_lock(tid)

        # Commit temporary values.
        for v in self.data.values():
            if v.temporary_value is not None and v.temporary_value.tid == tid:
                commit_value = v.temporary_value.value
                v.add_commit_value(CommitValue(commit_value, commit_time))
                v.temporary_value = None
                v.is_readable = True
        self.update_lock_table()

    def update_lock_table(self):
        """Update the lock table if current lock has been released.

        If current lock is released and there are other locks waiting in queue,
        the first lock in queue should be popped and become the current lock according
        to the First-Come-First-Serve protocol. If this lock is a read lock, then
        its followed read locks will share the same read lock until there is a
        write lock
        """
        for lock_manager in [x for x in self.lock_table.values() if not x.current_lock]:
            if len(lock_manager.lock_queue) == 0:
                continue
            first_waiting = lock_manager.lock_queue.popleft()

            lock_manager.set_current_lock(first_waiting)
            if first_waiting.lock_type == LockType.R and lock_manager.lock_queue:
                # If multiple read locks are blocked before a write lock, then
                # pop these read locks out of the queue and make them share the read lock.
                next_lock = lock_manager.lock_queue.popleft()
                while next_lock.lock_type == LockType.R and lock_manager.lock_queue:
                    lock_manager.shared_read_lock.add(next_lock.tid)
                    next_lock = lock_manager.lock_queue.popleft()
                lock_manager.lock_queue.appendleft(next_lock)

                # If the current lock is a read lock, and the next lock is the write lock
                # of the same transaction, then promote the current read lock.
                if len(lock_manager.shared_read_lock) == 1 and \
                        next_lock.tid == lock_manager.shared_read_lock[0]:
                    lock_manager.promote_current_lock(WriteLock(next_lock.tid, lock_manager.vid))
                    lock_manager.lock_queue.popleft()

    def fail(self, timestamp: int) -> None:
        """Fail the current site.

        Side Effect:
            (1) The `is_up` state would be set to false.
            (2) The lock table will be cleared.
        """
        self.is_up = False
        self.fail_timestamp.append(timestamp)
        for lock_manager in self.lock_table.values():
            lock_manager.clear()

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

    def generate_blocking_graph(self) -> defaultdict:
        """Generate blocking graph for the current site.
        """
        blocking_graph = defaultdict(set)
        for lock_manager in self.lock_table.values():
            if not lock_manager.current_lock or len(lock_manager.lock_queue) == 0:
                continue

            # Generate lock graph for the current lock with other locks in queue.
            current_lock = lock_manager.current_lock
            for lock in lock_manager.lock_queue:
                if is_conflict(current_lock, lock):
                    # If current lock is a read lock, then all the other transactions
                    # that share the same read lock would be conflicted with the write
                    # lock in queue.
                    if current_lock.lock_type == LockType.R:
                        for shared_lock_tid in [x for x in lock_manager.shared_read_lock
                                                if not (x == lock.tid)]:
                            blocking_graph[lock.tid].add(shared_lock_tid)
                    else:
                        # If current lock is a write lock, then according our rule of adding locks
                        # in queue, the blocked lock can only be the R/W locks of other transactions.
                        # Therefore, add to blocking graph directly.
                        blocking_graph[lock.tid].add(current_lock.tid)

            # Generate lock graph for the locks in queue with each other.
            for i in range(len(lock_manager.lock_queue)):
                lock1 = lock_manager.lock_queue[i]
                for j in range(i):
                    lock2 = lock_manager.lock_queue[j]
                    if is_conflict(lock2, lock1):
                        blocking_graph[lock1.tid].add(lock2.tid)
        return blocking_graph
