from collections import deque

from errors import TransactionError
from transaction.deadlock_detector import *
from transaction.operation import OperationType, ReadOperation, WriteOperation
from transaction.parser import Parser
from transaction.transaction import Transaction


class TransactionManager:
    def __init__(self):
        self.parser = Parser()
        self.transactions = defaultdict()
        self.timestamp = 0
        self.operations = deque()

        self.sites = []
        for i in range(1, 11):
            self.sites.append(DataManager(i))

    def process(self, s):
        """The main processing flow.

        The flow can be described as:
        (1) Parse the input command.
        (2) Check if there is a deadlock. If so, solve the deadlock and try
            to execute the operations that are waiting in queue again.
        (3) Process the specific command and print necessary information.
        (4) Try to execute the operations.
        (5) Increase the timestamp.

        @author Yuejiang Wu
        """
        arguments = self.parser.parse(s)
        if not arguments:
            return
        if self.detect_deadlock():
            self.execute_operations()
            print()
        print('------- Time {} -------'.format(self.timestamp))
        self.process_command(arguments)
        self.execute_operations()
        self.timestamp += 1
        print()

    def process_command(self, arguments: List[str]) -> None:
        """Execute different functions according to the given command.

        @author Yujie Fan
        """
        cmd = arguments[0]

        if cmd == 'begin':
            assert len(arguments) == 2
            self.begin(arguments)

        elif cmd == 'beginRO':
            assert len(arguments) == 2
            self.begin_ro(arguments)

        elif cmd == 'end':
            assert len(arguments) == 2
            self.end(arguments)

        elif cmd == 'W':
            assert len(arguments) == 4
            tid = arguments[1]
            vid = arguments[2]
            value = arguments[3]
            self.add_write_operation(tid, vid, value)

        elif cmd == 'R':
            assert len(arguments) == 3
            tid = arguments[1]
            vid = arguments[2]
            self.add_read_operation(tid, vid)

        elif cmd == 'dump':
            assert len(arguments) == 1
            self.dump()

        elif cmd == 'fail':
            assert len(arguments) == 2
            self.fail(arguments)

        else:
            assert len(arguments) == 2
            self.recover(arguments)

    def execute_operations(self):
        """Try to execute all the operations in queue.

        Side Effect:
            If certain operation is executed successfully, then remove it from queue.

        @author Yujie Fan
        """
        for operation in list(self.operations):
            tid = operation.tid
            vid = operation.vid

            if operation.operation_type == OperationType.R:
                is_success = self.snapshot_read(tid, vid) if self.transactions[tid].is_ro else self.read(tid, vid)
            else:
                is_success = self.write(tid, vid, operation.value)

            if is_success:
                self.operations.remove(operation)

    def begin(self, arguments):
        """Initialize a transaction in the transaction manager.

        Side Effect:
            A normal transaction object will be created in the transaction table,
            whose key is the corresponding transaction id.

        @author Yujie Fan
        """
        tid = arguments[1]
        if tid in self.transactions:
            raise TransactionError("{} has already begun.".format(tid))
        transaction = Transaction(tid, self.timestamp, False)
        self.transactions[tid] = transaction
        print('Transaction {} begins'.format(tid))

    def begin_ro(self, arguments):
        """Initialize a read-only transaction in the transaction manager.

        Side Effect:
            A read-only transaction object will be created in the transaction table,
            whose key is the corresponding transaction id.

        @author Yujie Fan
        """
        tid = arguments[1]
        if tid in self.transactions:
            raise TransactionError("{} has already begun.".format(tid))
        transaction = Transaction(tid, self.timestamp, True)
        self.transactions[tid] = transaction
        print('Read-only transaction {} begins'.format(tid))

    def end(self, arguments):
        """Decide whether to commit or abort the given transaction.

        @author Yujie Fan
        """
        tid = arguments[1]
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} doesn't exist.".format(tid)
        if trans.is_abort:
            self.abort(tid, True)
        else:
            self.commit(tid, self.timestamp)

    def snapshot_read(self, tid, vid):
        """Give the read-only transaction a snapshot value (if possible).

        @author Yujie Fan
        """
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist.".format(tid))
        timestamp = trans.timestamp
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                result_value = site.snapshot_read(vid, timestamp)
                if result_value.is_success:
                    print('Read-only transaction {} reads {}.{}: {}'.format(
                        tid, vid, site.sid, result_value.value))
                    return True
        print('Read-only transaction {} failed to read {}: no suitable site.'.format(tid, vid))
        return False

    def add_read_operation(self, tid, vid):
        """Add a read operation to queue.

        Side Effect:
            A normal read operation object will be appended to the queue.

        @author Yujie Fan
        """
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist, can't add read operation.".format(tid))
        self.operations.append(ReadOperation(tid, vid))

    def add_write_operation(self, tid, vid, value):
        """Add a write operation to queue.

        Side Effect:
            A write operation object will be appended to the queue.

        @author Yujie Fan
        """
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist, can't add write operation.".format(tid))
        self.operations.append(WriteOperation(tid, vid, value))

    def read(self, tid, vid):
        """Execute the read operation of normal transactions(not read-only).

        @author Yujie Fan
        """
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} hasn't begun, read operation fails.".format(tid))
        for site in [x for x in self.sites if (x.is_up and x.has_variable(vid))]:
            result_value = site.read(tid, vid)
            if result_value.is_success:
                trans.visited_sites.append(site.sid)
                print('Transaction {} reads {}.{}: {}'.format(tid, vid, site.sid, result_value.value))
                return True
        return False

    def write(self, tid, vid, value) -> bool:
        """Execute the write operation.

        @author Yuejiang Wu
        """
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist, write operation fails.".format(tid))
        target_sites = []
        for site in [x for x in self.sites if (x.has_variable(vid) and x.is_up)]:
            # If current site is up and has the certain vid, then try to get its write lock.
            # The write operation can only be applied when have all the write locks of up sites.
            write_lock = site.get_write_lock(tid, vid)
            if not write_lock:
                print("{} waits due to write lock conflict. Current lock : {}.".format(
                    tid, site.lock_table[vid].current_lock))
                return False
            target_sites.append(int(site.sid))

        # If no site satisfies the writing condition, then fail to write.
        if not target_sites:
            print()
            return False
        # Otherwise, write to all the up sites that contains the vid.
        for target_sid in target_sites:
            target_site = self.sites[target_sid - 1]
            target_site.write(tid, vid, value)
            self.transactions[tid].visited_sites.append(target_sid)
        print("Transaction {} writes variable {} with value {} to sites {}.".format(tid, vid, value, target_sites))
        return True

    def dump(self):
        """Show the data of all sites.

        @author Yujie Fan
        """
        print("Dump all sites:")
        for site in self.sites:
            site.dump()

    def fail(self, arguments):
        """Fail a site explicitly.

        @author Yujie Fan
        """
        sid = int(arguments[1])
        # site id starts from 1, while the index of self.sites starts from 0.
        site = self.sites[sid - 1]
        if not site.is_up:
            raise TransactionError("Site {} is already down.".format(sid))
        site.fail(sid)
        print("Site {} fails.".format(sid))
        for trans in self.transactions.values():
            if trans.is_ro or trans.is_abort or (sid not in trans.visited_sites):
                continue
            else:
                trans.is_abort = True

    def recover(self, arguments):
        """Recover a site explicitly.

        @author Yujie Fan
        """
        sid = int(arguments[1])
        site = self.sites[sid - 1]
        if site.is_up:
            print("Site {} is up, no need to recover.".format(sid))
            return
        site.recover(self.timestamp)
        print("Site {} recovers.".format(sid))

    def abort(self, tid: str, site_fail=False):
        """Abort a transaction.

        @author Yujie Fan
        """
        for site in self.sites:
            site.abort(tid)
        del self.transactions[tid]
        abort_reason = 'Site Failed' if site_fail else 'Deadlock'
        print('{} aborts. [{}]'.format(tid, abort_reason))
        # Delete all the operations invoked by the aborted transaction.
        for operation in list(self.operations):
            if operation.tid == tid:
                self.operations.remove(operation)

    def commit(self, tid, commit_time):
        """Commit a transaction, and output its commit time.

        @author Yujie Fan
        """
        for site in self.sites:
            site.commit(tid, commit_time)
        self.transactions.pop(tid)
        print("{} commits at time {}.".format(tid, commit_time))

    def detect_deadlock(self) -> bool:
        """Detect if there is a deadlock among existing transactions.

        @author Yujie Fan
        """
        blocking_graph = generate_blocking_graph(self.sites)
        victim = detect(self.transactions, blocking_graph)
        if victim is not None:
            print("Found deadlock, aborts the youngest transaction {}".format(victim))
            self.abort(victim)
            return True
        return False
