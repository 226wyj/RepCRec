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
        arguments = self.parser.parse(s)
        if not arguments:
            return
        # print('arguments: ')
        # print(arguments)
        if self.detect_deadlock():
            self.execute_operations()
        self.process_command(arguments)
        self.execute_operations()
        self.timestamp += 1

    def process_command(self, arguments: List[str]) -> None:
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
        i = 0
        while i < len(self.operations):
            operation = self.operations[i]
            tid = operation.tid
            vid = operation.vid

            if operation.operation_type == OperationType.R:
                is_success = self.snapshot_read(tid, vid) if self.transactions[tid].is_ro else self.read(tid, vid)
            else:
                is_success = self.write(tid, vid, operation.value)

            if is_success:
                del self.operations[i]
            else:
                i += 1

    def begin(self, arguments):
        tid = arguments[1]
        if tid in self.transactions:
            raise TransactionError("Transaction {} has already begun.".format(tid))
        transaction = Transaction(tid, self.timestamp, False)
        self.transactions[tid] = transaction
        print('Transaction {} begins.'.format(tid))

    def begin_ro(self, arguments):
        tid = arguments[1]
        if tid in self.transactions:
            raise TransactionError("Transaction {} has already begun.".format(tid))
        transaction = Transaction(tid, self.timestamp, True)
        self.transactions[tid] = transaction
        print('Read-only transaction {} begins.'.format(tid))

    def end(self, arguments):
        tid = arguments[1]
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} doesn't exist.".format(tid)
        if trans.is_abort:
            self.abort(tid, True)
        else:
            self.commit(tid, self.timestamp)

    def snapshot_read(self, tid, vid):
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist.".format(tid))
        timestamp = trans.timestamp
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                result_value = site.snapshot_read(vid, timestamp)
                if result_value.is_success:
                    print('Read-only transaction {} reads {} from {}: {}'.format(
                        tid, vid, site.sid, result_value.value))
                    return True
        return False

    def add_read_operation(self, tid, vid):
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist, can't add read operation.".format(tid))
        self.operations.append(ReadOperation(tid, vid))

    def add_write_operation(self, tid, vid, value):
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} doesn't exist, can't add write operation.".format(tid))
        self.operations.append(WriteOperation(tid, vid, value))

    def read(self, tid, vid):
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} hasn't begun, read operation fails.".format(tid))
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                result_value = site.read(tid, vid)
                if result_value.is_success:
                    trans.visited_sites.append(site.sid)
                    print('Normal read transaction {} reads {} from {}: {}'.format(
                        tid, vid, site.sid, result_value.value))
                    return True
        return False

    def write(self, tid, vid, value) -> bool:
        print('Transactions begun:')
        print(self.transactions)
        trans = self.transactions.get(tid)
        if not trans:
            raise TransactionError("Transaction {} hasn't begun, write operation fails.".format(tid))
        target_sites = []
        for site in self.sites:
            if site.has_variable(vid):
                # If current site is down, then check other sites that may contain the certain vid.
                if not site.is_up:
                    continue
                # If current site is up and has the certain vid, then try to get its write lock.
                # The write operation can only be applied when got all the write locks of up sites.
                print('transaction {}, writes variable {} with value {} to site {}'
                      .format(tid, vid, value, site.sid))
                write_lock = site.get_write_lock(tid, vid)
                if not write_lock:
                    return False
                target_sites.append(site)

        # No site satisfies the condition, fail to write.
        if not target_sites:
            return False
        # Otherwise, write to all the up sites that contains the vid.
        # print('target sites:')
        # print(target_sites)
        for target_site in target_sites:
            # lock_table = target_site.lock_table
            # data = target_site.data
            # print('data:')
            # print(data)
            # lock_manager = lock_table.get(vid)
            # print('Current lock:')
            # print(lock_manager.current_lock)

            target_site.write(tid, vid, value)
            self.transactions[tid].visited_sites.append(target_site.sid)
        print("Transaction {} finished writing variable {} with value {} to sites {}."
              .format(tid, vid, value, [site.sid for site in target_sites]))
        return True

    def dump(self):
        print("Dump all sites:")
        for site in self.sites:
            site.dump()

    def fail(self, arguments):
        sid = int(arguments[1])

        # site id starts from 1, while the index of self.sites starts from 0.
        site = self.sites[sid - 1]
        if not site.is_up:
            raise TransactionError("Site {} has already down.".format(sid))
        site.fail(sid)
        print("Site {} fails.".format(sid))
        for trans in self.transactions.values():
            if trans.is_ro or trans.is_abort or (sid not in trans.visited_sites):
                continue
            else:
                trans.is_abort = True

    def recover(self, arguments):
        sid = int(arguments[1])
        site = self.sites[sid - 1]
        if site.is_up:
            print("Site {} is up, no need to recover.".format(sid))
            return
        site.recover(self.timestamp)
        print("Site {} recovers.".format("sid"))

    def abort(self, tid: str, site_fail=False):
        for site in self.sites:
            site.abort(tid)
        del self.transactions[tid]
        abort_reason = 'Site Fail' if site_fail else 'Deadlock'
        print('Abort transaction {} because of: {}'.format(tid, abort_reason))
        # Delete all the operations invoked by the aborted transaction.
        for operation in list(self.operations):
            if operation.tid == tid:
                self.operations.remove(operation)

    def commit(self, tid, commit_time):
        for site in self.sites:
            site.commit(tid, commit_time)
        self.transactions.pop(tid)
        print("Transaction {} commits at time {}.".format(tid, commit_time))

    def detect_deadlock(self) -> bool:
        blocking_graph = generate_blocking_graph(self.sites)
        victim = detect(self.transactions, blocking_graph)
        # self.deadlock_detector.update_blocking_graph(self.sites)
        # victim = self.deadlock_detector.detect(self.transactions)
        if victim is not None:
            print("Found deadlock, abort the youngest transaction {}".format(victim))
            self.abort(victim)
            return True
        return False
