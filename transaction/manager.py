from collections import defaultdict, deque
from transaction.parser import Parser
from transaction.transaction import Transaction
from data.manager import DataManager
from operation import OperationType, ReadOperation, WriteOperation


class TransactionManager:
    def __init__(self):
        self.parser = Parser()
        self.transactions = defaultdict()
        self.timestamp = 0
        self.operations = deque()
        self.sites = []
        for i in range(1, 11):
            self.sites.append(DataManager(i))

    def process(self, s: str) -> None:
        arguments = self.parser.parse(s)
        cmd = arguments[0]

        if cmd == 'begin':
            self.begin(arguments)
        elif cmd == 'beginRO':
            self.begin_ro(arguments)
        elif cmd == 'end':
            self.end(arguments)
        elif cmd == 'W':
            tid = arguments[1]
            vid = arguments[2]
            value = arguments[3]
            self.write(tid, vid, value)
        elif cmd == 'R':
            tid = arguments[1]
            vid = arguments[2]
            self.read(tid, vid)
        elif cmd == 'dump':
            self.dump()
        elif cmd == 'fail':
            self.fail(arguments)
        else:
            self.recover(arguments)

        self.execute_operations()
        self.timestamp += 1

    def add_read_operation(self, tid, vid):
        trans = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} doesn't exist, can't add read operation.".format(tid)
        self.operations.append(ReadOperation(tid, vid))

    def add_write_operation(self, tid, vid, value):
        trans = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} doesn't exist, can't add write operation.".format(tid)
        self.operations.append(WriteOperation(tid, vid, value))

    def execute_operations(self):
        while self.operations:
            operation = self.operations.popleft()
        # for operation in self.operations:
            tid = operation.tid
            vid = operation.vid
            if operation.operation_type == OperationType.R:
                is_success = self.snapshot_read(tid, vid) if self.transactions[tid].is_ro else self.read(tid, vid)
            else:
                is_success = self.write(tid, vid, operation.value)

    def begin(self, arguments):
        tid = arguments[1]
        if tid in self.transactions:
            raise "Transaction {} has already begun.".format(tid)
        transaction = Transaction(tid, self.timestamp, False)
        self.transactions[tid] = transaction
        print('Transaction {} begins.'.format(tid))

    def begin_ro(self, arguments):
        tid = arguments[1]
        if tid in self.transactions:
            raise "Transaction {} has already begun.".format(tid)
        transaction = Transaction(tid, self.timestamp, True)
        self.transactions[tid] = transaction
        print('Read-only transaction {} begins.'.format(tid))

    def end(self, arguments):
        tid = arguments[1]
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} doesn't exist.".format(tid)
        if trans.is_abort:
            self.abort(tid)
        else:
            self.commit(tid, self.timestamp)

    def snapshot_read(self, tid, vid):
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "ERROR, Transaction {} doesn't exist.".format(tid)
        timestamp = trans.timestamp
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                result_value = site.snapshot_read(vid, timestamp)
                if result_value.is_success:
                    print('Read-only transaction {} reads {} from {}: {}'.format(
                        tid, vid, site.sid, result_value.value))
                    return True
        return False

    def read(self, tid, vid):
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} hasn't begun, read operation fails.".format(tid)
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
        trans = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} hasn't begun, write operation fails.".format(tid)
        target_sites = []
        for site in self.sites:
            if site.has_variable(vid):
                # If current site is down, then check other sites that may contain the certain vid.
                if not site.is_up:
                    continue
                # If current site is up and has the certain vid, then try to get its write lock.
                # The write operation can only be applied when got all the write locks of up sites.
                write_lock = site.get_write_lock(tid, vid)
                if not write_lock:
                    return False
                target_sites.append(site)

        # No site satisfies the condition, fail to write.
        if not target_sites:
            return False
        # Otherwise, write to all the up sites that contains the vid.
        for target_site in target_sites:
            target_site.write(tid, vid, value)
            self.transactions[tid].visited_sites.append(target_site.sid)
        print("Transaction {} writes variable {} with value {} to sites {}.".format(tid, vid, value, target_sites))
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
            raise "Site {} has already down.".format(sid)
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

    def commit(self, tid, commit_time):
        for site in self.sites:
            site.commit(tid, commit_time)
        del self.sites[tid]
        print("Transaction {} commits at time {}.".format(tid, commit_time))