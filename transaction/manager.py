from collections import defaultdict, deque
from transaction.parser import Parser
from transaction.transaction import Transaction
from data.manager import DataManager


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
            self.write(arguments)
        elif cmd == 'R':
            self.read(arguments)
        elif cmd == 'dump':
            self.dump(arguments)
        elif cmd == 'fail':
            self.fail(arguments)
        else:
            self.recover(arguments)

        self.execute_operations()
        self.timestamp += 1

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

    def read(self, arguments):
        tid = arguments[1]
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "Transaction {} hasn't begun, read operation fails.".format(tid)
        vid = arguments[2]
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                result_value = site.read(vid, tid)
                if result_value.is_success:
                    trans.visited_sites.append(site.sid)
                    print('Normal read transaction {} reads {} from {}: {}'.format(
                        tid, vid, site.sid, result_value.value))
                    return True
        return False

    def write(self, arguments):
        pass

    def dump(self, arguments):
        print("Dump Operation:")
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
        pass

    def execute_operations(self):
        pass

    def abort(self, tid: str, site_fail=False):
        for site in self.sites:
            site.abort(tid)
        del self.transactions[tid]
        abort_reason = 'Site Fail' if site_fail else 'Deadlock'
        print('Abort transaction {} because of: {}'.format(tid, abort_reason))

    def commit(self, tid, commit_time):
        pass