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
        if tid not in self.transactions:
            raise "Transaction {} doesn't exist.".format(tid)
        if self.transactions[tid].is_abort:
            self.abort(tid)
        else:
            self.commit(tid, self.timestamp)
        pass

    def snapshot_read(self, tid, vid):
        trans: Transaction = self.transactions.get(tid)
        if not trans:
            raise "ERROR, Transaction {} doesn't exist.".format(tid)
        timestamp = trans.timestamp


    def read(self, arguments):
        tid = arguments[0]
        if tid not in self.transactions:
            raise "Transaction {} hasn't begun, its read operation fails.".format(tid)

        vid = arguments[1]
        for site in self.sites:
            if site.is_up and site.has_variable(vid):
                pass
        pass

    def write(self, arguments):
        pass



    def dump(self, arguments):
        pass

    def fail(self, arguments):
        sid = int(arguments[1])
        site = self.sites[sid - 1]
        if not site.is_up:
            raise "Site {} has already down.".format(sid)
        site.fail(sid)

        pass

    def recover(self, arguments):
        pass

    def execute_operations(self):
        pass

    def abort(self, tid: str):
        pass

    def commit(self, tid, commit_time):
        pass