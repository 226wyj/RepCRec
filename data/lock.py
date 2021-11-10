from enum import Enum


class LockType(Enum):
    R = 0,
    W = 1


class Lock:
    def __init__(self, variable_id, lock_type):
        self.variable_id = variable_id
        self.lock_type = lock_type


class ReadLock(Lock):
    def __init__(self, variable_id, transaction_id):
        super(ReadLock, self).__init__(variable_id, LockType.R)
        self.transaction_id_set = {transaction_id}


class WriteLock(Lock):
    def __init__(self, variable_id, transaction_id):
        super(WriteLock, self).__init__(variable_id, LockType.W)
        self.transaction_id = transaction_id
