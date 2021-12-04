from enum import Enum


class OperationType(Enum):
    R = 0,
    W = 1


class Operation:
    def __init__(self, operation_type: OperationType, tid: str, vid: str):
        self.operation_type = operation_type
        self.tid = tid
        self.vid = vid


class ReadOperation(Operation):
    def __init__(self, tid: str, vid: str):
        super(ReadOperation, self).__init__(OperationType.R, tid, vid)


class WriteOperation(Operation):
    def __init__(self, tid: str, vid: str, value):
        super(WriteOperation, self).__init__(OperationType.W, tid, vid)
        self.value = value
