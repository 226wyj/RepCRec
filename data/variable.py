from collections import deque

from data.value import Value
from errors import DataError


class Variable:
    def __init__(self, vid: str, init: Value, is_replicated: bool):
        self.vid = vid
        self.commit_value_list = deque([init])
        self.temporary_value = None
        self.is_replicated = is_replicated
        self.is_readable = True

    def get_last_commit_value(self):
        res = self.commit_value_list.popleft()
        self.commit_value_list.appendleft(res)
        return res.value

    def add_commit_value(self, v):
        self.commit_value_list.appendleft(v)

    def get_temporary_value(self):
        if not self.temporary_value:
            raise DataError("Variable {} has no temporary value.".format(self.vid))
        else:
            return self.temporary_value.value
