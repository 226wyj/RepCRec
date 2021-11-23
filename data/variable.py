from collections import deque
from value import Value


class Variable:
    def __init__(self, vid: str, init: Value, is_replicated: bool):
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
