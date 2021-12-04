from collections import deque
from data.value import Value


class Variable:
    def __init__(self, vid: str, init: Value, is_replicated: bool):
        self.vid = vid
        self.commit_value_list = []
        self.init_value = init
        self.temporary_value = None
        self.is_replicated = is_replicated
        self.is_readable = True
        # initialize commit_value_list with the init_value
        self.commit_value_list.append(self.init_value)

    def get_last_commit_value(self):
        return self.commit_value_list[-1]

    def add_commit_value(self, v):
        self.commit_value_list.append(v)

    def get_temporary_value(self):
        if not self.temporary_value:
            raise "No temporary value."
        else:
            return self.temporary_value.value
