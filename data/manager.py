from collections import defaultdict


class Variable:
    def __init__(self, vid: str):
        self.vid = vid


class DataManager:
    def __init__(self, sid):
        self.sid = sid
        self.is_up = True
        self.data = defaultdict(str)
        for i in range(1, 21):
            vid = 'x' + str(i)
            if i % 2 == 0:
                # replicated value
                pass
            elif i % 10 + 1 == self.sid:
                # not replicated value
                pass

    def has_variable(self, vid: str) -> bool:
        return False if self.data[vid] == '' else True

    def fail(self, sid):
        pass