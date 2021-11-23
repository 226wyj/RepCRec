class Value:
    def __init__(self, v):
        self.value = v


class TemporaryValue(Value):
    def __init__(self, v, tid):
        super(TemporaryValue, self).__init__(v)
        self.tid = tid


class CommitValue(Value):
    def __init__(self, v, commit_time):
        super(CommitValue, self).__init__(v)
        self.commit_time = commit_time
