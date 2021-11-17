class Transaction:
    def __init__(self, tid: str, t: int, is_ro: bool):
        self.tid = tid
        self.timestamp = t
        self.is_ro = is_ro
        self.is_abort = False
        self.visited_sites = []
