class TransactionError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Transaction Error: " + self.message


class DataError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Data Error: " + self.message


class ParseError(Exception):
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return "Parse Error: " + self.message
