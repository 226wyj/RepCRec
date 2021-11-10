class Operation:
    def __init__(self, command, transaction_id, variable_id):
        self.command = command
        self.transaction_id = transaction_id
        self.variable_id = variable_id


class ReadOperation(Operation):
    def __init__(self, command, transaction_id, variable_id):
        super(ReadOperation, self).__init__(command, transaction_id, variable_id)


class WriteOperation(Operation):
    def __init__(self, command, transaction_id, variable_id, value):
        super(WriteOperation, self).__init__(command, transaction_id, variable_id)
        self.value = value
