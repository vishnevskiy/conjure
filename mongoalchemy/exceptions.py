class DocumentError(Exception):
    pass

class ConnectionError(Exception):
    pass

class ValidationError(Exception):
    pass

class OperationError(Exception):
    pass

class DoesNotExist(Exception):
    pass