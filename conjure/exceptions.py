__all__ = ['DocumentError', 'ConnectionError', 'ValidationError', 'OperationError',
           'DoesNotExist', 'InvalidQueryError']


class DocumentError(Exception):
    pass


class ConnectionError(DocumentError):
    pass


class ValidationError(DocumentError):
    pass


class OperationError(DocumentError):
    pass


class DoesNotExist(DocumentError):
    pass


class InvalidQueryError(Exception):
    pass
