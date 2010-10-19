class Alias(object):
    def __init__(self, field=None):
        self.field = field

    def __get__(self, instance, _):
        if instance is None:
            return self

        return instance.__get__(self.field)

    def __set__(self, instance, value):
        if instance is None:
            return self

        return instance.__set__(self.field, value)