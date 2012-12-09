from .exceptions import InvalidQueryError


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


def lookup_field(document, key):
    parts = key.split('.')

    fields = []
    field = None

    for field_name in parts:
        if field is None:
            field = document._fields[field_name]
        else:
            field = field.lookup_member(field_name)

            if field is None:
                raise InvalidQueryError('Cannot resolve field "%s"' % field_name)

        fields.append(field)

    return fields[-1]
