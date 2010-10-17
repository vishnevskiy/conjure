from mongoalchemy import spec
import types
import re
import exceptions
import datetime
import bson

class Field(object):
    def __init__(self, verbose_name=None, required=True, default=None, validators=None, choices=None):
        self.name = None
        self.verbose_name = verbose_name
        self.required = required
        self.default = default
        self.validators = validators or []
        self.choices = choices or []

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance._data.get(self.name)

        if value is None:
            value = self.get_default()

        return value

    def __set__(self, instance, value):
        instance._data[self.name] = value

    def has_default(self):
       return self.default is not None

    def get_default(self):
       if self.has_default():
           if callable(self.default):
               return self.default()

           return self.default

       return None

    def to_python(self, value):
        return value

    def to_mongo(self, value):
        return self.to_python(value)

    def validate(self, value):
        pass

    def _validate(self, value):
        if self.choices:
            if value not in self.choices:
                raise exceptions.ValidationError('Value must be one of %s.' % unicode(self.choices))

        for validator in self.validators:
            validator(value)
            
        self.validate(value)

    def __eq__(self, other):
        return self.eq(other)

    def eq(self, other):
        return spec.Equal([self.name, '', other])

    def __ne__(self, other):
        return self.ne(other)

    def ne(self, other):
        return spec.NotEqual([self.name, 'ne', other])

    def __lt__(self, other):
        return self.lt(other)

    def lt(self, other):
        return spec.LessThan([self.name, 'lt', other])

    def __le__(self, other):
        return self.lte(other)

    def lte(self, other):
        return spec.LessThanEqual([self.name, 'lte', other])

    def __gt__(self, other):
        return self.gt(other)

    def gt(self, other):
        return spec.GreaterThan([self.name, 'gt', other])

    def __ge__(self, other):
        return self.gte(other)

    def gte(self, other):
        return spec.GreaterThanEqual([self.name, 'gte', other])

    def in_(self, vals):
        return spec.In([self.name, 'in', vals])

    def nin(self, vals):
        return spec.NotIn([self.name, 'nin', vals])

    def exists(self):
        return spec.Exists([self.name, 'exists', True])

    def type(self, type_):
        return spec.Type([self.name, 'type', type_])

    def where(self, javascript):
        return spec.Where([self.name, 'where', javascript])

    def rename(self, *args, **kwargs):
        raise NotImplementedError()

    def set(self, val):
        self._validate(val)
        return spec.UpdateSpecification(['set', self.name, val])

    def unset(self):
        return spec.UpdateSpecification(['unset', self.name, 1])

class ObjectIdField(Field):
    def to_python(self, value):
        return value

    def to_mongo(self, value):
        if not isinstance(value, bson.objectid.ObjectId):
            try:
                return bson.objectid.ObjectId(unicode(value))
            except Exception, e:
                raise exceptions.ValidationError(unicode(e))

        return value

    def validate(self, value):
        try:
            bson.objectid.ObjectId(unicode(value))
        except bson.objectid.InvalidId:
            raise exceptions.ValidationError('Invalid Object ID')

class StringField(Field):
    def __init__(self, regex=None, min_length=None, max_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.min_length = min_length
        self.max_length = max_length
        Field.__init__(self, **kwargs)

    def to_python(self, value):
        return unicode(value)

    def validate(self, value):
        assert isinstance(value, (str, unicode))

        if self.max_length is not None and len(value) > self.max_length:
            raise exceptions.ValidationError('String value is too long')

        if self.min_length is not None and len(value) < self.min_length:
            raise exceptions.ValidationError('String value is too short')

        if self.regex is not None and self.regex.match(value) is None:
            raise exceptions.ValidationError('String value did not match validation regex')

class EmailField(StringField):
    EMAIL_REGEX = re.compile(
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"'
        r')@(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?$', re.IGNORECASE
    )

    def validate(self, value):
        if not EmailField.EMAIL_REGEX.match(value):
            raise exceptions.ValidationError('Invalid Email: %s' % value)

class IntegerField(Field):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        Field.__init__(self, **kwargs)

    def to_python(self, value):
        return int(value)

    def validate(self, value):
        try:
            value = int(value)
        except:
            raise exceptions.ValidationError('%s could not be converted to int' % value)

        if self.min_value is not None and value < self.min_value:
            raise exceptions.ValidationError('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            raise exceptions.ValidationError('Integer value is too large')

    def __add__(self, val):
        return self.inc(val)

    def inc(self, val=1):
        self._validate(val)
        return spec.UpdateSpecification(['inc', self.name, val])

    def __sub__(self, val):
        return self.dec(val)

    def dec(self, val=1):
        self._validate(val)
        return spec.UpdateSpecification(['inc', self.name, -val])

    def __mod__(self, other):
        class Mod(object):
            def __init__(self, name, a):
                self.name = name
                self.a = a

            def __eq__(self, b):
                return spec.Mod([self.name, 'mod', [self.a, b]])

            eq = __eq__

            def __ne__(self, b):
                return spec.Mod([self.name, 'not mod', [self.a, b]])

            ne = __ne__

        return Mod(self.name, other)

    def mod(self, a, b):
        return spec.Mod([self.name, 'mod', [a, b]])

class FloatField(IntegerField):
    def to_python(self, value):
        return float(value)

    def validate(self, value):
        if isinstance(value, int):
            value = float(value)
            
        assert isinstance(value, float)

        if self.min_value is not None and value < self.min_value:
            raise exceptions.ValidationError('Float value is too small')

        if self.max_value is not None and value > self.max_value:
            raise exceptions.ValidationError('Float value is too large')

class BooleanField(Field):
    def to_python(self, value):
        return bool(value)

    def validate(self, value):
        assert isinstance(value, bool)

class DateTimeField(Field):
    def validate(self, value):
        assert isinstance(value, datetime.datetime)

class DictField(Field):
    def validate(self, value):
        if not isinstance(value, dict):
            raise exceptions.ValidationError('Only dictionaries may be used in a DictField')

        if any(('.' in k or '$' in k) for k in value):
            raise exceptions.ValidationError('Invalid dictionary key name - keys may not '
                                             'contain "." or "$" characters')

class ListField(Field):
    def all(self, vals):
        return spec.All([self.name, 'all', vals])

    def size(self, size):
        return spec.Size([self.name, 'size', size])

    def pop(self):
        return spec.UpdateSpecification(['pop', self.name, 1])

    def popleft(self):
        return spec.UpdateSpecification(['pop', self.name, -1])

    def __getitem__(self, key):
        return self.slice(key)

    def slice(self, key):
        if isinstance(key, slice):
            return spec.Slice([self.name, 'slice', [key.start, key.stop]])

        return spec.Slice([self.name, 'slice', key])

    def __or__(self, val):
        return self.add_to_set(val)

    def add_to_set(self, val):
        return spec.UpdateSpecification(['addToSet', self.name, val])

    def __add__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.push_all(val)
        else:
            return self.push(val)

    def push(self, val):
        return spec.UpdateSpecification(['push', self.name, val])

    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return spec.UpdateSpecification(['pushAll', self.name, val])

    def __sub__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.pull_all(val)
        else:
            return self.pull(val)

    def pull(self, val):
        return spec.UpdateSpecification(['pull', self.name, val])

    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return spec.UpdateSpecification(['pullAll', self.name, val])