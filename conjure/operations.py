from .spec import Equal, NotEqual, LessThan, LessThanEqual, GreaterThan, GreaterThanEqual, In, NotIn, \
    Exists, Type, Where, UpdateSpecification, Mod, All, Size, Slice, QuerySpecification, Match
import types
import re

__all__ = ['Common', 'String', 'Number', 'List', 'Reference']


class _Base(object):
    def get_key(self, *args, **kwargs):
        raise NotImplemented

    def _validate(self, *args, **kwargs):
        raise NotImplemented

    def to_mongo(self, value):
        raise NotImplemented


class Common(_Base):
    def __eq__(self, other):
        return self.eq(other)

    def eq(self, other):
        return Equal([self.get_key(), '', other])

    def __ne__(self, other):
        return self.ne(other)

    def ne(self, other):
        return NotEqual([self.get_key(), 'ne', other])

    def __lt__(self, other):
        return self.lt(other)

    def lt(self, other):
        return LessThan([self.get_key(), 'lt', other])

    def __le__(self, other):
        return self.lte(other)

    def lte(self, other):
        return LessThanEqual([self.get_key(), 'lte', other])

    def __gt__(self, other):
        return self.gt(other)

    def gt(self, other):
        return GreaterThan([self.get_key(), 'gt', other])

    def __ge__(self, other):
        return self.gte(other)

    def gte(self, other):
        return GreaterThanEqual([self.get_key(), 'gte', other])

    def in_(self, vals):
        return In([self.get_key(), 'in', vals])

    def nin(self, vals):
        return NotIn([self.get_key(), 'nin', vals])

    def exists(self):
        return Exists([self.get_key(), 'exists', True])

    def type(self, type_):
        return Type([self.get_key(), 'type', type_])

    def where(self, javascript):
        return Where([self.get_key(), 'where', javascript])

    def rename(self):
        raise NotImplementedError()

    def set(self, val):
        self._validate(val)
        return UpdateSpecification(['set', self.get_key(True), self.to_mongo(val)])

    def unset(self):
        return UpdateSpecification(['unset', self.get_key(True), 1])


class String(_Base):
    def startswith(self, value):
        return self.re(r'^%s' % value)

    def istartswith(self, value):
        return self.ire(r'^%s' % value)

    def endswith(self, value):
        return self.re(r'%s$' % value)

    def iendswith(self, value):
        return self.ire(r'%s$' % value)

    def contains(self, value):
        return self.re(r'%s' % value)

    def icontains(self, value):
        return self.ire(r'%s' % value)

    def re(self, pattern):
        return Equal([self.get_key(), '', re.compile(pattern)])

    def ire(self, pattern):
        return Equal([self.get_key(), '', re.compile(pattern, re.IGNORECASE)])


class Number(_Base):
    def __add__(self, val):
        return self.inc(val)

    def inc(self, val=1):
        self._validate(val)
        return UpdateSpecification(['inc', self.get_key(True), val])

    def __sub__(self, val):
        return self.dec(val)

    def dec(self, val=1):
        self._validate(val)
        return UpdateSpecification(['inc', self.get_key(True), -val])

    def __mod__(self, other):
        class Proxy(object):
            def __init__(self, field, a):
                self.field = field
                self.a = a

            def __eq__(self, b):
                return Mod([self.field.get_key(False), 'mod', [self.a, b]])

            eq = __eq__

            def __ne__(self, b):
                return Mod([self.field.get_key(False), 'not mod', [self.a, b]])

            ne = __ne__

        return Proxy(self, other)

    def mod(self, a, b):
        return Mod([self.name, 'mod', [a, b]])


class List(_Base):
    def all(self, vals):
        return All([self.get_key(), 'all', vals])

    def size(self, size):
        return Size([self.get_key(), 'size', size])

    def match(self, *specs):
        return Match([self.get_key(), 'elemMatch', specs])

    def pop(self):
        return UpdateSpecification(['pop', self.get_key(True), 1])

    def popleft(self):
        return UpdateSpecification(['pop', self.get_key(True), -1])

    def __getitem__(self, key):
        return self.slice(key)

    def slice(self, key):
        if isinstance(key, slice):
            return Slice([self.get_key(), 'slice', [key.start, key.stop]])

        return Slice([self.get_key(), 'slice', key])

    def __or__(self, val):
        return self.add_to_set(val)

    def add_to_set(self, val):
        self.field._validate(val)
        return UpdateSpecification(['addToSet', self.get_key(True), self.field.to_mongo(val)])

    def __add__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.push_all(val)
        else:
            return self.push(val)

    def push(self, val):
        self.field._validate(val)
        return UpdateSpecification(['push', self.get_key(True), self.field.to_mongo(val)])

    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        map(self.field._validate, val)
        return UpdateSpecification(['pushAll', self.get_key(True), map(self.field.to_mongo, val)])

    def __sub__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.pull_all(val)
        else:
            return self.pull(val)

    def pull(self, val):
        if isinstance(val, QuerySpecification):
            val = val.compile(self.get_key(True))

        return UpdateSpecification(['pull', self.get_key(True), val])

    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return UpdateSpecification(['pullAll', self.get_key(True), val])

    def replace_with(self, val):
        self.field._validate(val)
        return UpdateSpecification(['set', self.get_key(True) + '.$', self.field.to_mongo(val)])

    def __mod__(self, val):
        return self.replace_with(val)


class Reference(Common):
    def eq(self, other):
        return Common.eq(self, self.to_mongo(other))

    def ne(self, other):
        return Common.ne(self, self.to_mongo(other))

    def lt(self, other):
        return Common.lt(self, self.to_mongo(other))

    def lte(self, other):
        return Common.lte(self, self.to_mongo(other))

    def gt(self, other):
        return Common.gt(self, self.to_mongo(other))

    def gte(self, other):
        return Common.gte(self, self.to_mongo(other))

    def in_(self, vals):
        vals = [self.to_mongo(val) for val in vals]
        return Common.in_(self, vals)

    def nin(self, vals):
        vals = [self.to_mongo(val) for val in vals]
        return Common.nin(self, vals)

    def set(self, val):
        self._validate(val)
        return Common.set(self, self.to_mongo(val))
