from mongoalchemy import spec
import types

class _Base(object):
    def get_key(self, *args, **kwargs):
        raise NotImplemented()

    def _validate(self, *args, **kwargs):
        raise NotImplemented()

class Common(_Base):
    def __eq__(self, other):
        return self.eq(other)

    def eq(self, other):
        return spec.Equal([self.get_key(), '', other])

    def __ne__(self, other):
        return self.ne(other)

    def ne(self, other):
        return spec.NotEqual([self.get_key(), 'ne', other])

    def __lt__(self, other):
        return self.lt(other)

    def lt(self, other):
        return spec.LessThan([self.get_key(), 'lt', other])

    def __le__(self, other):
        return self.lte(other)

    def lte(self, other):
        return spec.LessThanEqual([self.get_key(), 'lte', other])

    def __gt__(self, other):
        return self.gt(other)

    def gt(self, other):
        return spec.GreaterThan([self.get_key(), 'gt', other])

    def __ge__(self, other):
        return self.gte(other)

    def gte(self, other):
        return spec.GreaterThanEqual([self.get_key(), 'gte', other])

    def in_(self, vals):
        return spec.In([self.get_key(), 'in', vals])

    def nin(self, vals):
        return spec.NotIn([self.get_key(), 'nin', vals])

    def exists(self):
        return spec.Exists([self.get_key(), 'exists', True])

    def type(self, type_):
        return spec.Type([self.get_key(), 'type', type_])

    def where(self, javascript):
        return spec.Where([self.get_key(), 'where', javascript])

    def rename(self):
        raise NotImplementedError()

    def set(self, val):
        self._validate(val)
        return spec.UpdateSpecification(['set', self.get_key(True), val])

    def unset(self):
        return spec.UpdateSpecification(['unset', self.get_key(True), 1])

class Number(_Base):
    def __add__(self, val):
        return self.inc(val)

    def inc(self, val=1):
        self._validate(val)
        return spec.UpdateSpecification(['inc', self.get_key(True), val])

    def __sub__(self, val):
        return self.dec(val)

    def dec(self, val=1):
        self._validate(val)
        return spec.UpdateSpecification(['inc', self.get_key(True), -val])

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

class List(_Base):
    def all(self, vals):
        return spec.All([self.get_key(), 'all', vals])

    def size(self, size):
        return spec.Size([self.get_key(), 'size', size])

    def pop(self):
        return spec.UpdateSpecification(['pop', self.get_key(True), 1])

    def popleft(self):
        return spec.UpdateSpecification(['pop', self.get_key(True), -1])

    def __getitem__(self, key):
        return self.slice(key)

    def slice(self, key):
        if isinstance(key, slice):
            return spec.Slice([self.get_key(), 'slice', [key.start, key.stop]])

        return spec.Slice([self.get_key(), 'slice', key])

    def __or__(self, val):
        return self.add_to_set(val)

    def add_to_set(self, val):
        return spec.UpdateSpecification(['addToSet', self.get_key(True), val])

    def __add__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.push_all(val)
        else:
            return self.push(val)

    def push(self, val):
        return spec.UpdateSpecification(['push', self.get_key(True), val])

    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return spec.UpdateSpecification(['pushAll', self.get_key(True), val])

    def __sub__(self, val):
        if type(val) in [types.ListType, types.TupleType]:
            return self.pull_all(val)
        else:
            return self.pull(val)

    def pull(self, val):
        if isinstance(val, spec.QuerySpecification):
            val = val.compile(self.get_key(True))

        return spec.UpdateSpecification(['pull', self.get_key(True), val])

    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return spec.UpdateSpecification(['pullAll', self.get_key(True), val])