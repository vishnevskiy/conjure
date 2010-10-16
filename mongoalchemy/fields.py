from mongoalchemy import expressions
import types

class Field(object):
    def __init__(self, required=False, default=None):
        self.name = None
        self.default = default
        self.required = required

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = instance._data.get(self.name)

        if value is None:
            value = self.default
            
            if callable(value):
                value = value()

        return value

    def __set__(self, instance, value):
        instance._data[self.name] = value

    def to_python(self, value):
        return value

    def to_mongo(self, value):
        return self.to_python(value)

    def validate(self, value):
        pass

    # ==
    def __eq__(self, other):
        return expressions.EqualExpression({self.name: other})

    eq = __eq__

    # !=
    def __ne__(self, other):
        return expressions.NotEqualExpression({self.name: {'$ne': other}})

    ne = __ne__

    # <
    def __lt__(self, other):
        return expressions.LessThanExpression({self.name: {'$lt': other}})

    lt = __lt__

    # <=
    def __le__(self, other):
        return expressions.LessThanEqualExpression({self.name: {'$lte': other}})

    lte = __le__

    # >
    def __gt__(self, other):
        return expressions.GreaterThanExpression({self.name: {'$gt': other}})

    gt = __gt__

    # >=
    def __ge__(self, other):
        return expressions.GreaterThanEqualExpression({self.name: {'$gte': other}})

    gte = __gt__

    # %
    def __mod__(self, other):
        class Mod(object):
            def __init__(self, name, a):
                self.name = name
                self.a = a

            def __eq__(self, b):
                return expressions.ModExpression({self.name: {'$mod': [self.a, b]}})

            eq = __eq__

            def __ne__(self, b):
                return expressions.ModExpression({self.name: {'$not': {'$mod': [self.a, b]}}})

            ne = __ne__

        return Mod(self.name, other)

    mod = __mod__

    # in
    def in_(self, vals):
        return expressions.InExpression({self.name: {'$in': vals}})

    # not in
    def nin(self, vals):
        return expressions.NotInExpression({self.name: {'$nin': vals}})

    # all
    def all(self, vals):
        return expressions.AllExpression({self.name: {'$all': vals}})

    # size
    def size(self, size):
        return expressions.SizeExpression({self.name: {'$size': size}})
    
    # exists
    def exists(self):
        return expressions.ExistsExpression({self.name: {'$exists': True}})

    # type
    def type(self, type_):
        return expressions.TypeExpression({self.name: {'$type': type_}})

    # where
    def where(self, javascript):
        return expressions.WhereExpression({self.name: {'$where': javascript}})

    # slice
    def __getitem__(self, key):
        if isinstance(key, slice):
            return expressions.SliceExpression({self.name: {'$slice': [key.start, key.stop]}})
            
        return expressions.SliceExpression({self.name: {'$slice': key}})

    slice = __getitem__

    # pop
    def pop(self):
        return expressions.UpdateExpression({'$pop': {self.name: 1}})

    def popleft(self):
        return expressions.UpdateExpression({'$pop': {self.name: -1}})

    # addToSet
    def __or__(self, val):
        return expressions.UpdateExpression({'$addToSet': {self.name: val}})

    # rename
    def rename(self, *args, **kwargs):
        raise NotImplementedError('$rename not supported')

    # set
    def set(self, val):
        return expressions.UpdateExpression({'$set': {self.name: val}})
    
    # unset
    def unset(self):
        return expressions.UpdateExpression({'$unset': {self.name: 1}})

    # + (inc/push)
    def __add__(self, val=1):
        return expressions.UpdateExpression({'$inc': {self.name: val}})

    inc = __add__

    # push
    def push(self, val):
        return expressions.UpdateExpression({'$push': {self.name: val}})

    # pushAll
    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()
        
        return expressions.UpdateExpression({'$pushAll': {self.name: val}})

    # - (pull)
    def __sub__(self, val=1):
        return expressions.UpdateExpression({'$inc': {self.name: -val}})

    # push
    def pull(self, val):
        return expressions.UpdateExpression({'$pull': {self.name: val}})

    # pushAll
    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return expressions.UpdateExpression({'$pullAll': {self.name: val}})

    dec = __sub__

class ObjectIdField(Field):
    pass
    
class CharField(Field):
    pass

class IntegerField(Field):
    pass

class ListField(Field):
    # + (inc/push)
    def __add__(self, val=1):
        if type(val) in [types.ListType, types.TupleType]:
            return self.push_all(val)
        else:
            return self.push(val)

    inc = __add__

    # - (pull)
    def __sub__(self, val=1):
        if type(val) in [types.ListType, types.TupleType]:
            return self.pull_all(val)
        else:
            return self.pull(val)

    dec = __sub__

class BooleanField(Field):
    pass