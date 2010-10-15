from mongoalchemy import expressions
import types

class Field(object):
    def __init__(self, name='???', **kwargs): 
        self.name = name
        self.parent = None
        self.multi = False
        
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    @property
    def key(self):
        if self.parent and self.multi:
            return self.parent.key + '.$.' + self.name

        return self.name

    # ==
    def __eq__(self, other):
        return expressions.EqualExpression({self.key: other})

    eq = __eq__

    # !=
    def __ne__(self, other):
        return expressions.NotEqualExpression({self.key: {'$ne': other}})

    ne = __ne__

    # <
    def __lt__(self, other):
        return expressions.LessThanExpression({self.key: {'$lt': other}})

    lt = __lt__

    # <=
    def __le__(self, other):
        return expressions.LessThanEqualExpression({self.key: {'$lte': other}})

    lte = __le__

    # >
    def __gt__(self, other):
        return expressions.GreaterThanExpression({self.key: {'$gt': other}})

    gt = __gt__

    # >=
    def __ge__(self, other):
        return expressions.GreaterThanEqualExpression({self.key: {'$gte': other}})

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

        return Mod(self.key, other)

    mod = __mod__

    # in
    def in_(self, vals):
        return expressions.InExpression({self.key: {'$in': vals}})

    # not in
    def nin(self, vals):
        return expressions.NotInExpression({self.key: {'$nin': vals}})

    # all
    def all(self, vals):
        return expressions.AllExpression({self.key: {'$all': vals}})

    # size
    def size(self, size):
        return expressions.SizeExpression({self.key: {'$size': size}})
    
    # exists
    def exists(self):
        return expressions.ExistsExpression({self.key: {'$exists': True}})

    # type
    def type(self, type_):
        return expressions.TypeExpression({self.key: {'$type': type_}})

    # where
    def where(self, javascript):
        return expressions.WhereExpression({self.key: {'$where': javascript}})

    # slice
    def __getitem__(self, key):
        if isinstance(key, slice):
            return expressions.SliceExpression({self.key: {'$slice': [key.start, key.stop]}})
            
        return expressions.SliceExpression({self.key: {'$slice': key}})

    slice = __getitem__

    # pop
    def pop(self):
        return expressions.UpdateExpression({'$pop': {self.key: 1}})

    def popleft(self):
        return expressions.UpdateExpression({'$pop': {self.key: -1}})

    # addToSet
    def __or__(self, val):
        return expressions.UpdateExpression({'$addToSet': {self.key: val}})

    # rename
    def rename(self, *args, **kwargs):
        raise NotImplementedError('$rename not supported')

    # set
    def set(self, val):
        return expressions.UpdateExpression({'$set': {self.key: val}})

    # unset
    def unset(self):
        return expressions.UpdateExpression({'$unset': {self.key: 1}})

    # + (inc/push)
    def __add__(self, val=1):
        if self.multi:
            if type(val) in [types.ListType, types.TupleType]:
                return self.push_all(val)
            else:
                return self.push(val)

        return expressions.UpdateExpression({'$inc': {self.key: val}})

    inc = __add__

    # push
    def push(self, val):
        return expressions.UpdateExpression({'$push': {self.key: val}})

    # pushAll
    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()
        
        return expressions.UpdateExpression({'$pushAll': {self.key: val}})

    # - (pull)
    def __sub__(self, val=1):
        if self.multi:
            if type(val) in [types.ListType, types.TupleType]:
                return self.pull_all(val)
            else:
                return self.pull(val)

        return expressions.UpdateExpression({'$inc': {self.key: -val}})

    # push
    def pull(self, val):
        return expressions.UpdateExpression({'$pull': {self.key: val}})

    # pushAll
    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return expressions.UpdateExpression({'$pullAll': {self.key: val}})

    dec = __sub__

class ObjectIdField(Field):
    pass
    
class CharField(Field):
    pass

class IntegerField(Field):
    pass

class ListField(Field):
    pass

class BooleanField(Field):
    pass