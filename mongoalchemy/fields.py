from mongoalchemy import expressions
import types

class Field(object):
    """
    TODO: implement the following
    $elemMatch
    The $ positional operator
    """

    def __init__(self, name='???', **kwargs): 
        self._name = name
        
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

    # ==
    def __eq__(self, other):
        return expressions.EqualExpression({self._name: other})

    eq = __eq__

    # !=
    def __ne__(self, other):
        return expressions.NotEqualExpression({self._name: {'$ne': other}})

    ne = __ne__

    # <
    def __lt__(self, other):
        return expressions.LessThanExpression({self._name: {'$lt': other}})

    lt = __lt__

    # <=
    def __le__(self, other):
        return expressions.LessThanEqualExpression({self._name: {'$lte': other}})

    lte = __le__

    # >
    def __gt__(self, other):
        return expressions.GreaterThanExpression({self._name: {'$gt': other}})

    gt = __gt__

    # >=
    def __ge__(self, other):
        return expressions.GreaterThanEqualExpression({self._name: {'$gte': other}})

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

        return Mod(self._name, other)

    mod = __mod__

    # in
    def in_(self, vals):
        return expressions.InExpression({self._name: {'$in': vals}})

    # not in
    def nin(self, vals):
        return expressions.NotInExpression({self._name: {'$nin': vals}})

    # all
    def all(self, vals):
        return expressions.AllExpression({self._name: {'$all': vals}})

    # size
    def size(self, size):
        return expressions.SizeExpression({self._name: {'$size': size}})
    
    # exists
    def exists(self):
        return expressions.ExistsExpression({self._name: {'$exists': True}})

    # type
    def type(self, type_):
        return expressions.TypeExpression({self._name: {'$type': type_}})

    # where
    def where(self, javascript):
        return expressions.WhereExpression({self._name: {'$where': javascript}})

    # slice
    def __getitem__(self, key):
        if isinstance(key, slice):
            return expressions.SliceExpression({self._name: {'$slice': [key.start, key.stop]}})
            
        return expressions.SliceExpression({self._name: {'$slice': key}})

    slice = __getitem__

    # pop
    def pop(self):
        return expressions.Expression({'$pop': {self._name: 1}})

    def popleft(self):
        return expressions.Expression({'$pop': {self._name: -1}})

    # addToSet
    def __or__(self, val):
        return expressions.Expression({'$addToSet': {self._name: val}})

    __ior__ = __or__

    # rename
    def rename(self, *args, **kwargs):
        raise NotImplementedError('$rename not supported')

    # set
    def set(self, val):
        return expressions.Expression({'$set': {self._name: val}})

    # unset
    def unset(self):
        return expressions.Expression({'$unset': {self._name: 1}})

    # + (inc/push)
    def __add__(self, val=1):
        if getattr(self, 'multi', False):
            if type(val) in [types.ListType, types.TupleType]:
                return expressions.Expression({'$pushAll': {self._name: val}})
            else:
                return expressions.Expression({'$push': {self._name: val}})

        return expressions.Expression({'$inc': {self._name: val}})

    inc = __iadd__ = __add__

    # - (pull)
    def __sub__(self, val=1):
        if getattr(self, 'multi', False):
            if type(val) in [types.ListType, types.TupleType]:
                return expressions.Expression({'$pullAll': {self._name: val}})
            else:
                return expressions.Expression({'$pull': {self._name: val}})

        return expressions.Expression({'$inc': {self._name: -val}})

    dec = __isub__ = __sub__

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