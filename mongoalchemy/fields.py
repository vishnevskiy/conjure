from mongoalchemy import expressions

class Field(object):
    """
    TODO: implement the following
    $elementMatch
    $slice
    $inc
    $set
    $unset
    $push
    $pushAll
    $addToSet
    $pop
    $pull
    $pullAll
    $rename
    The $ positional operator
    """

    def __init__(self, name='???', **kwargs): 
        self._name = name

    # ==
    def __eq__(self, other):
        return expressions.EqualExpression({self._name: other})

    # !=
    def __ne__(self, other):
        return expressions.NotEqualExpression({self._name: {'$ne': other}})

    # <
    def __lt__(self, other):
        return expressions.LessThanExpression({self._name: {'$lt': other}})

    # <=
    def __le__(self, other):
        return expressions.LessThanEqualExpression({self._name: {'$lte': other}})

    # >
    def __gt__(self, other):
        return expressions.GreaterThanExpression({self._name: {'$gt': other}})

    # >=
    def __ge__(self, other):
        return expressions.GreaterThanEqualExpression({self._name: {'$gte': other}})

    # %
    def __mod__(self, other):
        class Mod(object):
            def __init__(self, name, a):
                self.name = name
                self.a = a

            def __eq__(self, b):
                return expressions.Expression({self.name: {'$mod': [self.a, b]}})

            def __ne__(self, b):
                return expressions.Expression({self.name: {'$not': {'$mod': [self.a, b]}}})

        return Mod(self._name, other)

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