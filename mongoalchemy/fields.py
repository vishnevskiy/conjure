from mongoalchemy.queryset import Expression

class Field(object): 
    def __init__(self, name='???', **kwargs): 
        self._name = name

    def __add__(self, other):
        return Expression({self._name: {'$inc': other}})

    def __eq__(self, other):
        return Expression({self._name: other})
    
    def __contains__(self, vals):
        return Expression({self._name: {'$in': vals}})

    def in_(self, *vals):
        return Expression({self._name: {'$in': list(vals)}})

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