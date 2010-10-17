from mongoalchemy import spec
import types

class NOT_PROVIDED:
    pass

class Field(object):
    def __init__(self, verbose_name=None, name=None, blank=False, null=None, default=NOT_PROVIDED, editable=True,
                 help_text='', validators=None, choices=None):
        
        self.name = name
        self.verbose_name = verbose_name
        self.blank = blank
        self.null = null
        self.default = default
        self.editable = editable
        self.help_text = help_text
        self.validators = validators or []
        self.choices = choices or []

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

    def has_default(self):
       return self.default is not NOT_PROVIDED

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
        return spec.UpdateSpecification(['set', self.name, val])

    def unset(self):
        return spec.UpdateSpecification(['unset', self.name, 1])

class ObjectIdField(Field):
    pass

class CharField(Field):
    pass

class IntegerField(Field):
    def __add__(self, val):
        return self.inc(val)

    def inc(self, val=1):
        return spec.UpdateSpecification(['inc', self.name, val])

    def __sub__(self, val):
        return self.dec(val)

    def dec(self, val=1):
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

class BooleanField(Field):
    pass