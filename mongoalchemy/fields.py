from mongoalchemy import statements
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

    # ==
    def __eq__(self, other):
        return statements.Equal([self.name, '', other])

    eq = __eq__

    # !=
    def __ne__(self, other):
        return statements.NotEqual([self.name, 'ne', other])

    ne = __ne__

    # <
    def __lt__(self, other):
        return statements.LessThan([self.name, 'lt', other])

    lt = __lt__

    # <=
    def __le__(self, other):
        return statements.LessThanEqual([self.name, 'lte', other])

    lte = __le__

    # >
    def __gt__(self, other):
        return statements.GreaterThan([self.name, 'gt', other])

    gt = __gt__

    # >=
    def __ge__(self, other):
        return statements.GreaterThanEqual([self.name, 'gte', other])

    gte = __gt__

    # in
    def in_(self, vals):
        return statements.In([self.name, 'in', vals])

    # not in
    def nin(self, vals):
        return statements.NotIn([self.name, 'nin', vals])

    # exists
    def exists(self):
        return statements.Exists([self.name, 'exists', True])

    # type
    def type(self, type_):
        return statements.Type([self.name, 'type', type_])

    # where
    def where(self, javascript):
        return statements.Where([self.name, 'where', javascript])

    # rename
    def rename(self, *args, **kwargs):
        raise NotImplementedError('$rename not supported')

    # set
    def set(self, val):
        return statements.UpdateStatement(['set', self.name, val])

    # unset
    def unset(self):
        return statements.UpdateStatement(['unset', self.name, 1])

class ObjectIdField(Field):
    pass

class CharField(Field):
    pass

class IntegerField(Field):
    # inc +
    def __add__(self, val=1):
        return statements.UpdateStatement(['inc', self.name, val])

    inc = __add__

    def __sub__(self, val=1):
        return statements.UpdateStatement(['inc', self.name, -val])

    dec = __sub__

    # mod %
    def __mod__(self, other):
        class Mod(object):
            def __init__(self, name, a):
                self.name = name
                self.a = a

            def __eq__(self, b):
                return statements.Mod([self.name, 'mod', [self.a, b]])

            eq = __eq__

            def __ne__(self, b):
                return statements.Mod([self.name, 'not mod', [self.a, b]])

            ne = __ne__

        return Mod(self.name, other)

    mod = __mod__

class ListField(Field):
    # all
    def all(self, vals):
        return statements.All([self.name, 'all', vals])

    # size
    def size(self, size):
        return statements.Size([self.name, 'size', size])

    # slice
    def __getitem__(self, key):
        if isinstance(key, slice):
            return statements.Slice([self.name, 'slice', [key.start, key.stop]])

        return statements.Slice([self.name, 'slice', key])

    slice = __getitem__

    # pop
    def pop(self):
        return statements.UpdateStatement(['pop', self.name, 1])

    def popleft(self):
        return statements.UpdateStatement(['pop', self.name, -1])

    # addToSet
    def __or__(self, val):
        return self.add_to_set(val)

    def add_to_set(self, val):
        return statements.UpdateStatement(['addToSet', self.name, val])

    # push
    def __add__(self, val=1):
        if type(val) in [types.ListType, types.TupleType]:
            return self.push_all(val)
        else:
            return self.push(val)

    inc = __add__

    def push(self, val):
        return statements.UpdateStatement(['push', self.name, val])

    # pushAll
    def push_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return statements.UpdateStatement(['pushAll', self.name, val])

    # pull
    def __sub__(self, val=1):
        if type(val) in [types.ListType, types.TupleType]:
            return self.pull_all(val)
        else:
            return self.pull(val)

    dec = __sub__

    def pull(self, val):
        return statements.UpdateStatement(['pull', self.name, val])

    # pullAll
    def pull_all(self, val):
        if type(val) not in [types.ListType, types.TupleType]:
            raise TypeError()

        return statements.UpdateStatement(['pullAll', self.name, val])

class BooleanField(Field):
    pass