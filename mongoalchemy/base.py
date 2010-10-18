from mongoalchemy import query, exceptions, operations
import copy
import bson

_cls_index = {}

class DocumentMeta(type):
    def __new__(cls, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(DocumentMeta, cls).__new__

        if metaclass and issubclass(metaclass, DocumentMeta):
            return super_new(cls, name, bases, attrs)

        _fields = {}

        _meta = {
            'db': 'mongodb://localhost:27017/main',
            'verbose_name': name.lower(),
            'verbose_name_plural': name.lower() + 's',
            'collection': name.lower() + 's',
            'sorting': [],
            'get_latest_by': [],
            'indexes': [],
            'embedded': False
        }

        for base in bases:
            if hasattr(base, '_fields'):
                _fields.update(copy.deepcopy(base._fields))

            if hasattr(base, '_meta'):
                _meta.update(copy.deepcopy(base._meta))

                if 'parent_field' in _meta:
                    del _meta['parent_field']

        Meta = attrs.pop('Meta', None)

        if Meta:
            for k in _meta:
                if hasattr(Meta, k):
                    _meta[k] = getattr(Meta, k)

        for attr_name, attr_value in attrs.iteritems():
            if hasattr(attr_value, '__class__') and issubclass(attr_value.__class__, BaseField):
                attr_value.name = attr_name
                _fields[attr_name] = attr_value

        if _meta['embedded']:
            _meta = dict([(k, _meta[k]) for k in ['embedded', 'verbose_name', 'verbose_name_plural']])
        elif '_id' not in _fields:
            _id = ObjectIdField()
            _id.name = '_id'
            _fields['_id'] = _id
            attrs['_id'] = _id

        attrs['_meta'] = _meta
        attrs['_fields'] = _fields

        new_cls = super_new(cls, name, bases, attrs)

        for field in new_cls._fields.values():
            field.owner = new_cls

        if not _meta['embedded']:
            new_cls.objects = query.Manager()
            _meta['cls_key'] = '%s/%s:%s' % (_meta['db'], _meta['collection'], name)

            global _cls_index
            _cls_index[_meta['cls_key']] = new_cls

        return new_cls

class BaseDocument(object):
    _fields = {}
    _meta = {}

    def __init__(self, **data):
        self._data = {}

        for attr_name, attr_value in self._fields.iteritems():
            if attr_name in data:
                setattr(self, attr_name, data.pop(attr_name))
            else:
                value = getattr(self, attr_name, None)
                setattr(self, attr_name, value)

    def __iter__(self):
        return iter(self._fields)

    def __getitem__(self, name):
        try:
            if name in self._fields:
                return getattr(self, name)
        except AttributeError:
            pass

        raise KeyError(name)

    def __setitem__(self, name, value):
        if name not in self._fields:
            raise KeyError(name)

        return setattr(self, name, value)

    def __contains__(self, name):
        try:
            value = getattr(self, name)
            return value is not None
        except AttributeError:
            return False

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return u'<%s: %s>' % (self.__class__.__name__, unicode(self))

    def __str__(self):
        try:
            return unicode(self).encode('utf-8')
        except:
            _id = getattr(self, '_id', None)

            if _id:
                return unicode(_id)

            return '%s object' % self.__class__.__name__

    @classmethod
    def to_python(cls, doc):
        if doc is not None:
            if '_cls' in doc:
                if doc['_cls'] != cls.__name__:
                    pass # TODO: implement

                del doc['_cls']

            for name, field in cls._fields.iteritems():
                if name in doc:
                    doc[name] = field.to_python(doc[name])

            doc = cls(**doc)

        return doc

    def to_mongo(self):
        doc = {}

        for field_name, field in self._fields.iteritems():
            value = getattr(self, field_name, None)

            if value is not None:
                doc[field.name] = field.to_mongo(value)

        if not self._meta['embedded']:
            doc['_cls'] = self.__class__.__name__

        return doc

    def validate(self):
        fields = [(field, getattr(self, name)) for name, field in self._fields.iteritems()]

        for field, value in fields:
            if value is not None:
                try:
                    field._validate(value)
                except (ValueError, AttributeError, AssertionError):
                    raise exceptions.ValidationError('Invalid value for field of type "' +
                                                     field.__class__.__name__ + '"')
            elif not field.name == '_id' and field.required:
                raise exceptions.ValidationError('Field "%s" is required' % field.name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if hasattr(self, '_id') and hasattr(other, '_id'):
                return self._id == other._id

            return self._data == other._data

        return False


class BaseField(operations.Common):
    def __init__(self, verbose_name=None, required=False, default=None, validators=None, choices=None):
        self.owner = None
        self.name = None
        self.verbose_name = verbose_name
        self.required = required
        self.default = default
        self.validators = validators or []
        self.choices = choices or []

    def get_key(self, positional=False):
        if isinstance(self.owner, BaseField):
            return self.owner.get_key(positional)
        elif  'parent_field' in self.owner._meta:
            if positional:
                sep = '.$.'
            else:
                sep = '.'

            return self.owner._meta['parent_field'].get_key(positional) + sep + self.name

        return self.name

    def __get__(self, instance, _):
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

class ObjectIdField(BaseField):
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