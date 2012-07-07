from .operations import Common
from .exceptions import ValidationError
from .query import Manager
from operator import itemgetter
import copy
import bson

_documents = []

class DocumentMeta(type):
    def __new__(cls, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(DocumentMeta, cls).__new__

        if metaclass and issubclass(metaclass, DocumentMeta):
            return super_new(cls, name, bases, attrs)

        _fields = {}
        _name = [name]
        _superclasses = {}

        _meta = {
            'db': 'mongodb://localhost:27017/_conjure',
            'verbose_name': name.lower(),
            'verbose_name_plural': name.lower() + 's',
            'collection': name.lower() + 's',
            'indexes': [],
            'embedded': False
        }

        for base in bases:
            if hasattr(base, '_fields'):
                _fields.update(copy.deepcopy(base._fields))

            if hasattr(base, '_name') and hasattr(base, '_superclasses'):
                _name.append(base._name)
                _superclasses[base._name] = base
                _superclasses.update(base._superclasses)

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

                if not _meta['embedded'] and attr_name == 'id':
                    attr_value.db_field = '_id'
                elif not attr_value.db_field:
                    attr_value.db_field = attr_name

                _fields[attr_name] = attr_value

        if _meta['embedded']:
            _meta = dict([(k, _meta[k]) for k in ['embedded', 'verbose_name', 'verbose_name_plural']])
        else:
            if 'id' not in _fields:
                id_field = ObjectIdField(db_field='_id')
                id_field.name = 'id'
                _fields['id'] = id_field
                attrs['id'] = id_field
            elif not isinstance(_fields['id'], ObjectIdField):
                _fields['id'].required = True

        attrs['_name'] = '.'.join(reversed(_name))
        attrs['_superclasses'] = _superclasses
        attrs['_meta'] = _meta
        attrs['_fields'] = _fields

        new_cls = super_new(cls, name, bases, attrs)

        for field in new_cls._fields.values():
            field.owner = new_cls
            field.add_to_document(new_cls)

        if not _meta['embedded']:
            global _documents
            _documents.append(new_cls)
            new_cls.objects = Manager()

        return new_cls

class BaseDocument(object):
#    _fields = {}
#    _meta = {}

    def __init__(self, **data):
        self._data = {}
        self._search_index = None

        for attr_name, attr_value in data.iteritems():
            try:
                setattr(self, attr_name, attr_value)
            except AttributeError:
                pass
#                value = getattr(self, attr_name, None)
#                setattr(self, attr_name, value)

    @classmethod
    def _get_subclasses(cls):
        try:
            subclasses = cls.__subclasses__()
        except:
            subclasses = cls.__subclasses__(cls)

        all_subclasses = {}

        for subclass in subclasses:
            all_subclasses[subclass._name] = subclass
            all_subclasses.update(subclass._get_subclasses())

        return all_subclasses

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
        return len(self._fields)

    def __repr__(self):
        return u'<%s: %s>' % (self.__class__.__name__, unicode(self))

    def __str__(self):
        try:
            return unicode(self).encode('utf-8')
        except:
            doc_id = getattr(self, 'id', None)

            if doc_id:
                return unicode(doc_id)

            return '%s object' % self.__class__.__name__

    @classmethod
    def to_python(cls, data):
        if data is not None:
            if '_cls' in data:
                cls_name = data['_cls']

                if cls_name != cls._name:
                    subclasses = cls._get_subclasses()

                    if cls_name not in subclasses:
                        return None

                    cls = subclasses[cls_name]

            doc = cls()

            for field in cls._fields.itervalues():
                if field.db_field in data:
                    doc._data[field.name] = field.to_python(data[field.db_field])

            return doc

        return data

    def to_mongo(self):
        doc = {}

        for field_name, field in self._fields.iteritems():
            value = self._data.get(field_name, None)

            if value is not None:
                doc[field.db_field] = field.to_mongo(value)

        if self._superclasses:
            doc['_cls'] = self._name

        return doc

    def to_json(self, only=None, methods=None):
        j = {}

        only = only or self._fields.keys()

        for field_name in only:
            field = self._fields[field_name]

            if not field.serialize:
                continue

            value = field.to_json(getattr(self, field_name))

            if value is not None:
                j[field_name] = value

        if methods:
            for method in methods:
                j[method] = getattr(self, method)()

        return j

    def validate(self):
        fields = [(field, getattr(self, name)) for name, field in self._fields.iteritems()]

        for field, value in fields:
            if value is not None:
                try:
                    field._validate(value)
                except (ValueError, AttributeError, AssertionError):
                    raise ValidationError('Invalid value for field of type "' +
                                                     field.__class__.__name__ + '"')
            elif not (isinstance(field, ObjectIdField) and field.name == 'id') and field.required:
                raise ValidationError('Field "%s" is required' % field.name)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if hasattr(self, 'id') and hasattr(other, 'id'):
                return self.id == other.id

            return self._data == other._data

        return False


class BaseField(Common):
    def __init__(self, verbose_name=None, db_field=None, required=False, default=None, validators=None, choices=None,
                 editable=True, help_text='', serialize=True):
        
        self.owner = None
        self.name = None
        self.verbose_name = verbose_name
        self.db_field = db_field
        self.required = required
        self.default = default
        self.validators = validators or []
        self.choices = choices or []
        self.editable = editable
        self.help_text = help_text
        self.serialize = serialize

    def get_key(self, positional=False):
        if isinstance(self.owner, BaseField):
            return self.owner.get_key(positional)
        elif  'parent_field' in self.owner._meta:
            parent_field = self.owner._meta['parent_field']

            if positional and parent_field.owner.__class__.__name__ == 'ListField':
                sep = '.$.'
            else:
                sep = '.'

            return parent_field.get_key(positional) + sep + self.db_field

        return self.db_field

    def __get__(self, instance, _):
        if instance is None:
            return self

        value = instance._data.get(self.name)

        if value is None:
            value = self.get_default()
            instance._data[self.name] = value

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

    def to_json(self, value):
        return self.to_python(value)

    def validate(self, value):
        pass

    def _validate(self, value):
        if self.choices:
            if value not in map(itemgetter(0), self.choices):
                raise ValidationError('Value must be one of %s.' % unicode(self.choices))

        for validator in self.validators:
            validator(value)

        self.validate(value)

    def add_to_document(self, cls):
        if self.choices:
            name = self.name
            field = self

            def proxy(self):
                value = self._data.get(name)

                for choice in field.choices:
                    if choice[0] == value:
                        return choice[1]

                return None


            setattr(cls, 'get_%s_display' % name, proxy)

    def lookup_member(self, name):
        return None

class ObjectIdField(BaseField):
    def to_python(self, value):
        return value

    def to_mongo(self, value):
        if value is not None and not isinstance(value, bson.objectid.ObjectId):
            try:
                return bson.objectid.ObjectId(unicode(value))
            except Exception, e:
                raise ValidationError(unicode(e))

        return value

    def to_json(self, value):
        if isinstance(value, bson.objectid.ObjectId):
            return str(value)

    def validate(self, value):
        try:
            bson.objectid.ObjectId(unicode(value))
        except bson.objectid.InvalidId:
            raise ValidationError('Invalid Object ID')