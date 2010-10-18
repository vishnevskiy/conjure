from mongoalchemy import query, fields
import copy
import exceptions
import pymongo

_cls_index = {}

class DocumentMetaclass(type):
    def __new__(cls, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(DocumentMetaclass, cls).__new__
        
        if metaclass and issubclass(metaclass, DocumentMetaclass):
            return super_new(cls, name, bases, attrs)

        _fields = {}

        Meta = attrs.pop('Meta', None)

        if Meta and getattr(Meta, 'embedded', False):
            _meta = {
                'embedded': True
            }
        else:
            _meta = {
                'db': 'mongodb://localhost:27017/main',
                'collection': name.lower() + 's',
                'sorting': [],
                'get_latest_by': [],
                'indexes': [],
                'embedded': False
            }

        _meta.update({
            'verbose_name': name.lower(),
            'verbose_name_plural': name.lower() + 's',
         })

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

        attrs['_meta'] = _meta

        for attr_name, attr_value in attrs.iteritems():
            if hasattr(attr_value, '__class__') and issubclass(attr_value.__class__, fields.Field):
                attr_value.name = attr_name
                _fields[attr_name] = attr_value

        attrs['_fields'] = _fields

        new_cls = super_new(cls, name, bases, attrs)

        for field in new_cls._fields.values():
            field.owner = new_cls

        if not _meta['embedded']:
            if '_id' not in _fields:
                raise exceptions.DocumentError('Missing "_id" field on "%s"' % new_cls.__name__)

            new_cls.objects = query.Manager()
            _meta['cls_key'] = '%s/%s:%s' % (_meta['db'], _meta['collection'], name)

            global _cls_index
            _cls_index[_meta['cls_key']] = new_cls

        return new_cls

class BaseDocument(object):
    # START - just so autocomplete works
    objects = query.QuerySet(None, None)
    _fields = {}
    _meta = {}
    # END

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

class Document(BaseDocument):
    __metaclass__ = DocumentMetaclass

    def save(self, safe=True, insert=False):
        self.validate()

        doc = self.to_mongo()

        try:
            collection = self.__class__.objects._collection
   
            if insert:
                object_id = collection.insert(doc, safe=safe)
            else:
                object_id = collection.save(doc, safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise exceptions.OperationError(unicode(err))

        self['_id'] = object_id

    def delete(self, safe=False):
        object_id = self._fields['_id'].to_mongo(self._id)

        try:
            self.__class__.objects.filter_by(_id=object_id).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise exceptions.OperationError(unicode(err))

    def reload(self):
        doc = self.__class__.objects.filter_by(_id=self._id)._one()
        
        for field in self._fields:
            setattr(self, field, self._fields[field].to_python(doc.get(field)))

    @classmethod
    def drop_collection(cls):
        cls.objects._collection.drop()