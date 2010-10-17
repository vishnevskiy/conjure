from mongoalchemy import spec, query, fields
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

        Meta = attrs.pop('Meta', None)

        if Meta:
            for k in _meta:
                if hasattr(Meta, k):
                    _meta[k] = getattr(Meta, k)

        attrs['_meta'] = _meta

        for attr_name, attr_value in attrs.items():
            if hasattr(attr_value, '__class__') and issubclass(attr_value.__class__, fields.Field):
                attr_value.name = attr_name
                _fields[attr_name] = attr_value

        attrs['_fields'] = _fields

        new_cls = super_new(cls, name, bases, attrs)

        for field in new_cls._fields.values():
            field.parent = new_cls

        if not _meta['embedded']:
            new_cls.objects = query.Manager()
            _meta['cls_key'] = '%s/%s:%s' % (_meta['db'], _meta['collection'], name)

            global _cls_index
            _cls_index[_meta['cls_key']] = new_cls

        return new_cls

class BaseDocument(object):
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

    def to_mongo(self):
        doc = {}

        for field_name, field in self._fields.iteritems():
            value = getattr(self, field_name, None)

            if value is not None:
                doc[field.name] = field.to_mongo(value)

        if not self._meta['embedded']:
            doc['_cls'] = self.__class__.__name__

        return doc

    @classmethod
    def from_mongo(cls, doc):
        if '_cls' in doc:
            if doc['_cls'] != cls.__name__:
                pass # TODO: implement

            del doc['_cls']

        doc = cls(**doc)

        return doc

    def __eq__(self, other):
        pass

class Document(BaseDocument):
    __metaclass__ = DocumentMetaclass

    def save(self, safe=True, insert=False):
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
        doc = self.__class__.objects.filter_by(_id=self._id).one()
        
        for field in self._fields:
            setattr(self, field, doc[field])
