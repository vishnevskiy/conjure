import copy
import types
import pymongo
import collections
import bson

class ObjectId(bson.objectid.ObjectId):
    def __init__(self, id=None):
        pymongo.objectid.ObjectId.__init__(self, str(id))

    @classmethod
    def convert(cls, id):
        try:
            return cls(id)
        except pymongo.errors.InvalidId:
            return id

class DocumentNotFound(Exception):
    def __init__(self, collection_name, query, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

        self.collection_name = collection_name
        self.query = str(query)

    def __str__(self):
        return 'Document matching %s does not exist in %s.' % (self.query, self.collection_name)

class CollectionManager(object):
    """This class redefines some methods from pymongo.Collection to wrap results with user's class."""
    def __init__(self, db, name, required, auto, cursor_class, document_class):
        self.db = db
        self._collection_name = name
        self._required = required
        self._auto = auto
        self._cursor_class = cursor_class
        self._document_class = document_class

    @property
    def collection(self):
        return self.db[self._collection_name]

    @property
    def db_name(self):
        return self.db.name

    @property
    def collection_name(self):
        return self._collection_name

    def remove(self, *args, **kwargs):
        self.collection.remove(*args, **kwargs)

    def all(self):
        return self._cursor_class(self.collection.find())

    def find(self, *args, **kwargs):
        return self._cursor_class(self.collection.find(*args, **kwargs))

    def find_one(self, *args, **kwargs):
        data = self.collection.find_one(*args, **kwargs)

        return data and self._document_class(__document__=data) or None

    def group(self, *args, **kwargs):
        return self.collection.group(*args, **kwargs)

    def hint(self, *args, **kwargs):
        return self._cursor_class(self.collection.hint(*args, **kwargs))

    def get(self, id, extra={}, *args, **kwargs):
        try:
            id = pymongo.objectid.ObjectId(str(id))
        except pymongo.errors.InvalidId:
            pass

        query = {'_id': id}
        query.update(extra)

        document = self.find_one(query, *args, **kwargs)

        if document: return document

        raise DocumentNotFound(collection_name=self.collection_name, query=query)

    def map_reduce(self, *args, **kwargs):
        return self.collection.map_reduce(*args, **kwargs)

    def save(self, obj):
        if self._required:
            for key, default in self._required.iteritems():
                if key not in obj:
                    if type(default) is types.BuiltinFunctionType:
                        default = default()
                    elif type(default) is types.FunctionType:
                        default = default()

                    obj[key] = default

        if self._auto:
            for key, func in self._auto.iteritems():
                obj[key] = func(obj)

        return self.collection.save(obj)

    def update(self, *args, **kwargs):
        return self.collection.update(*args, **kwargs)

    def eval(self, code, query={}, scope={}):
        scope.update({
            'collection': self._collection_name,
            'query': query
        })

        return self.db.eval(bson.code.Code(code, scope=scope))

    def __getattr__(self, name):
        return getattr(self.collection, name)

    def clone(self, document_class):
        class cursor_class(CursorProxy):
            _document_class = document_class

        return self.__class__(self.db, self._collection_name, self._required, self._auto, cursor_class, document_class)

class CursorProxy(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def next(self):
        """Wraps result into the custom class"""
        return self._document_class(__document__=self._cursor.next())

    def sort(self, *args, **kwargs):
        self._cursor.sort(*args, **kwargs)

        return self

    def first(self):
        try:
            return self[0]
        except IndexError:
            return None

    def all(self):
        return [document for document in self]

    def skip(self, *args, **kwargs):
        self._cursor.skip(*args, **kwargs)

        return self

    def limit(self, *args, **kwargs):
        self._cursor.limit(*args, **kwargs)
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __getitem__(self, index):
        """Wraps result into the custom class if it is one item"""
        result = self._cursor.__getitem__(index)

        return isinstance(index, slice) and self or self._document_class(__document__=result)

    def __len__(self):
        return self._cursor.count(with_limit_and_skip=True)

    def __iter__(self):
        return self

class DocumentBase(type):
    """Metaclass to create classes which inherit the Document class."""

    def __new__(cls, name, bases, attrs):
        if not [base for base in bases if isinstance(base, DocumentBase)]:
            """If this isn't a subclass of Model, don't do anything special."""
            return type.__new__(cls, name, bases, attrs)

        document_class = type.__new__(cls, name, bases, {'__module__': attrs.pop('__module__')})

        extend = attrs.pop('__extend__', None)

        if extend:
            for k, v in extend.iteritems():
                setattr(document_class, k, v)

                class cursor_class(CursorProxy):
                    _document_class = document_class

                setattr(document_class, 'documents', document_class.documents.clone(document_class))

            return document_class

        db = attrs.pop('db')
        collection = attrs.pop('collection')
        required = attrs.get('required', None)
        auto = attrs.get('auto', None)

        setattr(document_class, 'coerce', attrs.pop('coerce', ObjectId))

        for index in attrs.get('indexes', []):
            if type(index) is types.StringType or type(index) is types.ListType:
                db[collection].ensure_index(index)
            elif type(index) is types.TupleType:
                db[collection].ensure_index(*index)
            elif type(index) is types.DictionaryType:
                db[collection].ensure_index(**index)

        class cursor_class(CursorProxy): #@DuplicatedSignature
            _document_class = document_class

        """Set all necessary methods."""
        for key, value in attrs.iteritems():
            if isinstance(value, Lazyload):
                setattr(document_class, key, property(value))
            else:
                setattr(document_class, key, value)

        setattr(document_class, 'documents', CollectionManager(db, collection, required, auto, cursor_class, document_class))

        return document_class

class Document(object):
    __metaclass__ = DocumentBase

    def __init__(self, **kwargs):
        try:
            self.__dict__['__document__'] = kwargs['__document__']
        except KeyError:
            self.__dict__['__document__'] = kwargs

    def __getattr__(self, name):
        return _wrap(self.__dict__['__document__'].get(name, None))

    def __setattr__(self, name, value):
        self.__dict__['__document__'][name] = value

    def __getitem__(self, name):
        return self.__dict__['__document__'][name]

    def __setitem__(self, name, value):
        self.__dict__['__document__'][name] = value

    def __delattr__(self, name):
        self.__dict__['__document__'].__delitem__(name)

    def get(self, name, default):
        return self.__dict__['__document__'].get(name, default)

    @property
    def id(self):
        return str(self._id)

    def save(self):
        self.documents.save(self.__dict__['__document__'])
        return self

    def remove(self):
        self.documents.remove(self.__dict__['__document__'])
        return self

    def update(self, data):
        self.__dict__['__document__'].update(data)

    def reduce(self, *keys):
        reduced = {}

        for key in keys:
            reduced[key] = self.__dict__['__document__'].get(key, None)

        self.__dict__['__document__'] = reduced

        return self

    def reload(self):
        self.__dict__['__document__'] = self.documents.get(self.id).__dict__['__document__']

        return self

    def clone(self):
        clone = self.__class__()
        clone.__dict__['__document__'] = copy.copy(self.__dict__['__document__'])
        return clone

    def __getinitargs__(self):
        return ()

    def __getnewargs__(self):
        return ()

    def __getstate__(self):
        return self.__dict__['__document__']

    def __setstate__(self, document):
        self.__dict__['__document__'] = document

def _wrap(value):
    if isinstance(value, dict):
        return MongoDict(value)
    elif isinstance(value, list):
        return MongoList(value)

    return value

class MongoDict(object):
    def __init__(self, d):
        self.__dict__['__data__'] = d

    def __getattr__(self, name):
        return _wrap(self.__data__.get(name, None))

    def __setattr__(self, name, value):
        self.__data__[name] = value

    def __delattr__(self, name):
        self.__data__.__delitem__(name)

    def __getitem__(self, name):
        return _wrap(self.__data__[name])

    def __setitem__(self, name, value):
        self.__data__[name] = value

    def __delitem__(self, name):
        self.__data__.__delitem__(name)

    def __eq__(self, d):
        if isinstance(d, MongoDict):
            return self.__data__ == d._data
        else:
            return self.__data__ == d

    def __ne__(self, d):
        return not self.__eq__(d)

    def __iter__(self):
        return self.__data__.__iter__()

    def __contains__(self, name):
        return name in self.__data__

    def __len__(self):
        return self.__data__.__len__()

    def get(self, name, default=None):
        return self.__data__.get(name, default)

    def keys(self):
        return self.__data__.keys()

    def values(self):
        return self.__data__.values()

    def copy(self):
        return self.__data__.copy()

    def has_key(self, key):
        return self.__data__.has_key(key)

    def iteritems(self):
        for key, value in self.__data__.iteritems():
            yield (key, _wrap(value))

    def itervalues(self):
        for value in self.__data__.itervalues():
            yield _wrap(value)

    def to_dict(self):
        return self.__data__

class MongoList(object):
    def __init__(self, l):
        self.__data__ = l

    def __getitem__(self, index):
        return _wrap(self.__data__[index])

    def __setitem__(self, index, value):
        self.__data__[index] = value

    def __eq__(self, l):
        if isinstance(l, MongoList):
            return self.__data__ == l.__data__
        else:
            return self.__data__ == l

    def __ne__(self, l):
        return not self.__eq__(l)

    def __len__(self):
        return self.__data__.__len__()

    def remove(self, value):
        self.__data__.remove(value)

    def reverse(self):
        self.__data__.reverse()
        return self

    def append(self, value):
        self.__data__.append(value)
        return self

    def extend(self, l):
        self.__data__.extend(l)

    def index(self, item):
        return self.__data__.index(item)

    def to_list(self):
        return self.__data__

class Lazyload(object):
    def __init__(self, model, field):
        self.model = model
        self.field = field

    def __call__(self, document):
        id = document[self.field]

        try:
            cache = self.__dict__['__cache__']
        except KeyError:
            self.__dict__['__cache__'] = {}
            cache = self.__dict__['__cache__']

        try:
            return cache[id]
        except KeyError:
            cache[id] = self.model.documents.get(id)
            return cache[id]

class Model(object):
    def __init__(self, cls):
        self.cls = cls
        self.fields = None
        self.attrs = []

    def add_attr(self, id, val=None, multi=False):
        self.attrs.append((id, val if val else id.replace('_id', ''), multi))

    def add_fields(self, fields):
        if fields:
            if self.fields == None:
                self.fields = []

            for field in fields:
                if field not in self.fields:
                    self.fields.append(field)

class Mapping(object):
    def __init__(self):
        self.attrs = []
        self.docs = []

class Eagerload(object):
    def __init__(self, data):
        self.models = dict()
        self.mapping = collections.defaultdict(Mapping)

        if isinstance(data, list) or isinstance(data, MongoList):
            self.docs = data
        elif isinstance(data, dict) or isinstance(data, Document) or isinstance(data, MongoDict):
            self.docs = [data]
        else:
            self.docs = data.all()

    def add(self, cls, id_attr, val_attr=None, fields=None, multi=False):
        model = self.models.get(cls, None)

        if not model:
            model = Model(cls)
            self.models[cls] = model

        model.add_attr(id_attr, val_attr, multi)
        model.add_fields(fields)

        return self

    def flush(self):
        if len(self.docs) == 0:
            return

        for model in self.models.values():
            ids = set()

            # cached calls
            add = ids.add
            coerce = model.cls.coerce

            for doc in self.docs:
                for id_attr, val_attr, multi in model.attrs:
                    try:
                        id = doc[id_attr]

                        if id:
                            if multi:
                                for id_ in id:
                                    id_ = coerce(id_)
                                    doc_ = doc.get(id_attr, None)

                                    if doc_:
                                        self._add_mapping(id_, doc_, val_attr, multi)
                                        add(id_)
                            else:
                                id = coerce(id)
                                self._add_mapping(id, doc, val_attr, multi)
                                add(id)
                    except KeyError:
                        pass

            if ids:
                if len(ids) == 1:
                    spec = ids.pop()
                else:
                    spec = {'$in': list(ids)}

                for value in model.cls.documents.find({'_id': spec}, fields=model.fields):
                    m = self.mapping[value._id]

                    for doc in m.docs:
                        for attr, multi in m.attrs:
                            if multi:
                                try:
                                    while True:
                                        doc[doc.index(value._id)] = value
                                except ValueError:
                                    pass
                                except IndexError:
                                    pass
                                except KeyError:
                                    pass
                            else:
                                doc[attr] = value

        return self.docs

    def _add_mapping(self, id, doc, attr, multi):
        m = self.mapping[id]
        m.attrs.append((attr, multi))
        m.docs.append(doc)
