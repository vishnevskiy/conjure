from mongoalchemy.connection import connect
from mongoalchemy.utils import transform_keys
import spec
import exceptions
import pymongo

class Manager(object):
    def __init__(self):
        self._collection = None

    def __get__(self, instance, owner):
        if instance is not None:
            return self

        if self._collection is None:
            db = connect(owner._meta['db'])
            self._collection = db[owner._meta['collection']]

        return QuerySet(owner, self._collection)

class QuerySet(object):
    def __init__(self, document_cls, collection):
        self._document_cls = document_cls
        self._collection = collection
        self._spec = spec.QuerySpecification(None)
        self._pymongo_cursor = None
        self._fields = None
        
    def ensure_index(self, key_or_list):
        indexes = transform_keys(key_or_list)
        self._collection.ensure_index(indexes)
        return self

    def find(self, *expressions):
        return self.filter(*expressions).all()

    def find_one(self, *expressions):
        return self.filter(*expressions).one()

    def filter(self, *expressions):
        for expression in expressions:
            self._spec &= expression

        return self

    def filter_by(self, **kwargs):
        for k,v  in kwargs.iteritems():
            self._spec &= getattr(self._document_cls, k).eq(v)

        return self

    def exclude(self, *expressions):
        for expression in expressions:
            self._spec &= ~expression

        return self

    def exclude_by(self, **kwargs):
        for k,v  in kwargs.iteritems():
            self._spec &= ~getattr(self._document_cls, k).eq(v)

        return self

    def one(self):
        return self._collection.find_one(self._spec.compile(), fields=self._fields)

    def first(self):
        try:
            return self[0]
        except IndexError:
            raise exceptions.DoesNotExist()

    def all(self):
        return list(self)

    def with_id(self, object_id):
        return self._document_cls.from_mongo(self.filter_by(_id=self._document_cls._id.to_mongo(object_id)).one())

    def in_bulk(self, object_ids):
        field = self._document_cls._id
        return  dict([(doc._id, doc) for doc in self.filter(field.in_(map(field.to_mongo, object_ids)))])

    def next(self):
        try:
            return self._document_cls.from_mongo(self._cursor.next())
        except StopIteration, e:
            self.rewind()
            raise e
    
    def rewind(self):
        self._cursor.rewind()
        return self

    def count(self):
        return self._cursor.count(with_limit_and_skip=True)

    def __len__(self):
        return self.count()

    def limit(self, n):
        self._cursor.limit(n)
        return self

    def skip(self, n):
        self._cursor.skip(n)
        return self

    def __getitem__(self, key):
        if isinstance(key, slice):
            self._pymongo_cursor = self._cursor[key]
            return self
        elif isinstance(key, int):
            return self._document_cls.from_mongo(self._cursor[key])

    def only(self, *fields):
        self._fields = ['_cls']

        for field in fields:
            if isinstance(field, basestring):
                self._fields.append({
                    field: 1
                })
            elif isinstance(field, fields.Field):
                self._fields.append({
                    field.name: 1
                })
            elif isinstance(field, spec.Slice):
                self._fields.append(field.compile())
                
        return self

    def sort(self, keys):
        keys = transform_keys(keys)
        self._cursor.sort(keys)
        return self

    def explain(self, pretty=False):
        plan = self._cursor.explain()

        if pretty:
            import pprint
            plan = pprint.pformat(plan)

        return plan

    def delete(self, safe=False):
        return self._collection.remove(self._spec.compile(), safe=safe)

    def _update(self, update, safe, upsert, multi):
       try:
           self._collection.update(self._spec.compile(), update, safe=safe, upsert=upsert, multi=multi)
       except pymongo.errors.OperationFailure, err:
           raise exceptions.OperationError(unicode(err))

    def update(self, update_spec, safe=False):
        self._update(update_spec.compile(), safe, upsert=False, multi=True)

    def update_one(self, update_spec, safe=False):
        self._update(update_spec.compile(), safe, upsert=False, multi=False)

    def upsert(self, update_spec, safe=False):
        self._update(update_spec.compile(), safe, upsert=True, multi=False)

    def __iter__(self):
        return self

    @property
    def _cursor(self):
        if self._pymongo_cursor is None:
            self._pymongo_cursor = self._collection.find(self._spec.compile(), fields=self._fields)

        return self._pymongo_cursor

    def __repr__(self):
        return self._spec.__repr__()