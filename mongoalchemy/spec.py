import types
from mongoalchemy.connection import connect
from mongoalchemy.utils import transform_keys

class Manager(object):
    def __init__(self):
        self._collection = None

    def __get__(self, instance, owner):
        if instance is not None:
            return self

        if self._collection is None:
            db = connect(owner._meta['db'])
            self._collection = db[owner._meta['collection']]

        return Spec(owner, self._collection)

class Spec(object):
    def __init__(self, document_cls, collection):
        self._document_cls = document_cls
        self._collection = collection
        self._spec = {}

    def ensure_index(self, key_or_list):
        pass

    def find(self, *expressions):
        # same as filter(User.username == 'Stanislav').all()
        pass

    def find_one(self, *expressions):
        # same as filter(User.username == 'Stanislav').one()
        pass

    def filter(self, *expressions):
        # build a query, chains
        pass

    def filter_by(self, **query):
        # build a query, chains
        # allows just doing username='stanislav', email='vishnevskiy@gmail.com'
        pass

    def one(self):
        # calls find_one
        pass

    def first(self):
        # similar to one, but will throw DoesNotExist
        pass

    def all(self):
        # casts self to list
        pass

    def with_id(self):
        # identical to Test.objects.filter_by(_id=5).one()
        pass

    def in_bulk(self, object_ids):
        # queries by list of object ids and returns a map
        return  dict([(doc._id, doc) for doc in self._collection.find({'_id': {'$in': object_ids}})])

    def next(self):
        # get next from cursor
        try:
            if self._limit == 0:
                raise StopIteration
            #return self._document._from_son(self._cursor.next())
        except StopIteration, e:
            self.rewind()
            raise e
    
    def rewind(self):
        # rewind the cursor
        self._cursor.rewind()
        return self

    def count(self):
        # number of objects returned by query
        return self._cursor.count(with_limit_and_skip=True)

    def __len__(self):
        return self.count()

    def limit(self, n):
        n = n or 1
        self._cursor.limit(n)
        self._limit = n
        return self

    def skip(self, n):
        self._cursor.skip(n)
        self._skip = n
        return self

    def __getitem__(self, key):
        # will allow limit/skip/index
        pass

    def only(self, *fields):
        # list of fields to bring from db, defaults to all
        pass

    def sort(self, keys):
        keys = transform_keys(keys)
        self._ordering = keys
        self._cursor.sort(keys)
        return self

    def explain(self, pretty=False):
        plan = self._cursor.explain()

        if pretty:
            import pprint
            plan = pprint.pformat(plan)

        return plan

    def delete(self, safe=False):
        return self._collection.remove(self._spec, safe=safe)

    def update(self):
        # uses multi=True
        pass

    def update_one(self):
        pass

    def upsert(self):
        # users upsert=True
        pass

    def __iter__(self):
        pass

    def slave_ok(self):
        # allow querying avaliable slaves for this query
        pass

    def __repr__(self):
        return self._spec.__repr__()