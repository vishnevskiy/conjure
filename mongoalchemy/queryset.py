import types

class Manager(object):
    def __get__(self, instance, owner):
        if instance is not None:
            return self

        return QuerySet()

class QuerySet(object):
    def __init__(self, spec=None):
        self.spec = spec or dict()

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
        pass

    def next(self):
        # get next from cursor
        pass
    
    def rewind(self):
        # rewind the cursor
        pass

    def count(self):
        # number of objects returned by query
        pass

    def __len__(self):
        return self.count()

    def limit(self, n):
        pass

    def skip(self, n):
        pass

    def __getitem__(self, key):
        # will allow limit/skip/index
        pass

    def only(self, *fields):
        # list of fields to bring from db, defaults to all
        pass

    def sort(self, *keys):
        pass

    def explain(self):
        pass

    def delete(self):
        pass

    def remove(self):
        # alias for delete
        pass

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
        return self.spec.__repr__()