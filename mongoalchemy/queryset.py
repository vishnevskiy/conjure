import types

class Q(object):
    def __init__(self, spec=None):
        self.spec = spec or dict()
        
    def filter(self, *expressions):
        q = self.clone()
        
        for expr in expressions:
            for k, v in expr.spec.iteritems():
                if k in q.spec and type(v) is types.DictType\
                    and type(q.spec[k]) is types.DictType:
                        
                    q.spec[k].update(v)
                else:
                    q.spec[k] = v

        return q

    def update(self, spec):
        print 'update %s with %s' % (self.__repr__(), spec)

    def delete(self):
        return self.spec
        
    def count(self):
        pass
    
    def __add__(self, other):
        qs = self.clone()
        qs.spec.update(other.spec)
        
        return qs
    
    __iadd__ = __add__

    def __or__(self, other):
        return Q({'$or': [self.spec, other.spec]})
    
    __ior__ = __or__
    
    def __repr__(self):
        return self.spec.__repr__()
    
    def clone(self):
        return Q(self.spec)

class QuerySet(object):
    def ensure_index(self, key_or_list):
        pass

    def filter(self, *expressions):
        pass

    def filter_by(self, **query):
        # allows just doing username='stanislav', email='vishnevskiy@gmail.com'
        pass

    def one(self):
        # calls find_one
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