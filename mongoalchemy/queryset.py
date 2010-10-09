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

class Expression(object):
    def __init__(self, spec):
        self.spec = spec
    
    # |
    def __or__(self, other):
        self.spec = {'$or': [self.spec, other.spec]}
        return self
    
    __ior__ = __or__
    
    # &
    def __and__(self, other):
        self.spec.update(other.spec)
        return self

    __iand__ = __and__

    # ~
    def __invert__(self):
        if len(self.spec) == 1:
            v = self.spec.values()[0]
            
            if len(v) == 1 and v.has_key('$in'):
                v['$nin'] = v['$in']
                del v['$in']
        
                return self
        
        self.spec = {'$not': self.spec}
            
        return self