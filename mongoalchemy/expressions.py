import copy
import types
import collections

class UpdateExpression(object):
    def __init__(self, spec):
        self.spec = spec

    # &
    def __and__(self, other):
        spec = collections.defaultdict(dict)
        spec.update(copy.deepcopy(self.spec))

        for k, v in other.spec.iteritems():
            if k in ['$pushAll', '$pullAll']:
                for k2, v2 in v.iteritems():
                    if k2 in spec[k]:
                        spec[k][k2].extend(copy.deepcopy(v2))
                    else:
                        spec[k][k2] = copy.deepcopy(v2)
            elif k == '$inc':
                for k2, v2 in v.iteritems():
                    if k2 in spec[k]:
                        spec[k][k2] += v2
                    else:
                        spec[k][k2] = v2
            else:
                spec[k].update(copy.deepcopy(v))

        return UpdateExpression(dict(spec))
        
    def __eq__(self, other):
        if type(other) == dict:
            return self.spec == other

        return self == other

    def __repr__(self):
        return self.spec.__repr__()

class QueryExpression(object):
    def __init__(self, spec):
        self.spec = spec

    # |
    def __or__(self, other):
        return QueryExpression({'$or': [copy.deepcopy(self.spec), copy.deepcopy(other.spec)]})

    __ior__ = __or__

    # &
    def __and__(self, other):
        spec = copy.deepcopy(self.spec)

        for k, v in other.iteritems():
            if v != None and type(v) == type(spec.get(k)):
                if k == '$or' and type(v) == types.ListType:
                    spec[k].extend(v)
                    continue
                elif type(v) == types.DictType:
                    for k2, v2 in v.iteritems():
                        pass


            spec[k] = copy.deepcopy(v)

        return QueryExpression(spec)

    __iand__ = __and__

    # ~
    def __invert__(self):
        return QueryExpression(self._invert('$not'))

    def __eq__(self, other):
        if type(other) == dict:
            return self.spec == other

        return self == other

    def __repr__(self):
        return self.spec.__repr__()

    def _invert(self, x):
        spec = {}

        for k, v in self.spec.iteritems():
            if type(v) == types.DictType and x in v:
                spec[k] = copy.deepcopy(v[x])
            else:
                spec[k] = {x: copy.deepcopy(v)}

        return spec

    def _swap(self, x, y):
        spec = {}

        for k, v in self.spec.iteritems():
            spec[k] = {y: copy.deepcopy(v[x])}

        return spec

class CompoundExpression(QueryExpression):
    def _invert(self, x):
        raise NotImplemented()

class EqualExpression(QueryExpression):
    def __invert__(self):
        return NotEqualExpression(self._invert('$ne'))

class NotEqualExpression(QueryExpression):
    def __invert__(self):
        return LessThanExpression(self._invert('$ne'))

class LessThanExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('$lt', '$gt'))

class GreaterThanExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('$gt', '$lt'))

class LessThanEqualExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('$lte', '$gte'))

class GreaterThanEqualExpression(QueryExpression):
    def __invert__(self):
        return LessThanEqualExpression(self._swap('$gte', '$lte'))

class ModExpression(QueryExpression):
    pass

class InExpression(QueryExpression):
    def __invert__(self):
        return NotInExpression(self._swap('$in', '$nin'))

class NotInExpression(QueryExpression):
    def __invert__(self):
        return InExpression(self._swap('$nin', '$in'))

class AllExpression(QueryExpression):
    pass

class SizeExpression(QueryExpression):
    pass

class ExistsExpression(QueryExpression):
    def __invert__(self):
        spec = copy.deepcopy(self.spec)

        for v in spec.itervalues():
            v['$exists'] = not v['$exists']

        return ExistsExpression(spec)

class TypeExpression(QueryExpression):
    pass

class WhereExpression(QueryExpression):
    pass

class SliceExpression(QueryExpression):
    pass