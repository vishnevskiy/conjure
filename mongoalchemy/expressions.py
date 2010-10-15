import copy
import types
import collections

class Expression(object):
    def __init__(self, spec):
        self.spec = spec

    def __eq__(self, other):
        if type(other) == dict:
            return self.spec == other

        return self == other

    def __repr__(self):
        return self.spec.__repr__()

    def clone(self):
        return copy.deepcopy(self.spec)

class UpdateExpression(Expression):
    # &
    def __and__(self, other):
        spec = collections.defaultdict(dict)
        spec.update(self.clone())

        for k, v in other.clone().iteritems():
            if k in ['$pushAll', '$pullAll']:
                for k2, v2 in v.iteritems():
                    if k2 in spec[k]:
                        spec[k][k2].extend(v2)
                    else:
                        spec[k][k2] = v2
            elif k == '$inc':
                for k2, v2 in v.iteritems():
                    if k2 in spec[k]:
                        spec[k][k2] += v2
                    else:
                        spec[k][k2] = v2
            else:
                spec[k].update(v)

        return UpdateExpression(dict(spec))

class QueryExpression(Expression):
    # |
    def __or__(self, other):
        return QueryExpression({'$or': [self.clone(), other.clone()]})

    __ior__ = __or__

    # &
    def __and__(self, other):
        spec = self.clone()

        for k, v in other.clone().iteritems():
            if v != None and type(v) == type(spec.get(k)):
                if k == '$or' and type(v) == types.ListType:
                    spec[k].extend(v)
                    continue

            spec[k] = v

        return QueryExpression(spec)

    __iand__ = __and__

    # ~
    def __invert__(self):
        return QueryExpression(self._invert('$not'))

    def _invert(self, x):
        spec = {}

        for k, v in self.clone().iteritems():
            if type(v) == types.DictType and x in v:
                spec[k] = v[x]
            else:
                spec[k] = {x: v}

        return spec

    def _swap(self, x, y):
        spec = {}

        for k, v in self.clone().iteritems():
            spec[k] = {y: v[x]}

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
        spec = self.clone()

        for v in spec.itervalues():
            v['$exists'] = not v['$exists']

        return ExistsExpression(spec)

class TypeExpression(QueryExpression):
    pass

class WhereExpression(QueryExpression):
    pass

class SliceExpression(QueryExpression):
    pass