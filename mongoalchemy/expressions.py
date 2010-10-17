import copy
import types
import collections

class Expression(object):
    def __init__(self, exprs):
        self.spec = {}

        if type(exprs) == types.ListType:
            for expr in exprs:
                self.add(expr)

    def to_dict(self):
        spec = {}

        for key in self:
            k, ops = self._parse_key(key)
            v = self[key]

            try:
                ops.remove('')
            except ValueError:
                pass

            last = spec[k] = spec.get(k, {})
            path = [spec, last]

            for op in ops:
                last = last['$' + op] = last.get('$' + op, {})
                path.append(last)

            path[-2][k] = v
        
        return spec

    def _parse_key(self, key):
        if key.startswith(':'):
            k, ops = '', key[1:]
        elif key.endswith(':'):
            k, ops = key[:-1], ''
        else:
            k, _, ops = key.partition(':')

        ops = ops.split(':')

        return k, ops

    def __eq__(self, other):
        if type(other) == dict:
            return self.to_dict() == other

        return self == other

    def __contains__(self, v):
        return self.spec.__contain__(v)

    def __iter__(self):
        return self.spec.__iter__()

    def __repr__(self):
        return self.to_dict().__repr__()

    def __getitem__(self, k):
        return self.spec.__getitem__(k)

    def __setitem__(self, k, v):
        return self.spec.__setitem__(k, v)

    def __delitem__(self, k):
        return self.spec.__delitem__(k)

    def clone(self):
        return copy.deepcopy(self)

class UpdateExpression(Expression):
    def __init__(self, op, k, v):
        Expression.__init__(self)
        self.spec[op + ':' + k] = v

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
    def __init__(self, k, op, v):
        Expression.__init__(self)
        self.spec[k + ':' + ':'.join(op.split())] = v

    # |
    def __or__(self, other):
        return QueryExpression(['', 'or', [self.clone(), other.clone()]])

    __ior__ = __or__

    # &
    def __and__(self, other):
        expr = self.clone()

        for k_op in other:
            if k_op in expr and k_op == ':or':
                expr[k_op].extend(other[k_op])
                continue

            expr[k_op] = other[k_op]

        return expr

    __iand__ = __and__

    # ~
    def __invert__(self):
        return QueryExpression(self._invert('not'))

    def _invert(self, op):
        expr = self.clone()

        for k_ops in expr:
            k, ops = self._parse_key(k_ops)

            if op not in ops:
                ops.insert(0, op)
            else:
                ops.remove(op)

            expr[k + ':' + ':'.join(ops)] = expr[k_ops]
            del expr[k_ops]

        return expr

    def _swap(self, old_op, new_op):
        expr = self.clone()

        for k_ops in expr:
            k, ops = self._parse_key(k_ops)

            ops[ops.index(old_op)] = new_op

            expr[k + ':' + ':'.join(ops)] = expr[k_ops]
            del expr[k_ops]

        return expr

class CompoundExpression(QueryExpression):
    def _invert(self, x):
        raise NotImplemented()

class EqualExpression(QueryExpression):
    def __invert__(self):
        return NotEqualExpression(self._invert('ne'))

class NotEqualExpression(QueryExpression):
    def __invert__(self):
        return LessThanExpression(self._invert('ne'))

class LessThanExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('lt', 'gt'))

class GreaterThanExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('gt', 'lt'))

class LessThanEqualExpression(QueryExpression):
    def __invert__(self):
        return GreaterThanEqualExpression(self._swap('lte', 'gte'))

class GreaterThanEqualExpression(QueryExpression):
    def __invert__(self):
        return LessThanEqualExpression(self._swap('gte', 'lte'))

class ModExpression(QueryExpression):
    pass

class InExpression(QueryExpression):
    def __invert__(self):
        return NotInExpression(self._swap('in', 'nin'))

class NotInExpression(QueryExpression):
    def __invert__(self):
        return InExpression(self._swap('nin', 'in'))

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