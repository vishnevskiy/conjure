import copy
import types
import collections

class Statement(object):
    def __init__(self, expressions):
        if type(expressions) == types.ListType:
            self.expressions = {}
            self.add(*expressions)
        else:
            self.expressions = expressions

    def add(self, k, ops, v):
        raise NotImplemented()

    def to_dict(self):
        raise NotImplemented()

    def __eq__(self, other):
        if type(other) == dict:
            return self.to_dict() == other

        return self == other

    def __contains__(self, v):
        return self.expressions.__contains__(v)

    def __iter__(self):
        return self.expressions.__iter__()

    def __repr__(self):
        return self.to_dict().__repr__()

    def __getitem__(self, k):
        return self.expressions.__getitem__(k)

    def __setitem__(self, k, v):
        return self.expressions.__setitem__(k, v)

    def __delitem__(self, k):
        return self.expressions.__delitem__(k)

    def clone(self):
        return copy.deepcopy(self)

class UpdateStatement(Statement):
    def add(self, op, k, v):
        self.expressions[op + ':' + k] = v

    def to_dict(self):
        d = collections.defaultdict(dict)

        for key in self:
            op, _, k = key.partition(':')
            d['$' + op][k] = self[key]

        return dict(d)
    
    # &
    def __and__(self, other):
        stmt = self.clone()

        for key in other:
            if key in stmt:
                if key.startswith('pushAll:') or key.startswith('pullAll:'):
                    stmt[key].extend(copy.deepcopy(other[key]))
                    continue
                elif key.startswith('inc:'):
                    stmt[key] += other[key]
                    continue

            stmt[key] = copy.deepcopy(other[key])

        return UpdateStatement(stmt)

class SpecStatement(Statement):
    def add(self, k, ops, v):
        self.expressions[k + ':' + ':'.join(ops.split())] = v

    def to_dict(self):
        d = {}

        for expr in self:
            key, ops = self._parse_expression(expr)
            val = self[expr]

            current = d[key] = d.get(key, {})
            last_key = key
            path = [d, current]

            for op in ops:
                last_key = '$' + op
                next = current[last_key] = current.get(last_key, {})
                path.append(next)
                current = next

            path[-2][last_key] = val

        return d

    def _parse_expression(self, expr):
        if expr.startswith(':'):
            key, ops = '', expr[1:]
        elif expr.endswith(':'):
            key, ops = expr[:-1], ''
        else:
            key, _, ops = expr.partition(':')

        ops = ops.split(':')

        try:
            ops.remove('')
        except ValueError:
            pass

        return key, ops

    # |
    def __or__(self, other):
        return SpecStatement(['', 'or', [self.clone(), other.clone()]])

    __ior__ = __or__

    # &
    def __and__(self, other):
        stmt = self.clone()

        for expr in other:
            if expr in stmt and expr == ':or':
                stmt[expr].extend(other[expr])
                continue

            stmt[expr] = other[expr]

        return stmt

    __iand__ = __and__

    # ~
    def __invert__(self):
        return SpecStatement(self._invert('not'))

    def _invert(self, op):
        stmt = self.clone()

        for expr in self:
            key, ops = self._parse_expression(expr)

            if op not in ops:
                ops.insert(0, op)
            else:
                ops.remove(op)
                
            stmt[key + ':' + ':'.join(ops)] = stmt[expr]

            del stmt[expr]

        return stmt.expressions

    def _swap(self, old_op, new_op):
        stmt = self.clone()

        for expr in self:
            key, ops = self._parse_expression(expr)

            ops[ops.index(old_op)] = new_op

            stmt[key + ':' + ':'.join(ops)] = stmt[expr]
            del stmt[expr]

        return stmt.expressions

class Equal(SpecStatement):
    def __invert__(self):
        return NotEqual(self._invert('ne'))

class NotEqual(SpecStatement):
    def __invert__(self):
        return LessThan(self._invert('ne'))

class LessThan(SpecStatement):
    def __invert__(self):
        return GreaterThanEqual(self._swap('lt', 'gt'))

class GreaterThan(SpecStatement):
    def __invert__(self):
        return GreaterThanEqual(self._swap('gt', 'lt'))

class LessThanEqual(SpecStatement):
    def __invert__(self):
        return GreaterThanEqual(self._swap('lte', 'gte'))

class GreaterThanEqual(SpecStatement):
    def __invert__(self):
        return LessThanEqual(self._swap('gte', 'lte'))

class Mod(SpecStatement):
    pass

class In(SpecStatement):
    def __invert__(self):
        return NotIn(self._swap('in', 'nin'))

class NotIn(SpecStatement):
    def __invert__(self):
        return In(self._swap('nin', 'in'))

class All(SpecStatement):
    pass

class Size(SpecStatement):
    pass

class Exists(SpecStatement):
    def __invert__(self):
        stmt = self.clone()

        for expr in self:
            stmt[expr] = not stmt[expr]

        return Exists(stmt)

class Type(SpecStatement):
    pass

class Where(SpecStatement):
    pass

class Slice(SpecStatement):
    pass