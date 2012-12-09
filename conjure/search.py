import collections
import multiprocessing
import pyes
from pyes.exceptions import NotFoundException
import pymongo
import logging
import time
from bson.objectid import ObjectId
from .spec import QuerySpecification
from .oplog_watcher import OplogWatcher
import base64

_indexes = []
_connections = {}


class IndexMeta(type):
    def __new__(mcs, name, bases, attrs):
        metaclass = attrs.get('__metaclass__')
        super_new = super(IndexMeta, mcs).__new__

        if metaclass and issubclass(metaclass, IndexMeta):
            return super_new(mcs, name, bases, attrs)

        terms = {}

        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, Term):
                term = attr_value

                term.name = attr_name

                if term.index_name is None:
                    term.index_name = term.name

                terms[attr_name] = attr_value

                del attrs[attr_name]

        attrs['terms'] = terms

        meta = attrs.pop('Meta', None)

        attrs['_meta'] = {
            'host': getattr(meta, 'host'),
            'model': getattr(meta, 'model'),
            'spec': getattr(meta, 'spec', QuerySpecification()),
        }

        new_cls = super_new(mcs, name, bases, attrs)

        index = new_cls.instance()

        _indexes.append(index)
        index.model._search_index = index

        return new_cls


class Index(object):
    __metaclass__ = IndexMeta

    def __init__(self):
        self._meta = self.__class__._meta
        self.model = self._meta['model']
        self.spec = self._meta['spec']
        self.uri, _, db = self.model._meta['db'].rpartition('/')
        self.namespace = '%s-%s' % (db, self.model._meta['collection'])
        self.doc_type = self.model._name

    @classmethod
    def instance(cls):
        if not hasattr(cls, '_instance'):
            cls._instance = cls()

        return cls._instance

    @property
    def connection(self):
        if not hasattr(self, '_connection'):
            host = self._meta['host']

            if host not in _connections:
                _connections[host] = pyes.ES(host)

            self._connection = _connections[host]

        return self._connection

    def search(self, query, page=1, limit=5, filters=None):
        return search(self, query, page, limit, filters)

    def indexer(self):
        return Indexer(self)


class Term(object):
    def __init__(self, index_name=None, index=True, boost=1.0, null_value=None, coerce=None):
        self.name = None
        self.index_name = index_name
        self.index = index
        self.boost = boost
        self.null_value = null_value
        self.coerce = coerce


class Indexer(object):
    def __init__(self, index):
        self.index = index

    def index_document(self, obj, bulk=False):
        doc = {}

        for term in self.index.terms.values():
            if not term.index:
                continue

            value = getattr(obj, term.name)

            if value is not None:
                if isinstance(value, ObjectId):
                    value = str(value)

                if term.coerce is not None:
                    value = term.coerce(value)

                doc[term.index_name] = value

        self._execute(self.index.connection.index, doc, self.index.namespace,
                      self.index.doc_type, id=base64.b64encode(str(obj.id)), bulk=bulk)

    def delete_document(self, doc_id):
        self._execute(self.index.connection.delete, self.index.namespace,
            self.index.doc_type, base64.b64encode(str(doc_id)))

    def insert(self, obj):
        obj = self.index.model.to_python(obj)
        logging.info('Indexing %s (%s)' % (self.index.model._name, obj.id))
        self.index_document(obj)

    def update(self, obj_id, raw):
        o = raw['o']

        fields = self.index.terms.keys()

        if o.has_key('$set') and len(set(fields) - set(o['$set'].keys())) < len(fields):
            obj = self.index.model.objects.only(*fields).filter(self.index.spec).with_id(obj_id)

            if obj is not None:
                logging.info('Updating %s (%s)' % (self.index.model._name, obj.id))
                self.index_document(obj)
            else:
                self.delete(obj_id)

    def delete(self, obj_id):
        logging.info('Deleting %s (%s)' % (self.index.model._name, obj_id))
        self.delete_document(obj_id)

    def _execute(self, func, *args, **kwargs):
        attempts = 0

        while attempts < 5:
            try:
                func(*args, **kwargs)
                break
            except NotFoundException:
                break
            except Exception:
                attempts += 1
                logging.warning('Retrying... (%d)' % attempts, exc_info=True)
                time.sleep(1)


class ResultSet(object):
    def __init__(self, objects=None, total=0, elapsed_time=0, max_score=0):
        self.objects = objects or []
        self.meta = {}
        self.total = total
        self.elapsed_time = elapsed_time
        self.max_score = max_score

    def append(self, value, meta):
        if value is not None:
            self.objects.append(value)
            self.meta[value] = meta

    def has_more(self):
        return len(self.objects) < self.total

    def __len__(self):
        return self.objects.__len__()

    def __iter__(self):
        for obj in self.objects:
            yield obj, self.meta[obj]


def search(indexes, query, page=1, limit=5, filters=None):
    if not isinstance(indexes, list):
        indexes = [indexes]

    namespaces = []
    models = {}

    for i, index in enumerate(indexes):
        if not isinstance(index, Index):
            model = index

            for index in _indexes:
                if index.model == model:
                    indexes[i] = index
                    break

        namespaces.append(index.namespace)
        models[index.namespace] = index.model

    result_set = ResultSet()
    result_set.query = query

    if isinstance(query, (str, unicode)):
        if query.endswith(':'):
            query = query[:-1]

        if any([op in query for op in ['?', '*', '~', 'OR', 'AND', '+', 'NOT', '-', ':']]):
            query = pyes.StringQuery(query)
        else:
            query = pyes.StringQuery(query + '*')

    if not isinstance(query, pyes.FilteredQuery) and filters:
        term_filter = pyes.TermFilter()

        for field, value in filters.iteritems():
            term_filter.add(field, value)

        query = pyes.FilteredQuery(query, term_filter)

    page = int(page)
    limit = int(limit)
    skip = (page - 1) * limit

    try:
        response = _indexes[0].connection.search(query, indices=namespaces, **{
            'from': str(skip),
            'size': str(limit)
        })

        result_set.total = response.total
        result_set.elapsed_time = response._results['took'] / 1000.0
        result_set.max_score = response.max_score

        for i, hit in enumerate(response.hits):
            result_set.append(models[hit['_index']].objects.with_id(base64.b64decode(hit['_id'])), {
                'rank': skip + i + 1,
                'score': hit['_score'],
                'relevance': int(hit['_score'] / result_set.max_score * 100)
            })
    except pyes.exceptions.SearchPhaseExecutionException:
        pass

    return result_set


def reindex(only=None):
    logging.info('Reindexing...')

    for index in _indexes:
        if only and index.namespace not in only:
            continue

        try:
            index.connection.delete_index(index.namespace)
        except pyes.exceptions.IndexMissingException:
            pass

        index.connection.create_index(index.namespace)

        objects = index.model.objects.only(*index.terms.keys()).filter(index.spec)
        count = objects.count()

        logging.info('%d object(s) from %s' % (count, index.namespace))

        indexer = Indexer(index)

        for i, obj in enumerate(objects):
            i += 1

            if not i % 10000:
                logging.info('%d/%d', i, count)

            indexer.index_document(obj, bulk=True)

        indexer.index.connection.force_bulk()

    logging.info('Done!')


def watch():
    hosts = collections.defaultdict(list)

    global _indexes

    for index in _indexes:
        hosts[index.uri].append(index)

    def target(uri, indexes):
        namespaces = [index.namespace.replace('-', '.') for index in indexes]

        logging.info('Watching %s' % namespaces)

        oplog_watcher = OplogWatcher(pymongo.Connection(uri), namespaces=namespaces)

        for index in indexes:
            indexer = index.indexer()

            for op in ('insert', 'update', 'delete',):
                oplog_watcher.add_handler(index.namespace.replace('-', '.'), op, getattr(indexer, op))

        oplog_watcher.start()

    if len(hosts) > 1:
        for uri, _indexes in hosts.items():
            multiprocessing.Process(target=target, args=(uri, _indexes)).start()
    else:
        target(*hosts.items()[0])

    while True:
        time.sleep(1)
