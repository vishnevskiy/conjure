import pymongo.errors
import time


class OplogWatcher(object):
    def __init__(self, connection, namespaces, poll_time=1.0):
        if namespaces:
            self._ns_filter = {'$in': namespaces}
        else:
            self._ns_filter = None

        self.poll_time = poll_time
        self.connection = connection
        self._handlers = {}

    @staticmethod
    def __get_id(op):
        id = None

        o2 = op.get('o2')

        if o2 is not None:
            id = o2.get('_id')

        if id is None:
            id = op['o'].get('_id')

        return id

    def start(self):
        try:
            self.connection.admin.command({'replSetGetStatus': 1})
            oplog = self.connection.local['oplog.rs']
        except pymongo.errors.OperationFailure:
            oplog = self.connection.local['oplog.$main']

        ts = oplog.find().sort('$natural', -1)[0]['ts']

        while True:
            if self._ns_filter is None:
                filter = {}
            else:
                filter = {'ns': self._ns_filter}

            filter['ts'] = {'$gt': ts}

            try:
                cursor = oplog.find(filter, tailable=True)

                while True:
                    for op in cursor:
                        ts = op['ts']
                        id = self.__get_id(op)

                        self.all_with_noop(ns=op['ns'], ts=ts, op=op['op'], id=id, raw=op)

                    self.sleep()

                    if not cursor.alive:
                        break
            except pymongo.errors.OperationFailure:
                self.sleep()
            except pymongo.errors.AutoReconnect:
                self.sleep()

    def sleep(self):
        time.sleep(self.poll_time)

    def all_with_noop(self, ns, ts, op, id, raw):
        if op == 'n':
            self._execute(ns, 'noop', ts)
        else:
            self.all(ns, ts, op, id, raw)

    def all(self, ns, _, op, id, raw):
        if op == 'i':
            self._execute(ns, 'insert', raw['o'])
        elif op == 'u':
            self._execute(ns, 'update', id, raw)
        elif op == 'd':
            self._execute(ns, 'delete', id)
        elif op == 'c':
            self._execute(ns, 'command', raw)
        elif op == 'db':
            self._execute(ns, 'db_declare', raw)

    def add_handler(self, ns, op, func):
        self._handlers['%s:%s' % (ns, op)] = func

    def remove_handler(self, ns, op):
        del self._handlers['%s:%s' % (ns, op)]

    def _execute(self, ns, op, *args, **kwargs):
        func = self._handlers.get('%s:%s' % (ns, op))

        if func is not None:
            func(*args, **kwargs)
