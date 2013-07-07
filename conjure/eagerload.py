from collections import defaultdict
from .exceptions import EagerloadException

__all__ = ['Eagerload']

ITERABLE_TYPES = (tuple, list, set)
ITERABLE_FIELDS = {'ListField', 'MapField'}


class TargetField(object):
    def __init__(self, field):
        key = field.get_key()

        try:
            self.field_attr = key[:key.rindex('.')]
        except ValueError:
            self.field_attr = None

        if field.__class__.__name__ in ITERABLE_FIELDS:
            self.name = field.field.owner.name
            self.document_cls = field.field.document_cls
            self.id_attr = field.name + '_'
            self.iterable = True
        else:
            self.name = field.name
            self.document_cls = field.document_cls
            self.id_attr = field.name + '_id'
            self.iterable = False


class Eagerload(object):
    def __init__(self, only=None):
        self.only = only
        self.fields = []
        self.mapping = defaultdict(list)
        self.document_cls = None

    def add_field(self, field):
        target_field = TargetField(field)

        if not self.document_cls:
            self.document_cls = target_field.document_cls
        elif self.document_cls is not target_field.document_cls:
            raise EagerloadException('All fields must load the same document.')

        self.fields.append(target_field)

        return self

    def add_documents(self, documents):
        if not isinstance(documents, ITERABLE_TYPES):
            self.add_document(documents)
        else:
            for document in documents:
                self.add_document(document)
        return self

    def add_document(self, document):
        for field in self.fields:
            if field.field_attr:
                documents = getattr(field.field_attr, document)
                if not isinstance(documents, ITERABLE_TYPES):
                    documents = [documents]
            else:
                documents = [document]

            for doc in documents:
                self._add_document(field, doc)

        return self

    def _add_document(self, field, document):
        try:
            ids = getattr(document, field.id_attr)
        except AttributeError:
            return

        if not ids:
            return

        if field.iterable:
            if isinstance(ids, dict):
                gen = ids.viewitems()
            else:
                gen = enumerate(ids)

            for k, v in gen:
                self.mapping[v].append((k, document._data[field.name]))
        else:
            self.mapping[ids].append((field.name, document._data))

    def flush(self):
        if not self.mapping:
            return

        mapping = self.mapping
        cls = self.document_cls
        ids = mapping.keys()

        cursor = cls.objects.filter(cls.id == ids[0] if len(ids) == 1 else cls.id.in_(ids))

        if self.only is not None:
            cursor.only(*self.only)

        for document in cursor:
            for key, data in mapping[document._data['id']]:
                data[key] = document