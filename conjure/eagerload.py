from collections import defaultdict

__all__ = ['Eagerload']


def fieldgetter(item):
    names = item.split('.')

    def proxy(doc):
        for name in names[:-1]:
            doc = getattr(doc, name)

        return doc

    return proxy


class Meta(object):
    def __init__(self, field):
        self.key = field.get_key()
        self.depth = field.get_key().count('.')

        if field.__class__.__name__ == 'ListField':
            self.field = field.field
            self.name = field.name + '_'
            self.multi = True
        else:
            self.field = field
            self.name = field.name + '_id'
            self.multi = False


class Eagerload(object):
    def __init__(self, only=None):
        self.only = only
        self.fields = []
        self.documents = []
        self.document_cls = None

        self.single = {}
        self.multi = {}

    def add_field(self, field):
        meta = Meta(field)

        if not self.document_cls:
            self.document_cls = meta.field.document_cls
        else:
            assert self.document_cls is meta.field.document_cls, 'All fields must load the same document.'

        self.fields.append(meta)

        return self

    def add_documents(self, documents):
        if isinstance(documents, list):
            for document in documents:
                self.add_document(document)
        else:
            self.add_document(documents)

        return self

    def add_document(self, document):
        for meta in self.fields:
            if not meta.depth:
                self._add_document(meta, document)
            else:
                try:
                    documents = fieldgetter(meta.key)(document)

                    if isinstance(documents, list):
                        for document in documents:
                            self._add_document(meta, document)
                    else:
                        self._add_document(meta, document)
                except AttributeError:
                    pass

        return self

    def _add_document(self, meta, document):
        if getattr(document, meta.name, None):
            self.documents.append((meta, document))

    def flush(self):
        if not len(self.documents):
            return

        mapping = defaultdict(list)

        for meta, document in self.documents:
            id_values = getattr(document, meta.name)

            if id_values:
                if meta.multi:
                    for i, id_value in enumerate(id_values):
                        mapping[id_value].append((document._data[meta.field.owner.name], None, i))
                else:
                    mapping[id_values].append((document._data, meta.field.name, -1))

        if mapping:
            cls = self.document_cls

            ids = mapping.keys()

            if len(ids) == 1:
                values = cls.objects.filter(cls.id == ids[0])
            else:
                values = cls.objects.filter(cls.id.in_(ids))

            if self.only is not None:
                values = values.only(*self.only)

            for value in values:
                for data, name, i in mapping[values._data['id']]:
                    if name is None:
                        data[i] = value
                    else:
                        data[name] = value