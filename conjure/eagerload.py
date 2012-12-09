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

        ids = set()

        for meta, document in self.documents:
            ref_id = getattr(document, meta.name)

            if ref_id:
                if meta.multi:
                    ids |= set(ref_id)
                else:
                    ids.add(ref_id)

        if ids:
            cls = self.document_cls

            if len(ids) == 1:
                values = cls.objects.filter(cls.id == ids.pop())
            else:
                values = cls.objects.filter(cls.id.in_(list(ids)))

            if self.only is not None:
                values = values.only(*self.only)

            documents = self.documents

            for value in values:
                remaining = []

                for meta, document in self.documents:
                    if meta.multi:
                        try:
                            data = document._data[meta.field.owner.name]

                            while True:
                                data[data.index(value.id)] = value
                        except (ValueError, IndexError, KeyError):
                            pass
                    elif document._data.get(meta.field.name) == value._data['id']:
                        document._data[meta.field.name] = value
                        continue

                    remaining.append((meta, documents))

                documents = remaining
