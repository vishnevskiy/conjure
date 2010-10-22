class Eagerload(object):
    def __init__(self, field, fields=None):
        self.fields = fields

        if field.__class__.__name__ == 'ListField':
            self.field = field.field
            self.name = field.name + '_'
            self.multi = True
        else:
            self.field = field
            self.name = field.name + '_id'
            self.multi = False

        self.documents = []

    def add_documents(self, documents):
        if isinstance(documents, list):
            self.documents.extend(documents)
        else:
            self.documents.append(documents)

    def flush(self):
        if len(self.documents) == 0:
            return

        ids = set()

        for document in self.documents:
            ref_id = getattr(document, self.name)

            if ref_id:
                if isinstance(ref_id, list):
                    ids |= set(ref_id)
                else:
                    ids.add(ref_id)

        if ids:
            cls = self.field.document_cls

            if len(ids) == 1:
                values = cls.objects.filter(cls.id == ids.pop())
            else:
                values = cls.objects.filter(cls.id.in_(list(ids)))

            if self.fields is not None:
                values = values.only(*self.fields)

            for value in values:
                for document in self.documents:
                    if self.multi:
                        try:
                            data = document._data[self.field.name]

                            while True:
                                data[data.index(value.id)] = value
                        except (ValueError, IndexError, KeyError):
                            pass
                    else:
                        if document._data.get(self.field.name) == value._data['id']:
                            document._data[self.field.name] = value