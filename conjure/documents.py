from .base import BaseDocument, DocumentMeta, ObjectIdField
from .exceptions import OperationError
from .query import Query
import pymongo.errors

__all__ = ['Document', 'EmbeddedDocument']


class Document(BaseDocument):
    __metaclass__ = DocumentMeta

    id = ObjectIdField(db_field='_id')
    objects = Query(None, None)

    def save(self, safe=True, insert=False):
        self.validate()

        doc = self.to_mongo()

        try:
            collection = self.__class__.objects._collection

            if insert:
                object_id = collection.insert(doc, safe=safe)
            else:
                object_id = collection.save(doc, safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise OperationError(unicode(err))

        self['id'] = object_id

    def delete(self, safe=False):
        #noinspection PyUnresolvedReferences
        object_id = self._fields['id'].to_mongo(self.id)

        try:
            self.__class__.objects.filter_by(id=object_id).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise OperationError(unicode(err))

    def reload(self):
        data = self.__class__.objects.filter_by(id=self.id)._one()

        #noinspection PyUnresolvedReferences
        for field in self.__class__._fields.values():
            if field.db_field in data:
                self._data[field.name] = field.to_python(data[field.db_field])

    @classmethod
    def drop_collection(cls):
        cls.objects._collection.drop()


class EmbeddedDocument(BaseDocument):
    __metaclass__ = DocumentMeta
    _meta = {'embedded': True}
