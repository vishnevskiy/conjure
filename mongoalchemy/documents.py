from .base import BaseDocument, DocumentMeta, ObjectIdField
from .exceptions import OperationError
from .query import Query
import pymongo

class Document(BaseDocument):
    __metaclass__ = DocumentMeta

    _id = ObjectIdField()
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

        self['_id'] = object_id

    def delete(self, safe=False):
        object_id = self._fields['_id'].to_mongo(self._id)

        try:
            self.__class__.objects.filter_by(_id=object_id).delete(safe=safe)
        except pymongo.errors.OperationFailure, err:
            raise OperationError(unicode(err))

    def reload(self):
        doc = self.__class__.objects.filter_by(_id=self._id)._one()
        
        for field in self._fields:
            setattr(self, field, self._fields[field].to_python(doc.get(field)))

    @classmethod 
    def drop_collection(cls):
        cls.objects._collection.drop()

class EmbeddedDocument(BaseDocument):
    __metaclass__ = DocumentMeta
    _meta = {'embedded': True}