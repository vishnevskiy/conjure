from mongoalchemy.queryset import Q
from mongoalchemy import expressions

class Document(object):
    objects = Q()

    @staticmethod
    def setAll(values):
        return expressions.UpdateExpression({'$set': values})

class EmbeddedDocument(Document):
    pass