from mongoalchemy import expressions, queryset

class BaseDocument(object):
    @staticmethod
    def setAll(values):
        return expressions.UpdateExpression({'$set': values})

class Document(BaseDocument):
    objects = queryset.Manager()

class EmbeddedDocument(BaseDocument):
    pass