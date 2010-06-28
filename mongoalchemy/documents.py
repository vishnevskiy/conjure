from mongoalchemy.queryset import Q

class Document(object):
    objects = Q()
    
class EmbeddedDocument(object):
    pass