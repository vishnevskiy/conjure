from mongoalchemy.queryset import Q
from mongoalchemy import expressions

class Document(object):
    objects = Q()
    
class EmbeddedDocument(object):
    pass