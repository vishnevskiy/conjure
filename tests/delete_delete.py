from mongoalchemy import fields, documents
from unittest import TestCase

class Settings(documents.EmbeddedDocument):
    sound = fields.BooleanField(default=True)

class User(documents.Document):
    id = fields.ObjectIdField(primary_key=True)
    username = fields.CharField('username', max_length=5)
    friends = fields.ObjectIdField('friends', multi=True)
    guilds = fields.ObjectIdField('test', multi=True)
    settings = Settings
    
    class Meta:
        indexes = ['username', '-friends']

class QueryBuilderTest(TestCase):
    def test_delete(self):
        query = User.objects.filter(User.username == 'stanislav', ~User.friends.in_(2, 3, 4))
        query |= User.objects.filter(User.guilds.in_(2, 3, 4), User.settings.sound == False)
        self.assertEqual(query.delete(), {'$or': [{'username': 'stanislav', 'friends': {'$nin': [2, 3, 4]}}, {'test': {'$in': [2, 3, 4]}, '???': False}]})
