from mongoalchemy import fields, documents, constants
from unittest import TestCase

class Settings(documents.EmbeddedDocument):
    sound = fields.BooleanField(default=True)

class User(documents.Document):
    _id = fields.ObjectIdField(primary_key=True)
    username = fields.CharField('username', max_length=5)
    friends = fields.ObjectIdField('friends', multi=True)
    guilds = fields.ObjectIdField('test', multi=True)
    settings = Settings
    
    class Meta:
        indexes = ['username', '-friends']

class ExpressionTest(TestCase):
    def test_basic(self):
        # eq
        self.assertEqual(User.username == 5, {'username': 5})
        self.assertEqual(~(User.username != 5), {'username': 5})

        # ne
        self.assertEqual(User.username != 5, {'username': {'$ne': 5}})
        self.assertEqual(~(User.username == 5), {'username': {'$ne': 5}})

        # lt
        self.assertEqual(User.username < 5, {'username': {'$lt': 5}})
        self.assertEqual(~(User.username > 5), {'username': {'$lt': 5}})

        # lte
        self.assertEqual(User.username <= 5, {'username': {'$lte': 5}})
        self.assertEqual(~(User.username >= 5), {'username': {'$lte': 5}})

        # gt
        self.assertEqual(User.username > 5, {'username': {'$gt': 5}})
        self.assertEqual(~(User.username < 5), {'username': {'$gt': 5}})

        # gte
        self.assertEqual(User.username >= 5, {'username': {'$gte': 5}})
        self.assertEqual(~(User.username <= 5), {'username': {'$gte': 5}})

        # mod
        self.assertEqual(User.username % 10 == 0, {'username': {'$mod': [10, 0]}})
        self.assertEqual(User.username % 10 != 0, {'username': {'$not': {'$mod': [10, 0]}}})
        self.assertEqual(~(User.username % 10 == 0), {'username': {'$not': {'$mod': [10, 0]}}})
        
        # in
        self.assertEqual(User.username.in_([2, 5]), {'username': {'$in': [2 ,5]}})
        self.assertEqual(~User.username.nin([2, 5]), {'username': {'$in': [2 ,5]}})

        # nin
        self.assertEqual(User.username.nin([2, 5]), {'username': {'$nin': [2 ,5]}})
        self.assertEqual(~User.username.in_([2, 5]), {'username': {'$nin': [2 ,5]}})

        # all
        self.assertEqual(User.username.all([2, 5]), {'username': {'$all': [2 ,5]}})
        self.assertEqual(~User.username.all([2, 5]), {'username': {'$not': {'$all': [2 ,5]}}})

        # size
        self.assertEqual(User.username.size(5), {'username': {'$size': 5}})
        self.assertEqual(~User.username.size(5), {'username': {'$not': {'$size': 5}}})

        # exists
        self.assertEqual(User.username.exists(), {'username': {'$exists': True}})
        self.assertEqual(~User.username.exists(), {'username': {'$exists': False}})

        # type
        self.assertEqual(User.username.type(constants.ARRAY), {'username': {'$type': 4}})
        self.assertEqual(~User.username.type(constants.ARRAY), {'username': {'$not': {'$type': 4}}})

        # where
        self.assertEqual(User.username.where('this.username == 5'), {'username': {'$where': 'this.username == 5'}})
        self.assertEqual(~User.username.where('this.username == 5'), {'username': {'$not': {'$where': 'this.username == 5'}}})
