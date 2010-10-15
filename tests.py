from mongoalchemy import fields, documents, constants
from unittest import TestCase

class Settings(documents.EmbeddedDocument):
    sound = fields.BooleanField(default=True)

class User(documents.Document):
    _id = fields.ObjectIdField('_id', primary_key=True)
    username = fields.CharField('username', max_length=5)
    friends = fields.ObjectIdField('friends', multi=True)
    guilds = fields.ObjectIdField('guilds', multi=True)
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

        # slice
        self.assertEqual(User.username[5], {'username': {'$slice': 5}})
        self.assertEqual(User.username[5:-1], {'username': {'$slice': [5, -1]}})

        # pop
        self.assertEqual(User.username.pop(), {'$pop': {'username': 1}})
        self.assertEqual(User.username.popleft(), {'$pop': {'username': -1}})

        # addToset
        self.assertEqual(User.username | 5, {'$addToSet': {'username': 5}})

        # set
        self.assertEqual(User.username.set(5), {'$set': {'username': 5}})

        # unset
        self.assertEqual(User.username.unset(), {'$unset': {'username': 1}})

        # inc/dec
        self.assertEqual(User.username.inc(), {'$inc': {'username': 1}})
        self.assertEqual(User.username.inc(5), {'$inc': {'username': 5}})
        self.assertEqual(User.username + 5, {'$inc': {'username': 5}})
        self.assertEqual(User.username.dec(), {'$inc': {'username': -1}})
        self.assertEqual(User.username.dec(5), {'$inc': {'username': -5}})
        self.assertEqual(User.username - 5, {'$inc': {'username': -5}})

        # push
        self.assertEqual(User.guilds + 5, {'$push': {'guilds': 5}})
        self.assertEqual(User.guilds.push(5), {'$push': {'guilds': 5}})

        # pushAll
        self.assertEqual(User.guilds + [1, 5], {'$pushAll': {'guilds': [1, 5]}})
        self.assertEqual(User.guilds.push_all([1, 5]), {'$pushAll': {'guilds': [1, 5]}})

        # pull
        self.assertEqual(User.guilds - 5, {'$pull': {'guilds': 5}})
        self.assertEqual(User.guilds.pull(5), {'$pull': {'guilds': 5}})

        # pullAll
        self.assertEqual(User.guilds - [1, 5], {'$pullAll': {'guilds': [1, 5]}})
        self.assertEqual(User.guilds.pull_all([1, 5]), {'$pullAll': {'guilds': [1, 5]}})

    def test_update(self):
        self.assertEqual(User.setAll({'x': 2, 'y': 3}) & User.username.set(2), {'$set': {'y': 3, 'x': 2, 'username': 2}})
        self.assertEqual(User.guilds + [2, 5] & User.guilds + [2, 8], {'$pushAll': {'guilds': [2, 5, 2, 8]}})
        self.assertEqual((User.guilds | 2) & User.guilds + 5, {'$push': {'guilds': 5}, '$addToSet': {'guilds': 2}})
        self.assertEqual(User.username.set('wamb') & User.username.set('stan'), {'$set': {'username': 'stan'}})
        self.assertEqual(User.guilds.unset() & User.username.set('stan'), {'$unset': {'guilds': 1}, '$set': {'username': 'stan'}})
        self.assertEqual(User._id + 2 & User._id - 5, {'$inc': {'_id': -3}})
        self.assertEqual(User._id + 2 & User._id.dec(2), {'$inc': {'_id': 0}})

        # realistic test
        update = User.username.set(2) & User.guilds.push(5) & User.friends.pop()
        update &= User.friends.push(10)
        
        self.assertEqual(update, {'$set': {'username': 2}, '$push': {'friends': 10, 'guilds': 5}, '$pop': {'friends': 1}})
