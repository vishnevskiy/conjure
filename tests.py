from mongoalchemy import fields, documents, constants
from unittest import TestCase
import datetime
import bson

class Settings(documents.Document):
    sound = fields.BooleanField(default=True)

    class Meta:
        embedded = True

class User(documents.Document):
    _id = fields.ObjectIdField()
    username = fields.StringField()
    email = fields.EmailField()
    following = fields.ListField(fields.ObjectIdField())
    followers = fields.ListField(fields.ObjectIdField())
    age = fields.IntegerField()
    settings = fields.EmbeddedDocumentField(Settings)
    joined_on = fields.DateTimeField(default=datetime.datetime.now)
    
    def __unicode__(self):
        return self.username

    class Meta:
        indexes = ['username', '-followers']

class ExpressionTest(TestCase):
    def test_basic(self):
        # eq
        self.assertEqual(User.username == 'stanislav', {'username': 'stanislav'})
        self.assertEqual(~(User.username != 'stanislav'), {'username': 'stanislav'})

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
        self.assertEqual((User.username > 5) & (User.username < 10), {'username': {'$gt': 5, '$lt': 10}})
        self.assertEqual(~(User.username < 5), {'username': {'$gt': 5}})

        # gte
        self.assertEqual(User.username >= 5, {'username': {'$gte': 5}})
        self.assertEqual(~(User.username <= 5), {'username': {'$gte': 5}})

        # mod
        self.assertEqual(User.age % 10 == 0, {'age': {'$mod': [10, 0]}})
        self.assertEqual(User.age % 10 != 0, {'age': {'$not': {'$mod': [10, 0]}}})
        self.assertEqual(~(User.age % 10 == 0), {'age': {'$not': {'$mod': [10, 0]}}})
        
        # in
        self.assertEqual(User.followers.in_([2, 5]), {'followers': {'$in': [2 ,5]}})
        self.assertEqual(~User.followers.nin([2, 5]), {'followers': {'$in': [2 ,5]}})

        # nin
        self.assertEqual(User.followers.nin([2, 5]), {'followers': {'$nin': [2 ,5]}})
        self.assertEqual(~User.followers.in_([2, 5]), {'followers': {'$nin': [2 ,5]}})

        # all
        self.assertEqual(User.followers.all([2, 5]), {'followers': {'$all': [2 ,5]}})
        self.assertEqual(~User.followers.all([2, 5]), {'followers': {'$not': {'$all': [2 ,5]}}})

        # size
        self.assertEqual(User.followers.size(5), {'followers': {'$size': 5}})
        self.assertEqual(~User.followers.size(5), {'followers': {'$not': {'$size': 5}}})

        # exists
        self.assertEqual(User.followers.exists(), {'followers': {'$exists': True}})
        self.assertEqual(~User.followers.exists(), {'followers': {'$exists': False}})

        # type
        self.assertEqual(User.username.type(constants.STRING), {'username': {'$type': 2}})
        self.assertEqual(~User.username.type(constants.STRING), {'username': {'$not': {'$type': 2}}})

        # where
        self.assertEqual(User.username.where('this.username == "stan"'), {'username': {'$where': 'this.username == "stan"'}})
        self.assertEqual(~User.username.where('this.username == "stan"'), {'username': {'$not': {'$where': 'this.username == "stan"'}}})

        # slice
        self.assertEqual(User.followers[5], {'followers': {'$slice': 5}})
        self.assertEqual(User.followers[5:-1], {'followers': {'$slice': [5, -1]}})

        # pop
        self.assertEqual(User.followers.pop(), {'$pop': {'followers': 1}})
        self.assertEqual(User.followers.popleft(), {'$pop': {'followers': -1}})

        # addToset
        self.assertEqual(User.followers | 5, {'$addToSet': {'followers': 5}})

        # set
        self.assertEqual(User.username.set('stanislav'), {'$set': {'username': 'stanislav'}})

        # unset
        self.assertEqual(User.username.unset(), {'$unset': {'username': 1}})

        # inc/dec
        self.assertEqual(User.age.inc(), {'$inc': {'age': 1}})
        self.assertEqual(User.age.inc(5), {'$inc': {'age': 5}})
        self.assertEqual(User.age + 5, {'$inc': {'age': 5}})
        self.assertEqual(User.age.dec(), {'$inc': {'age': -1}})
        self.assertEqual(User.age.dec(5), {'$inc': {'age': -5}})
        self.assertEqual(User.age - 5, {'$inc': {'age': -5}})

        # push
        self.assertEqual(User.followers + 5, {'$push': {'followers': 5}})
        self.assertEqual(User.followers.push(5), {'$push': {'followers': 5}})

        # pushAll
        self.assertEqual(User.followers + [1, 5], {'$pushAll': {'followers': [1, 5]}})
        self.assertEqual(User.followers.push_all([1, 5]), {'$pushAll': {'followers': [1, 5]}})

        # pull
        self.assertEqual(User.followers - 5, {'$pull': {'followers': 5}})
        self.assertEqual(User.followers.pull(5), {'$pull': {'followers': 5}})

        # pullAll
        self.assertEqual(User.followers - [1, 5], {'$pullAll': {'followers': [1, 5]}})
        self.assertEqual(User.followers.pull_all([1, 5]), {'$pullAll': {'followers': [1, 5]}})

    def test_merging(self):
        statement = User.followers == 5
        statement |= User.followers == 9
        statement &= ((User.username != 'wamb') | (User.age == 5))

        self.assertEquals(statement, {'$or': [{'followers': 5}, {'followers': 9}, {'username': {'$ne': 'wamb'}}, {'age': 5}]})

    def test_model(self):
        user = User(username='stanislav', email='stanislav@guildwork.com', age=22)
        user.followers.append(bson.objectid.ObjectId())
        user.settings = Settings()
        user.save()

        self.assertEqual(type(user._id), bson.objectid.ObjectId)

        user2 = User.objects.with_id(user._id)

        self.assertEqual(user, user2)

        user.username = 'wambulance'
        user.save()

        self.assertNotEqual(user.username, user2.username)

        user2.reload()

        self.assertEqual(user.username, user2.username)

        User.objects.filter(User._id == user._id).update(User.username.set('stanislav'))

        user.reload()

        self.assertNotEqual(user.username, user2.username)

        user2.reload()

        self.assertEqual(user.username, user2.username)

        self.assertEqual(user.age, 22)

        User.objects.filter_by(_id=user._id).update(User.age - 1)

        user = User.objects.find_one(User._id == user._id)

        self.assertEqual(user.settings.sound, True)

        self.assertEqual(user.age, 21)

        self.assertEqual(User.objects.filter(User._id == user._id).count(), 1)

        user.delete()

        self.assertEqual(User.objects.filter(User._id == user._id).count(), 0)

    def test_update(self):
        self.assertEqual(User.followers + [2, 5] & User.followers + [2, 8], {'$pushAll': {'followers': [2, 5, 2, 8]}})
        self.assertEqual((User.followers | 2) & User.followers + 5, {'$push': {'followers': 5}, '$addToSet': {'followers': 2}})
        self.assertEqual(User.username.set('wamb') & User.username.set('stan'), {'$set': {'username': 'stan'}})
        self.assertEqual(User.followers.unset() & User.username.set('stan'), {'$unset': {'followers': 1}, '$set': {'username': 'stan'}})
        self.assertEqual(User.age + 2 & User.age - 5, {'$inc': {'age': -3}})
        self.assertEqual(User.age + 2 & User.age.dec(2), {'$inc': {'age': 0}})

        # realistic test
        update = User.username.set('stanislav') & User.followers.push(5) & User.following.pop()
        update &= User.following.push(10)

        self.assertEqual(update, {'$set': {'username': 'stanislav'}, '$push': {'following': 10, 'followers': 5}, '$pop': {'following': 1}})
