from conjure import fields, documents
import unittest
import datetime
from conjure.documents import EmbeddedDocument
from conjure.fields import StringField, EmbeddedDocumentField, ListField
import conjure

class Settings(documents.EmbeddedDocument):
    sound = fields.BooleanField(default=True)

class Widget(documents.EmbeddedDocument):
    index = fields.IntegerField()
    
class User(documents.Document):
    username = fields.StringField()
    email = fields.EmailField()
    following = fields.ListField(fields.IntegerField())
    followers = fields.ListField(fields.IntegerField())
    widgets = fields.ListField(fields.EmbeddedDocumentField(Widget))
    age = fields.IntegerField()
    settings = fields.EmbeddedDocumentField(Settings)
    joined_on = fields.DateTimeField(default=datetime.datetime.now)

    def __unicode__(self):
        return self.username

    class Meta:
        indexes = ['username', '-followers']

class SpecTest(unittest.TestCase):
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
        self.assertEqual(User.username.type(conjure.STRING), {'username': {'$type': 2}})
        self.assertEqual(~User.username.type(conjure.STRING), {'username': {'$not': {'$type': 2}}})

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

    def test_or(self):
        spec = User.followers == 2
        spec |= User.followers == 3
        spec |= User.followers == 4
        self.assertEqual(spec, {'$or': [{'followers': 2}, {'followers': 3}, {'followers': 4}]})

    def test_elem_match(self):
        self.assertEqual(User.widgets.match(Widget.index == 5), {'widgets': {'$elemMatch': {'index': 5}}})
        self.assertEqual(User.widgets.match(Widget.index < 2, Widget.index > 5), {'widgets': {'$elemMatch': {'index': {'$lt': 2, '$gt': 5}}}})

    def test_list_embedded(self):
        class Note(EmbeddedDocument):
            text = StringField()

        class Settings(EmbeddedDocument):
            text = StringField()

        class User(EmbeddedDocument):
            notes = ListField(EmbeddedDocumentField(Note))
            settings = EmbeddedDocumentField(Settings)

        self.assertEqual(Note.text == 'test', {'notes.text': 'test'})
        self.assertEqual(Note.text.set('test'), {'$set': {'notes.$.text': 'test'}})
        self.assertEqual(Settings.text.set('test'), {'$set': {'settings.text': 'test'}})

    def test_replace_with(self):
        class Note(EmbeddedDocument):
            text = StringField()

        class User(EmbeddedDocument):
            notes = ListField(EmbeddedDocumentField(Note))

        self.assertEqual(User.notes.replace_with(Note(text='It works!')), {'$set': {'notes.$': {'text': u'It works!'}}})
        self.assertEqual(User.notes % Note(text='It works!'), {'$set': {'notes.$': {'text': u'It works!'}}})

if __name__ == '__main__':
    unittest.main()