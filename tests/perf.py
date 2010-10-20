import unittest
from time import time
from mongoalchemy.documents import Document, EmbeddedDocument
from mongoalchemy.fields import StringField, BooleanField, ListField, EmbeddedDocumentField
from mongoalchemy.base import ObjectIdField
from bson.objectid import ObjectId
import pymongo
from _mongomapper import Document as MMDocument

class PerformanceTest(unittest.TestCase):
    def setUp(self):
        conn = pymongo.connection.Connection()
        self.db = conn.main
        self.collection = self.db.users

        self.collection.drop()

        class Settings(EmbeddedDocument):
            sound = BooleanField(default=False)

        self.Settings = Settings

        class User(Document):
            username = StringField(required=True)
            display_name = StringField(required=True)
            staff = BooleanField(default=False)
            friends = ListField(ObjectIdField())
            settings = EmbeddedDocumentField(Settings)

        self.User = User

        class User2(MMDocument):
            db = self.db
            collection = 'users'

            required = {
                'staff': False,
                'settings': {
                    'sound': False
                }
            }

        self.User2 = User2

    def test_serializtion(self):
        Settings = self.Settings
        User = self.User
        User2 = self.User2
        
        REPEAT = 10000

        start = time()

        for n in xrange(REPEAT):
            User(username='stanislav', display_name='Stanislav', staff=True,
                     friends=[ObjectId(), ObjectId()], settings=Settings()).save()

        end = time()
        print 'MongoAlchemy', end - start

        start = time()

        for n in xrange(REPEAT):
            User2(username='stanislav', display_name='Stanislav', staff=True,
                     friends=[ObjectId(), ObjectId()], settings={'sound': True}).save()

        end = time()
        print 'MongoMapper', end - start

        start = time()
        for n in xrange(REPEAT):
            self.collection.insert({
                'username': 'stanislav',
                'display_name': 'Stanislav',
                'staff': True,
                'friends': [ObjectId(), ObjectId()],
                'settings': {
                    'sound': False
                }
            })

        end = time()
        print 'PyMongo', end - start

    def test_deserialize(self):
        User = self.User
        User2 = self.User2

        REPEAT = 25000

        for n in xrange(REPEAT):
            self.collection.insert({
                'username': 'stanislav',
                'display_name': 'Stanislav',
                'staff': True,
                'friends': [ObjectId(), ObjectId()],
                'settings': {
                    'sound': False
                }
            })

        self.assertEqual(self.collection.find({'username': 'stanislav'}).count(), REPEAT)

        start = time()

        User.objects.filter(User.username == 'stanislav').all()

        end = time()
        print 'MongoAlchemy', end - start

        start = time()

        User2.documents.find({'username': 'stanislav'}).all()

        end = time()
        print 'MongoMapper', end - start

        start = time()

        list(self.collection.find({'username': 'stanislav'}))

        end = time()
        print 'PyMongo', end - start

    def test_atomic_update(self):
        User = self.User
        User2 = self.User2

        REPEAT = 10

        for n in xrange(REPEAT):
            self.collection.insert({
                'username': 'stanislav',
                'display_name': 'Stanislav',
                'staff': True,
                'friends': [ObjectId(), ObjectId()],
                'settings': {
                    'sound': False
                }
            })

        self.assertEqual(self.collection.find({'username': 'stanislav'}).count(), REPEAT)

        start = time()

        for n in xrange(REPEAT):
            User.objects.filter(User.username == 'stanislav').update(User.display_name.set('Test') & User.staff.set(False))
            
        end = time()
        print 'MongoAlchemy', end - start

        start = time()

        for n in xrange(REPEAT):
            User2.documents.update({'username': 'stanislav'}, {'$set': {'staff': False, 'dislay_name': 'Test'}})

        end = time()
        print 'MongoMapper', end - start

        start = time()

        for n in xrange(REPEAT):
            self.collection.update({'username': 'stanislav'}, {'$set': {'staff': False, 'dislay_name': 'Test'}})

        end = time()
        print 'PyMongo', end - start

    def tearDown(self):
        self.collection.drop()