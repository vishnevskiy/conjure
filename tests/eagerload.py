import unittest
from mongoalchemy.documents import Document
from mongoalchemy.fields import StringField, ReferenceField, IntegerField

class EagerloadTest(unittest.TestCase):
    def test_eagerload(self):
        class User(Document):
               name = StringField()
               age = IntegerField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)

        User.drop_collection()
        BlogPost.drop_collection()

        author1 = User(name='Test User #1')
        author1.save()

        author2 = User(name='Test User #2')
        author2.save()

        post1 = BlogPost(content='Test Post #1')
        post1.author = author1
        post1.save()

        post2 = BlogPost(content='Test Post #2')
        post2.author = author2
        post2.save()

        post3 = BlogPost(content='Test Post #3')
        post3.author = author1
        post3.save()

        post = BlogPost.objects.eagerload(BlogPost.author).one()

        self.assertEqual(author1, post._data['author'])

        for post in BlogPost.objects.eagerload(BlogPost.author, [User.name]):
            self.assertEqual(type(post._data['author']), User)

        User.drop_collection()
        BlogPost.drop_collection()