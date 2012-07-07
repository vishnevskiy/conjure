import unittest
from conjure.documents import Document, EmbeddedDocument
from conjure.fields import StringField, ReferenceField, IntegerField, EmbeddedDocumentField, ListField

class EagerloadTest(unittest.TestCase):
    def test_eagerload(self):
        class User(Document):
               name = StringField()
               age = IntegerField()

        class Comment(EmbeddedDocument):
            by = ReferenceField(User)
            message = StringField()

        class BlogPost(Document):
            content = StringField()
            author = ReferenceField(User)
            comments = ListField(EmbeddedDocumentField(Comment))
            likes = ListField(ReferenceField(User))

        User.drop_collection()
        BlogPost.drop_collection()

        author1 = User(name='Test User #1')
        author1.save()

        author2 = User(name='Test User #2')
        author2.save()

        post1 = BlogPost(content='Test Post #1')
        post1.author = author1
        post1.comments = [Comment(by=author1), Comment(by=author2)]
        post1.save()

        post2 = BlogPost(content='Test Post #2')
        post2.author = author2
        post2.comments = [Comment(by=author1), Comment(by=author2)]
        post2.save()

        post3 = BlogPost(content='Test Post #3')
        post3.author = author1
        post3.likes = [author2]
        post3.save()

        post = BlogPost.objects.eagerload(BlogPost.author).one()

        self.assertEqual(author1, post._data['author'])

        for post in BlogPost.objects.eagerload(BlogPost.author, Comment.by, BlogPost.likes, only=[User.name]):
            self.assertEqual(type(post._data['author']), User)

            for comment in post.comments:
                self.assertEqual(type(comment._data['by']), User)

            for like in post._data['likes']:
                self.assertEqual(type(like), User)

        User.drop_collection()
        BlogPost.drop_collection()