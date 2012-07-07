import unittest
import conjure
import json

class JsonTest(unittest.TestCase):
    def test_json(self):
        class User(conjure.Document):
            name = conjure.StringField()
            age = conjure.IntegerField()

        class Comment(conjure.EmbeddedDocument):
            by = conjure.ReferenceField(User)
            message = conjure.StringField()

        class BlogPost(conjure.Document):
            content = conjure.StringField()
            author = conjure.ReferenceField(User)
            comments =  conjure.ListField( conjure.EmbeddedDocumentField(Comment))
            likes =  conjure.ListField(conjure.ReferenceField(User))

        User.drop_collection()
        BlogPost.drop_collection()

        author1 = User(name='Test User #1')
        author1.id = conjure.ObjectId('4ff8c1d20d196d04cc000028')
        author1.save()

        author2 = User(name='Test User #2')
        author2.id = conjure.ObjectId('4ff8c1d20d196d04cc000029')
        author2.save()

        post1 = BlogPost(content='Test Post #1')
        post1.id = conjure.ObjectId('4ff8c1d20d196d04cc000030')
        post1.author = author1
        post1.comments = [Comment(by=author1), Comment(by=author2)]
        post1.save()

        post2 = BlogPost(content='Test Post #2')
        post2.id = conjure.ObjectId('4ff8c1d20d196d04cc000031')
        post2.author = author2
        post2.comments = [Comment(by=author1), Comment(by=author2)]
        post2.likes = [author1]
        post2.save()

        post3 = BlogPost(content='Test Post #3')
        post3.id = conjure.ObjectId('4ff8c1d20d196d04cc000032')
        post3.author = author1
        post3.likes = [author2]
        post3.save()

        self.assertEqual(json.dumps(post2.to_json()), """{"content": "Test Post #2", "author": {"name": "Test User #2", "id": "4ff8c1d20d196d04cc000029"}, "id": "4ff8c1d20d196d04cc000031", "comments": [{"message": "", "by": {"name": "Test User #1", "id": "4ff8c1d20d196d04cc000028"}}, {"message": "", "by": {"name": "Test User #2", "id": "4ff8c1d20d196d04cc000029"}}], "likes": [{"name": "Test User #1", "id": "4ff8c1d20d196d04cc000028"}]}""")

        User.drop_collection()
        BlogPost.drop_collection()