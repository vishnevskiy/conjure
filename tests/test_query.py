import unittest
import pymongo
from datetime import datetime
from conjure import documents, fields, query, exceptions
import bson

class QueryTest(unittest.TestCase):
    def setUp(self):
        class User(documents.Document):
            name = fields.StringField()
            age = fields.IntegerField()

        self.User = User

    def test_initialisation(self):
        User = self.User

        self.assertTrue(isinstance(User.objects, query.Query))
        self.assertEqual(User.objects._collection.name, User._meta['collection'])
        self.assertTrue(isinstance(User.objects._collection, pymongo.collection.Collection))

    def test_find(self):
        User = self.User

        user1 = User(name='User A', age=20)
        user1.save()
        user2 = User(name='User B', age=30)
        user2.save()

        users = User.objects
        self.assertEqual(len(users), 2)
        results = list(users)
        self.assertTrue(isinstance(results[0], User))
        self.assertTrue(isinstance(results[0].id, (bson.objectid.ObjectId, str, unicode)))
        self.assertEqual(results[0].name, 'User A')
        self.assertEqual(results[0].age, 20)
        self.assertEqual(results[1].name, 'User B')
        self.assertEqual(results[1].age, 30)

        users = User.objects.filter(User.age == 20)
        self.assertEqual(len(users), 1)
        user = users.next()
        self.assertEqual(user.name, 'User A')
        self.assertEqual(user.age, 20)

        users = list(User.objects.limit(1))
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].name, 'User A')

        users = list(User.objects.skip(1))
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].name, 'User B')

        user3 = User(name='User C', age=40)
        user3.save()

        users = list(User.objects[:2])
        self.assertEqual(len(users), 2)
        self.assertEqual(users[0].name, 'User A')
        self.assertEqual(users[1].name, 'User B')

        users = list(User.objects[1:])
        self.assertEqual(len(users), 2)
        self.assertEqual(users[0].name, 'User B')
        self.assertEqual(users[1].name, 'User C')

        users = list(User.objects[1:2])
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0].name, 'User B')

        users = list(User.objects[1:1])
        self.assertEqual(len(users), 0)

    def test_find_one(self):
        User = self.User

        user1 = User(name='User A', age=20)
        user1.save()
        user2 = User(name='User B', age=30)
        user2.save()

        user = self.User.objects.first()
        self.assertTrue(isinstance(user, User))
        self.assertEqual(user.name, 'User A')
        self.assertEqual(user.age, 20)

        user = User.objects.filter(User.age == 30).first()
        self.assertEqual(user.name, 'User B')

        user = User.objects.filter(User.age < 30).first()
        self.assertEqual(user.name, 'User A')

        user = User.objects[0]
        self.assertEqual(user.name, 'User A')

        user = User.objects[1]
        self.assertEqual(user.name, 'User B')

        self.assertRaises(IndexError, User.objects.__getitem__, 2)

        user = User.objects.with_id(user1.id)
        self.assertEqual(user.name, "User A")

    def test_find_only_one(self):
        User = self.User

        self.assertRaises(exceptions.DoesNotExist, User.objects.first)

        user1 = User(name='User A', age=20)
        user1.save()
        user2 = User(name='User B', age=30)
        user2.save()

        user = User.objects.first(User.age == 30)
        self.assertEqual(user.name, 'User B')

        user = User.objects.first(User.age < 30)
        self.assertEqual(user.name, 'User A')

    def test_repeated_iteration(self):
        User = self.User

        User(name='User 1', age=20).save()
        User(name='User 2', age=22).save()

        q = self.User.objects
        users1 = [person for person in q]
        users2 = [person for person in q]

        self.assertEqual(users1, users2)

    def test_regex_query_shortcuts(self):
        User = self.User

        user = User(name='Guido van Rossum')
        user.save()

        obj = User.objects.filter(User.name.contains('van')).one()
        self.assertEqual(obj, user)
        obj = User.objects.filter(User.name.contains('Van')).one()
        self.assertEqual(obj, None)

        obj = User.objects.filter(User.name.icontains('Van')).one()
        self.assertEqual(obj, user)

        obj = User.objects.filter(User.name.startswith('Guido')).one()
        self.assertEqual(obj, user)
        obj = User.objects.filter(User.name.startswith('guido')).one()
        self.assertEqual(obj, None)

        obj = User.objects.filter(User.name.istartswith('guido')).one()
        self.assertEqual(obj, user)

        obj = User.objects.filter(User.name.endswith('Rossum')).one()
        self.assertEqual(obj, user)
        obj = User.objects.filter(User.name.endswith('rossuM')).one()
        self.assertEqual(obj, None)

        obj = User.objects.filter(User.name.iendswith('rossuM')).one()
        self.assertEqual(obj, user)

    def test_filter_chaining(self):
        class BlogPost(documents.Document):
            title = fields.StringField()
            is_published = fields.BooleanField()
            published_date = fields.DateTimeField()

        blog_post_1 = BlogPost(title='Blog Post #1', is_published=True, published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title='Blog Post #2', is_published=True, published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title='Blog Post #3', is_published=True, published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()

        published_posts = BlogPost.objects.filter_by(is_published=True).filter(BlogPost.published_date < datetime(2010, 1, 7, 0, 0 ,0))

        self.assertEqual(published_posts.count(), 2)

        BlogPost.drop_collection()

    def test_sort1(self):
        class BlogPost(documents.Document):
            title = fields.StringField()
            published_date = fields.DateTimeField()

        BlogPost.drop_collection()

        blog_post_1 = BlogPost(title='Blog Post #1', published_date=datetime(2010, 1, 5, 0, 0 ,0))
        blog_post_2 = BlogPost(title='Blog Post #2', published_date=datetime(2010, 1, 6, 0, 0 ,0))
        blog_post_3 = BlogPost(title='Blog Post #3', published_date=datetime(2010, 1, 7, 0, 0 ,0))

        blog_post_1.save()
        blog_post_2.save()
        blog_post_3.save()

        latest_post = BlogPost.objects.sort('-published_date').first()
        self.assertEqual(latest_post.title, "Blog Post #3")

        first_post = BlogPost.objects.sort('+published_date').first()
        self.assertEqual(first_post.title, "Blog Post #1")

        BlogPost.drop_collection()

    def test_only(self):
        User = self.User

        user = User(name='test', age=25)
        user.save()

        obj = User.objects.only('name').one()
        self.assertEqual(obj.name, user.name)
        self.assertEqual(obj.age, None)

        obj = User.objects.only('age').one()
        self.assertEqual(obj.name, None)
        self.assertEqual(obj.age, user.age)

        obj = User.objects.only('name', 'age').one()
        self.assertEqual(obj.name, user.name)
        self.assertEqual(obj.age, user.age)

        class Employee(User): 
            salary = fields.IntegerField()

        employee = Employee(name='test employee', age=40, salary=30000)
        employee.save()

        obj = Employee.objects.filter_by(id=employee.id).only('salary').one()
        self.assertEqual(obj.salary, employee.salary)
        self.assertEqual(obj.name, None)

    def test_find_embedded(self):
        class User(documents.EmbeddedDocument):
            name = fields.StringField()

        class BlogPost(documents.Document):
            content = fields.StringField()
            author = fields.EmbeddedDocumentField(User)

        BlogPost.drop_collection()

        post = BlogPost(content='Had a good coffee today...')
        post.author = User(name='Test User')
        post.save()

        result = BlogPost.objects.first()
        self.assertTrue(isinstance(result.author, User))
        self.assertEqual(result.author.name, 'Test User')

        BlogPost.drop_collection()

    def test_find_dict_item(self):
        class BlogPost(documents.Document):
            info = fields.DictField()

        BlogPost.drop_collection()

        post = BlogPost(info={'title': 'test'})
        post.save()

        post_obj = BlogPost.objects.filter(BlogPost.info['title'] == 'test').first()
        self.assertEqual(post_obj.id, post.id)

        BlogPost.drop_collection()

    def test_delete(self):
        User = self.User

        User(name='User A', age=20).save()
        User(name='User B', age=30).save()
        User(name='User C', age=40).save()

        self.assertEqual(len(User.objects), 3)

        User.objects.filter(User.age < 30).delete()
        self.assertEqual(len(User.objects), 2)

        User.objects.delete()
        self.assertEqual(len(User.objects), 0)

    def test_update(self):
        class BlogPost(documents.Document):
            title = fields.StringField()
            hits = fields.IntegerField()
            tags = fields.ListField(fields.StringField())

        BlogPost.drop_collection()

        post = BlogPost(title='Test Post', hits=5, tags=['test'])
        post.save()

        BlogPost.objects.update(BlogPost.hits.set(10))
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update_one(BlogPost.hits + 1)
        post.reload()
        self.assertEqual(post.hits, 11)

        BlogPost.objects.update_one(BlogPost.hits - 1)
        post.reload()
        self.assertEqual(post.hits, 10)

        BlogPost.objects.update(BlogPost.tags + 'mongo')
        post.reload()
        self.assertTrue('mongo' in post.tags)

        BlogPost.objects.update_one(BlogPost.tags + ['db', 'nosql'])
        post.reload()
        self.assertTrue('db' in post.tags and 'nosql' in post.tags)

        BlogPost.drop_collection()

    def test_update_pull(self):
        class Comment(documents.EmbeddedDocument):
            content = fields.StringField()

        class BlogPost(documents.Document):
            slug = fields.StringField()
            comments = fields.ListField(fields.EmbeddedDocumentField(Comment))

        comment1 = Comment(content='test1')
        comment2 = Comment(content='test2')

        post = BlogPost(slug='test', comments=[comment1, comment2])
        post.save()
        self.assertTrue(comment2 in post.comments)

        BlogPost.objects.filter_by(slug='test').update(BlogPost.comments - (Comment.content == 'test2'))
        post.reload()
        
        self.assertTrue(comment2 not in post.comments)

    def test_sort2(self):
        User = self.User

        User(name='User A', age=20).save()
        User(name='User B', age=40).save()
        User(name='User C', age=30).save()

        names = [p.name for p in User.objects.sort('-age')]
        self.assertEqual(names, ['User B', 'User C', 'User A'])

        names = [p.name for p in User.objects.sort('+age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        names = [p.name for p in User.objects.sort('age')]
        self.assertEqual(names, ['User A', 'User C', 'User B'])

        ages = [p.age for p in User.objects.sort('-name')]
        self.assertEqual(ages, [30, 40, 20])

    def test_query_value_conversion(self):
        User = self.User
        
        class BlogPost(documents.Document):
            author = fields.ReferenceField(User)

        BlogPost.drop_collection()

        user = User(name='test', age=30)
        user.save()

        post = BlogPost(author=user)
        post.save()

        post_obj = BlogPost.objects.filter(BlogPost.author == user).first()
        self.assertEqual(post.id, post_obj.id)

        post_obj = BlogPost.objects.filter(BlogPost.author.in_([user])).first()
        self.assertEqual(post.id, post_obj.id)

        BlogPost.drop_collection()

    def test_update_value_conversion(self):
        User = self.User

        class Group(documents.Document):
            members = fields.ListField(fields.ReferenceField(User))

        Group.drop_collection()

        user1 = User(name='user1')
        user1.save()
        user2 = User(name='user2')
        user2.save()

        group = Group()
        group.save()

        Group.objects.filter(Group.id == group.id).update(Group.members.set([user1, user2]))
        group.reload()

        self.assertTrue(len(group.members) == 2)
        self.assertEqual(group.members[0].name, user1.name)
        self.assertEqual(group.members[1].name, user2.name)

        Group.drop_collection()

    def test_bulk(self):
        class BlogPost(documents.Document):
            title = fields.StringField()

        BlogPost.drop_collection()

        post_1 = BlogPost(title='Post #1')
        post_2 = BlogPost(title='Post #2')
        post_3 = BlogPost(title='Post #3')
        post_4 = BlogPost(title='Post #4')
        post_5 = BlogPost(title='Post #5')

        post_1.save()
        post_2.save()
        post_3.save()
        post_4.save()
        post_5.save()

        ids = [post_1.id, post_2.id, post_5.id]
        objects = BlogPost.objects.in_bulk(ids)

        self.assertEqual(len(objects), 3)

        self.assertTrue(post_1.id in objects)
        self.assertTrue(post_2.id in objects)
        self.assertTrue(post_5.id in objects)

        self.assertTrue(objects[post_1.id].title == post_1.title)
        self.assertTrue(objects[post_2.id].title == post_2.title)
        self.assertTrue(objects[post_5.id].title == post_5.title)

        BlogPost.drop_collection()

    def test_chain_regex(self):
        class TextHolder(documents.Document):
            data = fields.StringField()
            
        TextHolder.drop_collection()

        text1 = TextHolder(data='Hello')
        text2 = TextHolder(data='hello')
        
        text1.save()
        text2.save()

        obs = TextHolder.objects.filter(TextHolder.data.icontains('hello'))

        obs = obs.filter(TextHolder.data.icontains('ello'))

        self.assertEqual(len(obs), 2)

    def tearDown(self):
        self.User.drop_collection()

if __name__ == '__main__':
    unittest.main()
