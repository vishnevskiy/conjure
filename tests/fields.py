from conjure import documents, fields, exceptions
import unittest
import datetime

class FieldTest(unittest.TestCase):
    def test_default_values(self):
        class User(documents.Document):
            _id = fields.StringField(default=lambda: 'test')
            name = fields.StringField()
            age = fields.IntegerField(default=30)

        user = User(name='Test User')
        self.assertEqual(user.age, 30)
        self.assertEqual(user._id, 'test')

    def test_required_values(self):
        class User(documents.Document):
            name = fields.StringField(required=True)
            age = fields.IntegerField(required=True)
            userid = fields.StringField()

        user = User(name='Test User')
        self.assertRaises(exceptions.ValidationError, user.validate)
        user = User(age=30)
        self.assertRaises(exceptions.ValidationError, user.validate)

    def test_object_id_validation(self):
        class User(documents.Document):
            name = fields.StringField()

        user = User(name='Test User')
        self.assertEqual(user.id, None)

        user.id = 47
        self.assertRaises(exceptions.ValidationError, user.validate)

        user.id = 'abc'
        self.assertRaises(exceptions.ValidationError, user.validate)

        user.id = '497ce96f395f2f052a494fd4'
        user.validate()

    def test_string_validation(self):
        class User(documents.Document):
            _id = fields.StringField(r'[0-9a-z_]+$')
            name = fields.StringField(max_length=20)

        user = User(name=34)
        self.assertRaises(exceptions.ValidationError, user.validate)

        user = User(_id='test.User')
        self.assertRaises(exceptions.ValidationError, user.validate)

        user._id = 'test_user'
        self.assertEqual(user._id, 'test_user')
        user.validate()

        user = User(name='Name that is more than twenty characters')
        self.assertRaises(exceptions.ValidationError, user.validate)

        user._id = '1'
        user.name = 'Shorter name'
        user.validate()

    def test_int_validation(self):
        class User(documents.Document):
            age = fields.IntegerField(min_value=0, max_value=110)

        user = User()
        user.age = 50
        user.validate()

        user.age = -1
        self.assertRaises(exceptions.ValidationError, user.validate)
        user.age = 120
        self.assertRaises(exceptions.ValidationError, user.validate)
        user.age = 'ten'
        self.assertRaises(exceptions.ValidationError, user.validate)

    def test_float_validation(self):
        class User(documents.Document):
            height = fields.FloatField(min_value=0.1, max_value=3.5)

        user = User()
        user.height = 1.89
        user.validate()

        user.height = '2.0'
        self.assertRaises(exceptions.ValidationError, user.validate)
        user.height = 0.01
        self.assertRaises(exceptions.ValidationError, user.validate)
        user.height = 4.0
        self.assertRaises(exceptions.ValidationError, user.validate)

    def test_boolean_validation(self):
        class User(documents.Document):
            admin = fields.BooleanField()

        user = User()
        user.admin = True
        user.validate()

        user.admin = 2
        self.assertRaises(exceptions.ValidationError, user.validate)
        user.admin = 'Yes'
        self.assertRaises(exceptions.ValidationError, user.validate)

    def test_datetime_validation(self):
        class LogEntry(documents.Document):
            time = fields.DateTimeField()

        log = LogEntry()
        log.time = datetime.datetime.now()
        log.validate()

        log.time = -1
        self.assertRaises(exceptions.ValidationError, log.validate)
        log.time = '1pm'
        self.assertRaises(exceptions.ValidationError, log.validate)

    def test_list_validation(self):
        class Comment(documents.EmbeddedDocument):
            content = fields.StringField()

        class BlogPost(documents.Document):
            content = fields.StringField()
            comments = fields.ListField(fields.EmbeddedDocumentField(Comment))
            tags = fields.ListField(fields.StringField())

        post = BlogPost(content='Went for a walk today...')
        post.validate()

        post.tags = 'fun'
        self.assertRaises(exceptions.ValidationError, post.validate)
        post.tags = [1, 2]
        self.assertRaises(exceptions.ValidationError, post.validate)

        post.tags = ['fun', 'leisure']
        post.validate()
        post.tags = ('fun', 'leisure')
        post.validate()

        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.validate()

        post.comments = ['a']
        self.assertRaises(exceptions.ValidationError, post.validate)
        post.comments = 'yay'
        self.assertRaises(exceptions.ValidationError, post.validate)

    def test_dict_validation(self):
        class BlogPost(documents.Document):
            info = fields.DictField()

        post = BlogPost()
        post.info = 'my post'
        self.assertRaises(exceptions.ValidationError, post.validate)

        post.info = ['test', 'test']
        self.assertRaises(exceptions.ValidationError, post.validate)

        post.info = {'$title': 'test'}
        self.assertRaises(exceptions.ValidationError, post.validate)

        post.info = {'the.title': 'test'}
        self.assertRaises(exceptions.ValidationError, post.validate)

        post.info = {'title': 'test'}
        post.validate()

    def test_embedded_document_validation(self):
        class Comment(documents.EmbeddedDocument):
            content = fields.StringField()

        class PersonPreferences(documents.EmbeddedDocument):
            food = fields.StringField(required=True)
            number = fields.IntegerField()

        class Person(documents.Document):
            name = fields.StringField()
            preferences = fields.EmbeddedDocumentField(PersonPreferences)

        person = Person(name='Test User')
        person.preferences = 'My Preferences'
        self.assertRaises(exceptions.ValidationError, person.validate)

        person.preferences = Comment(content='Nice blog post...')
        self.assertRaises(exceptions.ValidationError, person.validate)

        person.preferences = PersonPreferences()
        self.assertRaises(exceptions.ValidationError, person.validate)

        person.preferences = PersonPreferences(food='Cheese', number=47)
        self.assertEqual(person.preferences.food, 'Cheese')
        person.validate()

    def test_embedded_document_inheritance(self):
        class User(documents.EmbeddedDocument):
            name = fields.StringField()

        class PowerUser(User):
            power = fields.IntegerField()

        class BlogPost(documents.Document):
            content = fields.StringField()
            author = fields.EmbeddedDocumentField(User)

        post = BlogPost(content='What I did today...')
        post.author = User(name='Test User')
        post.author = PowerUser(name='Test User', power=47)

    def test_reference_validation(self):
        class User(documents.Document):
            name = fields.StringField()

        class BlogPost(documents.Document):
            content = fields.StringField()
            author = fields.ReferenceField(User)

        User.drop_collection()
        BlogPost.drop_collection()

        self.assertRaises(exceptions.ValidationError, fields.ReferenceField, documents.EmbeddedDocument)

        user = User(name='Test User')

        post1 = BlogPost(content='Chips and gravy taste good.')
        post1.author = user
        self.assertRaises(exceptions.ValidationError, post1.save)

        post2 = BlogPost(content='Chips and chilli taste good.')
        post1.author = post2
        self.assertRaises(exceptions.ValidationError, post1.validate)

        user.save()
        post1.author = user
        post1.save()

        post2.save()
        post1.author = post2
        self.assertRaises(exceptions.ValidationError, post1.validate)

        User.drop_collection()
        BlogPost.drop_collection()

    def test_dereference_lazyload_only(self):
        class User(documents.Document):
            name = fields.StringField()
            email = fields.StringField()

        class Group(documents.Document):
            members = fields.ListField(fields.ReferenceField(User, lazyload_only=['name']))

        User.drop_collection()
        Group.drop_collection()

        user1 = User(name='user1', email='user2@google.com')
        user1.save()
        user2 = User(name='user2', email='user2@google.com')
        user2.save()

        group = Group(members=[user1, user2])
        group.save()

        group_obj = Group.objects.first()

        self.assertEqual(group_obj.members[0].email, None)
        self.assertEqual(group_obj.members[1].email, None)

        User.drop_collection()
        Group.drop_collection()

    def test_list_item_dereference(self):
        class User(documents.Document):
            name = fields.StringField()

        class Group(documents.Document):
            members = fields.ListField(fields.ReferenceField(User))

        User.drop_collection()
        Group.drop_collection()

        user1 = User(name='user1')
        user1.save()
        user2 = User(name='user2')
        user2.save()

        group = Group(members=[user1, user2])
        group.save()

        group_obj = Group.objects.first()

        self.assertEqual(group_obj.members[0].name, user1.name)
        self.assertEqual(group_obj.members[1].name, user2.name)

        User.drop_collection()
        Group.drop_collection()

    def test_recursive_reference(self):
        class Employee(documents.Document):
            name = fields.StringField()
            boss = fields.ReferenceField('self')

        bill = Employee(name='Bill Lumbergh')
        bill.save()
        peter = Employee(name='Peter Gibbons', boss=bill)
        peter.save()

        peter = Employee.objects.with_id(peter.id)
        self.assertEqual(peter.boss, bill)

    def test_reference_query_conversion(self):
        class Member(documents.Document):
            user_num = fields.IntegerField()

        class BlogPost(documents.Document):
            title = fields.StringField()
            author = fields.ReferenceField(Member)

        Member.drop_collection()
        BlogPost.drop_collection()

        m1 = Member(user_num=1)
        m1.save()
        m2 = Member(user_num=2)
        m2.save()

        post1 = BlogPost(title='post 1', author=m1)
        post1.save()

        post2 = BlogPost(title='post 2', author=m2)
        post2.save()

        post = BlogPost.objects.filter(BlogPost.author == m1).first()
        self.assertEqual(post.id, post1.id)

        post = BlogPost.objects.filter(BlogPost.author == m2).first()
        self.assertEqual(post.id, post2.id)

        Member.drop_collection()
        BlogPost.drop_collection()

    def test_choices_validation(self):
        class Shirt(documents.Document):
            size = fields.StringField(max_length=3, choices=[('S', 'Small'),('M', 'Medium'), ('L', 'Large')])

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = 'S'
        shirt.validate()

        shirt.size = 'XS'
        self.assertRaises(exceptions.ValidationError, shirt.validate)

        Shirt.drop_collection()

    def test_choices_display(self):
        class Shirt(documents.Document):
            size = fields.StringField(max_length=3, choices=[('S', 'Small'),('M', 'Medium'), ('L', 'Large')])

        shirt = Shirt()
        shirt.size = 'S'

        self.assertEqual(shirt.get_size_display(), 'Small')

    def test_map_field(self):
        class Group(documents.EmbeddedDocument):
            name = fields.StringField()

        class Page(documents.EmbeddedDocument):
            name = fields.StringField()
            position = fields.IntegerField()
            groups = fields.ListField(fields.EmbeddedDocumentField(Group))

        class Wiki(documents.Document):
            pages = fields.MapField(fields.EmbeddedDocumentField(Page))
            tag_cloud = fields.MapField(fields.ListField(fields.StringField()))

        Wiki.drop_collection()

        self.assertEqual(Wiki.pages['home'] << (Page.position % 5 == 2, Page.name == 'Home'),
             {'pages.home.name': 'Home', 'pages.home.position': {'$mod': [5, 2]}})
        self.assertEqual(Wiki.pages['home'] << (Page.position.set(5), Group.name.set('Leader')),
             {'$set': {'pages.home.position': 5, 'pages.home.groups.$.name': u'Leader'}})
        
        self.assertEqual(Wiki.tag_cloud['home'] + 'cat', {'$push': {'tag_cloud.home': u'cat'}})

        page = Page(name='Home', position=1, groups=[Group(name='Leader')])

        wiki = Wiki(pages={'home': page})
        wiki.save()

        self.assertEqual(Wiki.objects.with_id(wiki.id).pages['home']._data, page._data)

        Wiki.drop_collection()
        
if __name__ == '__main__':
    unittest.main()