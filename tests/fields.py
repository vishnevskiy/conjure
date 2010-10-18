import unittest
from mongoalchemy import documents, fields, exceptions
import datetime

class FieldTest(unittest.TestCase):
    def test_default_values(self):
        class User(documents.Document):
            _id = fields.StringField(default=lambda: 'test')
            name = fields.StringField()
            age = fields.IntegerField(default=30)

        user = User(name='Test User')
        self.assertEqual(user._data['age'], 30)
        self.assertEqual(user._data['_id'], 'test')

    def test_required_values(self):
        class User(documents.Document):
            _id = fields.ObjectIdField()
            name = fields.StringField(required=True)
            age = fields.IntegerField(required=True)
            userid = fields.StringField()

        user = User(name='Test User')
        self.assertRaises(exceptions.ValidationError, user.validate)
        user = User(age=30)
        self.assertRaises(exceptions.ValidationError, user.validate)

    def test_object_id_validation(self):
        class User(documents.Document):
            _id = fields.ObjectIdField()
            name = fields.StringField()

        user = User(name='Test User')
        self.assertEqual(user._id, None)

        user._id = 47
        self.assertRaises(exceptions.ValidationError, user.validate)

        user._id = 'abc'
        self.assertRaises(exceptions.ValidationError, user.validate)

        user._id = '497ce96f395f2f052a494fd4'
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

        user.name = 'Shorter name'
        user.validate()

    def test_int_validation(self):
        class User(documents.Document):
            _id = fields.ObjectIdField()
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
            _id = fields.ObjectIdField()
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
            _id = fields.ObjectIdField()
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
            _id = fields.ObjectIdField()
            time = fields.DateTimeField()

        log = LogEntry()
        log.time = datetime.datetime.now()
        log.validate()

        log.time = -1
        self.assertRaises(exceptions.ValidationError, log.validate)
        log.time = '1pm'
        self.assertRaises(exceptions.ValidationError, log.validate)

    def test_list_validation(self):
        class Comment(documents.Document):
            content = fields.StringField()

            class Meta:
                embedded = True

        class BlogPost(documents.Document):
            _id = fields.ObjectIdField()
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
            _id = fields.ObjectIdField()
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
        class Comment(documents.Document):
            content = fields.StringField()

            class Meta:
                embedded = True

        class PersonPreferences(documents.Document):
            food = fields.StringField(required=True)
            number = fields.IntegerField()

            class Meta:
                embedded = True

        class Person(documents.Document):
            _id = fields.ObjectIdField()
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
        class User(documents.Document):
            name = fields.StringField()

            class Meta:
                embedded = True

        class PowerUser(User):
            power = fields.IntegerField()

        class BlogPost(documents.Document):
            _id = fields.ObjectIdField()
            content = fields.StringField()
            author = fields.EmbeddedDocumentField(User)

        post = BlogPost(content='What I did today...')
        post.author = User(name='Test User')
        post.author = PowerUser(name='Test User', power=47)

#    def test_reference_validation(self):
#        """Ensure that invalid docment objects cannot be assigned to reference
#        fields.
#        """
#        class User(Document):
#            name = StringField()
#
#        class BlogPost(Document):
#            content = StringField()
#            author = ReferenceField(User)
#
#        User.drop_collection()
#        BlogPost.drop_collection()
#
#        self.assertRaises(ValidationError, ReferenceField, EmbeddedDocument)
#
#        user = User(name='Test User')
#
#        # Ensure that the referenced object must have been saved
#        post1 = BlogPost(content='Chips and gravy taste good.')
#        post1.author = user
#        self.assertRaises(ValidationError, post1.save)
#
#        # Check that an invalid object type cannot be used
#        post2 = BlogPost(content='Chips and chilli taste good.')
#        post1.author = post2
#        self.assertRaises(ValidationError, post1.validate)
#
#        user.save()
#        post1.author = user
#        post1.save()
#
#        post2.save()
#        post1.author = post2
#        self.assertRaises(ValidationError, post1.validate)
#
#        User.drop_collection()
#        BlogPost.drop_collection()
#
#    def test_list_item_dereference(self):
#        """Ensure that DBRef items in ListFields are dereferenced.
#        """
#        class User(Document):
#            name = StringField()
#
#        class Group(Document):
#            members = ListField(ReferenceField(User))
#
#        User.drop_collection()
#        Group.drop_collection()
#
#        user1 = User(name='user1')
#        user1.save()
#        user2 = User(name='user2')
#        user2.save()
#
#        group = Group(members=[user1, user2])
#        group.save()
#
#        group_obj = Group.objects.first()
#
#        self.assertEqual(group_obj.members[0].name, user1.name)
#        self.assertEqual(group_obj.members[1].name, user2.name)
#
#        User.drop_collection()
#        Group.drop_collection()
#
#    def test_recursive_reference(self):
#        """Ensure that ReferenceFields can reference their own documents.
#        """
#        class Employee(Document):
#            name = StringField()
#            boss = ReferenceField('self')
#
#        bill = Employee(name='Bill Lumbergh')
#        bill.save()
#        peter = Employee(name='Peter Gibbons', boss=bill)
#        peter.save()
#
#        peter = Employee.objects.with_id(peter.id)
#        self.assertEqual(peter.boss, bill)
#
#    def test_undefined_reference(self):
#        """Ensure that ReferenceFields may reference undefined Documents.
#        """
#        class Product(Document):
#            name = StringField()
#            company = ReferenceField('Company')
#
#        class Company(Document):
#            name = StringField()
#
#        ten_gen = Company(name='10gen')
#        ten_gen.save()
#        mongodb = Product(name='MongoDB', company=ten_gen)
#        mongodb.save()
#
#        obj = Product.objects(company=ten_gen).first()
#        self.assertEqual(obj, mongodb)
#        self.assertEqual(obj.company, ten_gen)
#
#    def test_reference_query_conversion(self):
#        """Ensure that ReferenceFields can be queried using objects and values
#        of the type of the primary key of the referenced object.
#        """
#        class Member(Document):
#            user_num = IntField(primary_key=True)
#
#        class BlogPost(Document):
#            title = StringField()
#            author = ReferenceField(Member)
#
#        Member.drop_collection()
#        BlogPost.drop_collection()
#
#        m1 = Member(user_num=1)
#        m1.save()
#        m2 = Member(user_num=2)
#        m2.save()
#
#        post1 = BlogPost(title='post 1', author=m1)
#        post1.save()
#
#        post2 = BlogPost(title='post 2', author=m2)
#        post2.save()
#
#        post = BlogPost.objects(author=m1).first()
#        self.assertEqual(post.id, post1.id)
#
#        post = BlogPost.objects(author=m2).first()
#        self.assertEqual(post.id, post2.id)
#
#        Member.drop_collection()
#        BlogPost.drop_collection()
#
#    def test_generic_reference(self):
#        """Ensure that a GenericReferenceField properly dereferences items.
#        """
#        class Link(Document):
#            title = StringField()
#            meta = {'allow_inheritance': False}
#
#        class Post(Document):
#            title = StringField()
#
#        class Bookmark(Document):
#            bookmark_object = GenericReferenceField()
#
#        Link.drop_collection()
#        Post.drop_collection()
#        Bookmark.drop_collection()
#
#        link_1 = Link(title="Pitchfork")
#        link_1.save()
#
#        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
#        post_1.save()
#
#        bm = Bookmark(bookmark_object=post_1)
#        bm.save()
#
#        bm = Bookmark.objects(bookmark_object=post_1).first()
#
#        self.assertEqual(bm.bookmark_object, post_1)
#        self.assertTrue(isinstance(bm.bookmark_object, Post))
#
#        bm.bookmark_object = link_1
#        bm.save()
#
#        bm = Bookmark.objects(bookmark_object=link_1).first()
#
#        self.assertEqual(bm.bookmark_object, link_1)
#        self.assertTrue(isinstance(bm.bookmark_object, Link))
#
#        Link.drop_collection()
#        Post.drop_collection()
#        Bookmark.drop_collection()
#
#    def test_generic_reference_list(self):
#        """Ensure that a ListField properly dereferences generic references.
#        """
#        class Link(Document):
#            title = StringField()
#
#        class Post(Document):
#            title = StringField()
#
#        class User(Document):
#            bookmarks = ListField(GenericReferenceField())
#
#        Link.drop_collection()
#        Post.drop_collection()
#        User.drop_collection()
#
#        link_1 = Link(title="Pitchfork")
#        link_1.save()
#
#        post_1 = Post(title="Behind the Scenes of the Pavement Reunion")
#        post_1.save()
#
#        user = User(bookmarks=[post_1, link_1])
#        user.save()
#
#        user = User.objects(bookmarks__all=[post_1, link_1]).first()
#
#        self.assertEqual(user.bookmarks[0], post_1)
#        self.assertEqual(user.bookmarks[1], link_1)
#
#        Link.drop_collection()
#        Post.drop_collection()
#        User.drop_collection()

    def test_choices_validation(self):
        class Shirt(documents.Document):
            _id = fields.ObjectIdField()
            size = fields.StringField(max_length=3, choices=('S','M','L','XL','XXL'))

        Shirt.drop_collection()

        shirt = Shirt()
        shirt.validate()

        shirt.size = 'S'
        shirt.validate()

        shirt.size = 'XS'
        self.assertRaises(exceptions.ValidationError, shirt.validate)

        Shirt.drop_collection()

if __name__ == '__main__':
    unittest.main()