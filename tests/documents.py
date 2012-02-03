import unittest
import datetime
from conjure.documents import Document, EmbeddedDocument
from conjure.fields import StringField, IntegerField, ReferenceField, DateTimeField, EmailField, ListField, EmbeddedDocumentField
from conjure.exceptions import ValidationError
from conjure.utils import Alias
import bson

class DocumentTest(unittest.TestCase):
    def setUp(self):
        class User(Document):
            name = StringField()
            age = IntegerField()

        self.User = User

    def test_definition(self):
        name_field = StringField()
        age_field = IntegerField()

        class User(Document):
            name = name_field
            age = age_field
            non_field = True

        self.assertEqual(User._fields['name'], name_field)
        self.assertEqual(User._fields['age'], age_field)
        self.assertFalse('non_field' in User._fields)
        self.assertTrue('id' in User._fields)
        fields = list(User())
        self.assertTrue('name' in fields and 'age' in fields)
        self.assertFalse(hasattr(Document, '_fields'))

    def test_get_superclasses(self):
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        self.assertEqual(Mammal._superclasses, {'Animal': Animal})

        self.assertEqual(Dog._superclasses, {
            'Animal': Animal,
            'Animal.Mammal': Mammal,
        })

    def test_get_subclasses(self):
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass

        self.assertEqual(Mammal._get_subclasses(), {
            'Animal.Mammal.Dog': Dog,
            'Animal.Mammal.Human': Human
        })

        self.assertEqual(Animal._get_subclasses(), {
            'Animal.Fish': Fish,
            'Animal.Mammal': Mammal,
            'Animal.Mammal.Dog': Dog,
            'Animal.Mammal.Human': Human
        })

    def test_polymorphic_queries(self):
        class Animal(Document): pass
        class Fish(Animal): pass
        class Mammal(Animal): pass
        class Human(Mammal): pass
        class Dog(Mammal): pass
        
        Animal.drop_collection()

        Animal().save()
        Fish().save()
        Mammal().save()
        Human().save()
        Dog().save()

        classes = [obj.__class__ for obj in Animal.objects]
        self.assertEqual(classes, [Animal, Fish, Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Mammal.objects]
        self.assertEqual(classes, [Mammal, Human, Dog])

        classes = [obj.__class__ for obj in Human.objects]
        self.assertEqual(classes, [Human])

        Animal.drop_collection()

    def test_inheritance(self):
        class Employee(self.User):
            salary = IntegerField()

        self.assertTrue('name' in Employee._fields)
        self.assertTrue('salary' in Employee._fields)
        self.assertEqual(Employee._meta['collection'],  self.User._meta['collection'])

        class A(Document): pass
        class B(A): pass
        class C(B): pass

    def test_inherited_collections(self):
        class Drink(Document):
            name = StringField()

        class AlcoholicDrink(Drink):
            meta = {'collection': 'booze'}

        class Drinker(Document):
            drink = ReferenceField(Drink)

        Drink.drop_collection()
        AlcoholicDrink.drop_collection()
        Drinker.drop_collection()

        red_bull = Drink(name='Red Bull')
        red_bull.save()

        programmer = Drinker(drink=red_bull)
        programmer.save()

        beer = AlcoholicDrink(name='Beer')
        beer.save()

        real_person = Drinker(drink=beer)
        real_person.save()

        self.assertEqual(Drinker.objects[0].drink.name, red_bull.name)
        self.assertEqual(Drinker.objects[1].drink.name, beer.name)

    def test_custom_id_field(self):
        class User(Document):
            id = StringField()
            name = StringField()

            username = Alias('id')

        User.drop_collection()

        def create_invalid_user():
            User(name='test').save() 

        self.assertRaises(ValidationError, create_invalid_user)

        class EmailUser(User):
            email = StringField()

        user = User(id='test', name='test user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'test')

        user_son = User.objects._collection.find_one()
        
        self.assertEqual(user_son['_id'], 'test')
        self.assertTrue('username' not in user_son)

        User.drop_collection()

        user = User(id='mongo', name='mongo user')
        user.save()

        user_obj = User.objects.first()
        self.assertEqual(user_obj.id, 'mongo')

        user_son = User.objects._collection.find_one()
        self.assertEqual(user_son['_id'], 'mongo')
        self.assertTrue('username' not in user_son)

        User.drop_collection()

    def test_db_field(self):
        class Date(EmbeddedDocument):
            year = IntegerField(db_field='yr')

        class BlogPost(Document):
            title = StringField()
            author = ReferenceField(self.User, db_field='user_id')
            date = EmbeddedDocumentField(Date)
            slug = StringField()

        BlogPost.drop_collection()

        author = self.User(username='stanislav')
        author.save()

        post1 = BlogPost(title='test1', date=Date(year=2009), slug='test', author=author)
        post1.save()

        self.assertEqual(BlogPost.objects.filter(Date.year == 2009).first().date.year, 2009)
        self.assertEqual(BlogPost.objects.filter(Date.year == 2009).first().author, author)

        BlogPost.drop_collection()

    def test_creation(self):
        user = self.User(name="Test User", age=30)
        self.assertEqual(user.name, "Test User")
        self.assertEqual(user.age, 30)

    def test_reload(self):
        user = self.User(name="Test User", age=20)
        user.save()

        user_obj = self.User.objects.first()
        user_obj.name = "Mr Test User"
        user_obj.age = 21
        user_obj.save()

        self.assertEqual(user.name, "Test User")
        self.assertEqual(user.age, 20)

        user.reload()
        self.assertEqual(user.name, "Mr Test User")
        self.assertEqual(user.age, 21)

    def test_dictionary_access(self):
        user = self.User(name='Test User', age=30)
        self.assertEquals(user['name'], 'Test User')

        self.assertRaises(KeyError, user.__getitem__, 'salary')
        self.assertRaises(KeyError, user.__setitem__, 'salary', 50)

        user['name'] = 'Another User'
        self.assertEquals(user['name'], 'Another User')

        # Length = length(assigned fields + id)
        self.assertEquals(len(user), 3)

        self.assertTrue('age' in user)
        user.age = None
        self.assertFalse('age' in user)
        self.assertFalse('nationality' in user)

    def test_embedded_document(self):
        class Comment(EmbeddedDocument):
            content = StringField()

        self.assertTrue('content' in Comment._fields)
        self.assertFalse('id' in Comment._fields)
        self.assertFalse('collection' in Comment._meta)

    def test_embedded_document_validation(self):
        class Comment(EmbeddedDocument):
            date = DateTimeField()
            content = StringField(required=True)

        comment = Comment()
        self.assertRaises(ValidationError, comment.validate)

        comment.content = 'test'
        comment.validate()

        comment.date = 4
        self.assertRaises(ValidationError, comment.validate)

        comment.date = datetime.datetime.now()
        comment.validate()

    def test_save(self):
        user = self.User(name='Test User', age=30)
        user.save()

        person_obj = self.User.objects.find_one(self.User.name == 'Test User')
        self.assertEqual(person_obj['name'], 'Test User')
        self.assertEqual(person_obj['age'], 30)
        self.assertEqual(person_obj['_id'], user.id)

        class Recipient(Document):
            email = EmailField(required=True)

        recipient = Recipient(email='root@localhost')
        self.assertRaises(ValidationError, recipient.save)

    def test_delete(self):
        user = self.User(name="Test User", age=30)
        user.save()
        self.assertEqual(len(self.User.objects), 1)
        user.delete()
        self.assertEqual(len(self.User.objects), 0)

    def test_save_custom_id(self):
        user = self.User(name='Test User', age=30, id='497ce96f395f2f052a494fd4')
        user.save()
        
        user_obj = self.User.objects.find_one(self.User.name == 'Test User')
        self.assertEqual(str(user_obj['_id']), '497ce96f395f2f052a494fd4')

    def test_save_list(self):
        class Comment(EmbeddedDocument):
            content = StringField()

        class BlogPost(Document):
            content = StringField()
            comments = ListField(EmbeddedDocumentField(Comment))
            tags = ListField(StringField())

        BlogPost.drop_collection()

        post = BlogPost(content='Went for a walk today...')
        post.tags = tags = ['fun', 'leisure']
        comments = [Comment(content='Good for you'), Comment(content='Yay.')]
        post.comments = comments
        post.save()

        post_obj = BlogPost.objects.find_one()
        self.assertEqual(post_obj['tags'], tags)
        for comment_obj, comment in zip(post_obj['comments'], comments):
            self.assertEqual(comment_obj['content'], comment['content'])

        BlogPost.drop_collection()

    def test_save_embedded_document(self):
        class EmployeeDetails(EmbeddedDocument):
            position = StringField()

        class Employee(self.User):
            salary = IntegerField()
            details = EmbeddedDocumentField(EmployeeDetails)

        employee = Employee(name='Test Employee', age=50, salary=20000)
        employee.details = EmployeeDetails(position='Developer')
        employee.save()

        employee_obj = Employee.objects.find_one({'name': 'Test Employee'})
        self.assertEqual(employee_obj['name'], 'Test Employee')
        self.assertEqual(employee_obj['age'], 50)

        self.assertEqual(employee_obj['details']['position'], 'Developer')

    def test_save_reference(self):
        class BlogPost(Document):
            meta = {'collection': 'blogpost_1'}

            content = StringField()
            author = ReferenceField(self.User)

        BlogPost.drop_collection()

        author = self.User(name='Test User')
        author.save()

        post = BlogPost(content='Watched some TV today... how exciting.')
        
        post.author = author
        post.save()

        post_obj = BlogPost.objects.first()

        self.assertTrue(isinstance(post_obj._data['author'], bson.objectid.ObjectId))
        self.assertTrue(isinstance(post_obj.author, self.User))
        self.assertEqual(post_obj.author.name, 'Test User')

        post_obj.author.age = 25
        post_obj.author.save()

        author = list(self.User.objects.filter_by(name='Test User'))[-1]
        self.assertEqual(author.age, 25)

        BlogPost.drop_collection()

    def test_meta_cls(self):
        class Test(EmbeddedDocument):
            name = IntegerField()

        class Test2(Test):
            name = IntegerField()

        self.assertFalse('_cls' in Test().to_mongo())
        self.assertTrue('_cls' in Test2().to_mongo())

    def tearDown(self):
        self.User.drop_collection()

if __name__ == '__main__':
    unittest.main()