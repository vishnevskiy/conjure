from mongoalchemy import operations
import re
import exceptions
import datetime
import bson

class Field(operations.Common):
    def __init__(self, verbose_name=None, required=True, default=None, validators=None, choices=None):
        self.owner = None
        self.name = None
        self.verbose_name = verbose_name
        self.required = required
        self.default = default
        self.validators = validators or []
        self.choices = choices or []

    def get_key(self, positional=False):
        if isinstance(self.owner, Field):
            return self.owner.get_key(positional)
        elif  'parent_field' in self.owner._meta:
            if positional:
                sep = '.$.'
            else:
                sep = '.'

            return self.owner._meta['parent_field'].get_key(positional) + sep + self.name

        return self.name

    def __get__(self, instance, _):
        if instance is None:
            return self

        value = instance._data.get(self.name)

        if value is None:
            value = self.get_default()

        return value

    def __set__(self, instance, value):
        instance._data[self.name] = value

    def has_default(self):
       return self.default is not None

    def get_default(self):
       if self.has_default():
           if callable(self.default):
               return self.default()

           return self.default

       return None

    def to_python(self, value):
        return value

    def to_mongo(self, value):
        return self.to_python(value)

    def validate(self, value):
        pass

    def _validate(self, value):
        if self.choices:
            if value not in self.choices:
                raise exceptions.ValidationError('Value must be one of %s.' % unicode(self.choices))

        for validator in self.validators:
            validator(value)
            
        self.validate(value)

class ObjectIdField(Field):
    def to_python(self, value):
        return value

    def to_mongo(self, value):
        if not isinstance(value, bson.objectid.ObjectId):
            try:
                return bson.objectid.ObjectId(unicode(value))
            except Exception, e:
                raise exceptions.ValidationError(unicode(e))

        return value

    def validate(self, value):
        try:
            bson.objectid.ObjectId(unicode(value))
        except bson.objectid.InvalidId:
            raise exceptions.ValidationError('Invalid Object ID')

class StringField(operations.String, Field):
    def __init__(self, regex=None, min_length=None, max_length=None, **kwargs):
        self.regex = re.compile(regex) if regex else None
        self.min_length = min_length
        self.max_length = max_length
        Field.__init__(self, **kwargs)

    def to_python(self, value):
        return unicode(value)

    def validate(self, value):
        assert isinstance(value, (str, unicode))

        if self.max_length is not None and len(value) > self.max_length:
            raise exceptions.ValidationError('String value is too long')

        if self.min_length is not None and len(value) < self.min_length:
            raise exceptions.ValidationError('String value is too short')

        if self.regex is not None and self.regex.match(value) is None:
            raise exceptions.ValidationError('String value did not match validation regex')

class EmailField(StringField):
    EMAIL_REGEX = re.compile(
        r"(^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"'
        r')@(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?$', re.IGNORECASE
    )

    def validate(self, value):
        if not EmailField.EMAIL_REGEX.match(value):
            raise exceptions.ValidationError('Invalid Email: %s' % value)

class IntegerField(operations.Number, Field):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        Field.__init__(self, **kwargs)

    def to_python(self, value):
        return int(value)

    def validate(self, value):
        try:
            value = int(value)
        except:
            raise exceptions.ValidationError('%s could not be converted to int' % value)

        if self.min_value is not None and value < self.min_value:
            raise exceptions.ValidationError('Integer value is too small')

        if self.max_value is not None and value > self.max_value:
            raise exceptions.ValidationError('Integer value is too large')

class FloatField(IntegerField):
    def to_python(self, value):
        return float(value)

    def validate(self, value):
        if isinstance(value, int):
            value = float(value)
            
        assert isinstance(value, float)

        if self.min_value is not None and value < self.min_value:
            raise exceptions.ValidationError('Float value is too small')

        if self.max_value is not None and value > self.max_value:
            raise exceptions.ValidationError('Float value is too large')

class BooleanField(Field):
    def to_python(self, value):
        return bool(value)

    def validate(self, value):
        assert isinstance(value, bool)

class DateTimeField(Field):
    def validate(self, value):
        assert isinstance(value, datetime.datetime)

class DictField(Field):
    def validate(self, value):
        if not isinstance(value, dict):
            raise exceptions.ValidationError('Only dictionaries may be used in a DictField')

        if any(('.' in k or '$' in k) for k in value):
            raise exceptions.ValidationError('Invalid dictionary key name - keys may not contain "." or "$" characters')

    def __getitem__(self, key):
        class Proxy(operations.Common):
            def __init__(self, key, field):
                self.key = key
                self.field = field

            def get_key(self, **kwargs):
                return self.field.get_key(False) + '.' + self.key

        return Proxy(key, self)

class ListField(operations.List, Field):
    def __init__(self, field, default=None, **kwargs):
        if not isinstance(field, Field):
            raise exceptions.ValidationError('Argument to ListField constructor must be a valid field')

        field.owner = self
        self.field = field
        Field.__init__(self, default=default or [], **kwargs)

    def to_python(self, value):
        return [self.field.to_python(item) for item in value]

    def to_mongo(self, value):
        return [self.field.to_mongo(item) for item in value]

    def validate(self, value):
        if not isinstance(value, (list, tuple)):
            raise exceptions.ValidationError('Only lists and tuples may be used in a list field')

        try:
            [self.field.validate(item) for item in value]
        except Exception, err:
            raise exceptions.ValidationError('Invalid ListField item (%s)' % str(err))

class EmbeddedDocumentField(Field):
    def __init__(self, document, **kwargs):
        if not (hasattr(document, '_meta') and document._meta['embedded']):
            raise exceptions.ValidationError('Invalid embedded document class provided to an EmbeddedDocumentField')

        if 'parent_field' in document._meta:
            raise exceptions.ValidationError('This document is already embedded')

        document._meta['parent_field'] = self
        self.document = document

        Field.__init__(self, **kwargs)

    def to_python(self, value):
        if not isinstance(value, self.document):
            return self.document.to_python(value)

        return value

    def to_mongo(self, value):
        return self.document.to_mongo(value)

    def validate(self, value):
        if not isinstance(value, self.document):
            raise exceptions.ValidationError('Invalid embedded document instance provided to an EmbeddedDocumentField')

        self.document.validate(value)