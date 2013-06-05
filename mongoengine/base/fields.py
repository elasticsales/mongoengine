import operator
import warnings
import weakref

from bson import DBRef, ObjectId, SON
import pymongo

from mongoengine.common import _import_class
from mongoengine.errors import ValidationError

from mongoengine.base.common import ALLOW_INHERITANCE
from mongoengine.base.datastructures import BaseDict, BaseList

__all__ = ("BaseField", "ComplexBaseField", "ObjectIdField", "GeoJsonBaseField")

class BaseField(object):
    """A base class for fields in a MongoDB document. Instances of this class
    may be added to subclasses of `Document` to define a document's schema.

    .. versionchanged:: 0.5 - added verbose and help text
    """
    def __init__(self, db_field=None, required=False, default=None,
                 unique=False, unique_with=None, primary_key=False,
                 validation=None, choices=None, verbose_name=None,
                 help_text=None):
        self.name = None # filled in by document
        self.db_field = db_field
        self.required = required or primary_key
        self.default = default
        self.unique = bool(unique or unique_with)
        self.unique_with = unique_with
        self.primary_key = primary_key
        if self.primary_key and not db_field:
            self.db_field = '_id'
        self.validation = validation
        self.choices = choices
        self.verbose_name = verbose_name
        self.help_text = help_text

    def error(self, message="", errors=None, field_name=None):
        """Raises a ValidationError.
        """
        raise ValidationError(message, errors=errors, field_name=field_name)

    def prepare_query_value(self, op, value):
        """Prepare a value that is being used in a query for PyMongo.
        """
        return value

    def validate(self, value, clean=True):
        """Perform validation on a value.
        """
        pass

    def to_python(self, value):
        """Convert a MongoDB-compatible type to a Python type.
        """
        return value

    def to_mongo(self, value):
        """Convert a Python type to a MongoDB-compatible type.
        """
        return value

    def from_python(self, value):
        """Convert a raw Python value (in an assignment) to the internal
        Python representation.
        """
        if value == None:
            return self.default() if callable(self.default) else self.default
        return value

    def validate(self, value, clean=True):
        """Perform validation on a value.
        """
        pass

    def _validate(self, value, **kwargs):
        from mongoengine.document import Document, EmbeddedDocument

        # check choices
        if self.choices:
            is_cls = isinstance(value, (Document, EmbeddedDocument))
            value_to_check = value.__class__ if is_cls else value
            err_msg = 'an instance' if is_cls else 'one'
            if isinstance(self.choices[0], (list, tuple)):
                option_keys = [k for k, v in self.choices]
                if value_to_check not in option_keys:
                    msg = ('Value must be %s of %s' %
                           (err_msg, unicode(option_keys)))
                    self.error(msg)
            elif value_to_check not in self.choices:
                msg = ('Value must be %s of %s' %
                       (err_msg, unicode(self.choices)))
                self.error(msg)

        # check validation argument
        if self.validation is not None:
            if callable(self.validation):
                if not self.validation(value):
                    self.error('Value does not match custom validation method')
            else:
                raise ValueError('validation argument for "%s" must be a '
                                 'callable.' % self.name)

        self.validate(value, **kwargs)

class ComplexBaseField(BaseField):
    """Handles complex fields, such as lists / dictionaries.

    Allows for nesting of embedded documents inside complex types.
    Handles the lazy dereferencing of a queryset by lazily dereferencing all
    items in a list / dict rather than one at a time.

    .. versionadded:: 0.5
    """

    def validate(self, value):
        """If field is provided ensure the value is valid.
        """
        errors = {}
        if self.field:
            if hasattr(value, 'iteritems') or hasattr(value, 'items'):
                sequence = value.iteritems()
            else:
                sequence = enumerate(value)
            for k, v in sequence:
                try:
                    self.field._validate(v)
                except ValidationError, error:
                    errors[k] = error.errors or error
                except (ValueError, AssertionError), error:
                    errors[k] = error

            if errors:
                field_class = self.field.__class__.__name__
                self.error('Invalid %s item (%s)' % (field_class, value),
                           errors=errors)
        # Don't allow empty values if required
        if self.required and not value:
            self.error('Field is required and cannot be empty')

    def lookup_member(self, member_name):
        if self.field:
            return self.field.lookup_member(member_name)
        return None

    def _set_owner_document(self, owner_document):
        if self.field:
            self.field.owner_document = owner_document
        self._owner_document = owner_document

    def _get_owner_document(self, owner_document):
        self._owner_document = owner_document

    owner_document = property(_get_owner_document, _set_owner_document)


class ObjectIdField(BaseField):
    """A field wrapper around MongoDB's ObjectIds.
    """

    def to_python(self, value):
        return value

    def to_mongo(self, value):
        if value and not isinstance(value, ObjectId):
            try:
                return ObjectId(unicode(value))
            except Exception, e:
                # e.message attribute has been deprecated since Python 2.6
                self.error(unicode(e))
        return value

    def prepare_query_value(self, op, value):
        return self.to_mongo(value)

    def validate(self, value):
        try:
            ObjectId(unicode(value))
        except:
            self.error('Invalid Object ID')


class GeoJsonBaseField(BaseField):
    """A geo json field storing a geojson style object.
    .. versionadded:: 0.8
    """

    _geo_index = pymongo.GEOSPHERE
    _type = "GeoBase"

    def __init__(self, auto_index=True, *args, **kwargs):
        """
        :param auto_index: Automatically create a "2dsphere" index. Defaults
            to `True`.
        """
        self._name = "%sField" % self._type
        if not auto_index:
            self._geo_index = False
        super(GeoJsonBaseField, self).__init__(*args, **kwargs)

    def validate(self, value):
        """Validate the GeoJson object based on its type
        """
        if isinstance(value, dict):
            if set(value.keys()) == set(['type', 'coordinates']):
                if value['type'] != self._type:
                    self.error('%s type must be "%s"' % (self._name, self._type))
                return self.validate(value['coordinates'])
            else:
                self.error('%s can only accept a valid GeoJson dictionary'
                           ' or lists of (x, y)' % self._name)
                return
        elif not isinstance(value, (list, tuple)):
            self.error('%s can only accept lists of [x, y]' % self._name)
            return

        validate = getattr(self, "_validate_%s" % self._type.lower())
        error = validate(value)
        if error:
            self.error(error)

    def _validate_polygon(self, value):
        if not isinstance(value, (list, tuple)):
            return 'Polygons must contain list of linestrings'

        # Quick and dirty validator
        try:
            value[0][0][0]
        except:
            return "Invalid Polygon must contain at least one valid linestring"

        errors = []
        for val in value:
            error = self._validate_linestring(val, False)
            if not error and val[0] != val[-1]:
                error = 'LineStrings must start and end at the same point'
            if error and error not in errors:
                errors.append(error)
        if errors:
            return "Invalid Polygon:\n%s" % ", ".join(errors)

    def _validate_linestring(self, value, top_level=True):
        """Validates a linestring"""
        if not isinstance(value, (list, tuple)):
            return 'LineStrings must contain list of coordinate pairs'

        # Quick and dirty validator
        try:
            value[0][0]
        except:
            return "Invalid LineString must contain at least one valid point"

        errors = []
        for val in value:
            error = self._validate_point(val)
            if error and error not in errors:
                errors.append(error)
        if errors:
            if top_level:
                return "Invalid LineString:\n%s" % ", ".join(errors)
            else:
                return "%s" % ", ".join(errors)

    def _validate_point(self, value):
        """Validate each set of coords"""
        if not isinstance(value, (list, tuple)):
            return 'Points must be a list of coordinate pairs'
        elif not len(value) == 2:
            return "Value (%s) must be a two-dimensional point" % repr(value)
        elif (not isinstance(value[0], (float, int)) or
              not isinstance(value[1], (float, int))):
            return "Both values (%s) in point must be float or int" % repr(value)

    def to_mongo(self, value):
        if isinstance(value, dict):
            return value
        return SON([("type", self._type), ("coordinates", value)])
