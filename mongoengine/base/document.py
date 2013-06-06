from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned
from mongoengine.connection import get_db, DEFAULT_CONNECTION_NAME
from bson import SON
import pymongo

from mongoengine.base.common import _all_subclasses, get_document, ALLOW_INHERITANCE
from mongoengine.base.metaclasses import BaseDocumentMetaclass
from mongoengine.errors import ValidationError, LookUpError
from mongoengine.base.fields import BaseField

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


_set = object.__setattr__

class fieldprop(object):
    def __init__(self, name, field):
        self.name = name
        self.field = field

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        else:
            name = self.name
            data = instance._internal_data
            if not name in data:
                if instance._lazy and name != instance._meta['id_field']:
                    # We need to fetch the doc from the database.
                    instance.reload()
                db_field = instance._rename_to_db.get(name, name)
                field = self.field
                try:
                    data[name] = field.to_python(instance._db_data[db_field])
                except KeyError:
                    data[name] = field.default() if callable(field.default) else field.default

            return data[name]

    def __set__(self, instance, value):
        if instance._lazy:
            # Fetch the from the database before we assign to a lazy object.
            instance.reload()
        instance._internal_data[self.name] = self.field.from_python(value)

class BaseDocument(object):
    __metaclass__ = BaseDocumentMetaclass

    @classmethod
    def register(cls):
        from mongoengine.document import Document, EmbeddedDocument
        from mongoengine.base.common import _registered_documents, _document_registry

        if cls in _registered_documents:
            # TODO: This is untested.
            raise ValueError('Document is already registered.')

        # Registers the document class and caches fields and and field
        # functions. When any changes to fields are made the class must be
        # reregistered (untested).

        cls._rename_to_db = {} # field name -> db field name
        cls._rename_to_python = {} # db field name -> field name
        cls._fields = fields = {}
        cls._meta = {
            'ordering': 'id',
            'indexes': [],
            'id_field': 'id',
            'abstract': True,
            'allow_inheritance': False,
            'max_size': None,
            'max_documents': None,
        }

        is_base_class = cls in (BaseDocument, Document, EmbeddedDocument)

        class_name = [cls.__name__]

        bases = cls.__bases__

        collection = None

        for base in bases:
            if issubclass(base, Document):
                if not base in _registered_documents:
                    base.register()

                base_meta = base._meta

                if not is_base_class and not base_meta['abstract']:
                    if base_meta['allow_inheritance']:
                        collection = base_meta['collection']
                    else:
                        raise ValueError('Error registering %s: Document %s may not be subclassed' % (cls.__name__, base.__name__))

                cls._meta.update(base_meta)
                if not base_meta.get('abstract', True):
                    class_name.append(base.__name__)

                fields.update(base._fields)

        if not is_base_class:
            cls._meta.update({
                'abstract': False,
                'collection': collection,
            })
        if 'meta' in cls.__dict__:
            cls._meta.update(getattr(cls, 'meta'))
        cls._class_name = '.'.join(reversed(class_name))

        for field_name in dir(cls):
            # hasattr() will return False if it's a property since we don't want to evaluate them.
            if hasattr(cls, field_name) and field_name[0] != '_':
                field = getattr(cls, field_name)
                if isinstance(field, BaseField):
                    fields[field_name] = field

        for field_name, field in fields.iteritems():
            if field.primary_key:
                cls._meta['id_field'] = field_name
                break

        cls._register_default_fields()

        for field_name, field in fields.iteritems():
            setattr(cls, field_name, fieldprop(field_name, field))
            db_field = field.db_field
            field.name = field_name
            if db_field:
                cls._rename_to_python[db_field] = field_name
                cls._rename_to_db[field_name] = db_field
            else:
                field.db_field = field_name

        exceptions_to_merge = (DoesNotExist, MultipleObjectsReturned)
        for exc in exceptions_to_merge:
            # Create new exception and set to new_class
            name = exc.__name__
            setattr(cls, name, type(name, (exc,), {}))

        class subclasses(object):
            def __get__(self, instance, owner):
                return [owner._class_name] + [cls._class_name for cls in _all_subclasses(owner) if issubclass(cls, Document)]

        cls._subclasses = subclasses()

        _registered_documents.add(cls)
        _document_registry[cls._class_name] = cls

    @classmethod
    def _register_default_fields(cls):
        pass

    @classmethod
    def _build_index_specs(cls, meta_indexes):
        """Generate and merge the full index specs
        """

        #geo_indices = cls._geo_indices()
        unique_indices = cls._unique_with_indexes()
        index_specs = [cls._build_index_spec(spec)
                       for spec in meta_indexes]

        def merge_index_specs(index_specs, indices):
            if not indices:
                return index_specs

            spec_fields = [v['fields']
                           for k, v in enumerate(index_specs)]
            # Merge unqiue_indexes with existing specs
            for k, v in enumerate(indices):
                if v['fields'] in spec_fields:
                    index_specs[spec_fields.index(v['fields'])].update(v)
                else:
                    index_specs.append(v)
            return index_specs

        #index_specs = merge_index_specs(index_specs, geo_indices)
        index_specs = merge_index_specs(index_specs, unique_indices)
        return index_specs

    @classmethod
    def _build_index_spec(cls, spec):
        """Build a PyMongo index spec from a MongoEngine index spec.
        """
        if isinstance(spec, basestring):
            spec = {'fields': [spec]}
        elif isinstance(spec, (list, tuple)):
            spec = {'fields': list(spec)}
        elif isinstance(spec, dict):
            spec = dict(spec)

        index_list = []
        direction = None

        # Check to see if we need to include _cls
        allow_inheritance = cls._meta.get('allow_inheritance',
                                          ALLOW_INHERITANCE)
        include_cls = allow_inheritance and not spec.get('sparse', False)

        for key in spec['fields']:
            # If inherited spec continue
            if isinstance(key, (list, tuple)):
                continue

            # ASCENDING from +,
            # DESCENDING from -
            # GEO2D from *
            direction = pymongo.ASCENDING
            if key.startswith("-"):
                direction = pymongo.DESCENDING
            elif key.startswith("*"):
                direction = pymongo.GEO2D
            if key.startswith(("+", "-", "*")):
                key = key[1:]

            # Use real field name, do it manually because we need field
            # objects for the next part (list field checking)
            parts = key.split('.')
            if parts in (['pk'], ['id'], ['_id']):
                key = '_id'
                fields = []
            else:
                fields = cls._lookup_field(parts)
                parts = [field if field == '_id' else field.db_field
                         for field in fields]
                key = '.'.join(parts)
            index_list.append((key, direction))

        # Don't add cls to a geo index
        if include_cls and direction is not pymongo.GEO2D:
            index_list.insert(0, ('_cls', 1))

        if index_list:
            spec['fields'] = index_list
        if spec.get('sparse', False) and len(spec['fields']) > 1:
            raise ValueError(
                'Sparse indexes can only have one field in them. '
                'See https://jira.mongodb.org/browse/SERVER-2193')

        return spec

    @classmethod
    def _unique_with_indexes(cls, namespace=""):
        """
        Find and set unique indexes
        """
        unique_indexes = []
        for field_name, field in cls._fields.items():
            sparse = False
            # Generate a list of indexes needed by uniqueness constraints
            if field.unique:
                field.required = True
                unique_fields = [field.db_field]

                # Add any unique_with fields to the back of the index spec
                if field.unique_with:
                    if isinstance(field.unique_with, basestring):
                        field.unique_with = [field.unique_with]

                    # Convert unique_with field names to real field names
                    unique_with = []
                    for other_name in field.unique_with:
                        parts = other_name.split('.')
                        # Lookup real name
                        parts = cls._lookup_field(parts)
                        name_parts = [part.db_field for part in parts]
                        unique_with.append('.'.join(name_parts))
                        # Unique field should be required
                        parts[-1].required = True
                        sparse = (not sparse and
                                  parts[-1].name not in cls.__dict__)
                    unique_fields += unique_with

                # Add the new index to the list
                fields = [("%s%s" % (namespace, f), pymongo.ASCENDING)
                          for f in unique_fields]
                index = {'fields': fields, 'unique': True, 'sparse': sparse}
                unique_indexes.append(index)

            # Grab any embedded document field unique indexes
            if (field.__class__.__name__ == "EmbeddedDocumentField" and
               field.document_type != cls):
                field_namespace = "%s." % field_name
                doc_cls = field.document_type
                unique_indexes += doc_cls._unique_with_indexes(field_namespace)

        return unique_indexes

    @classmethod
    def _lookup_field(cls, parts):
        from mongoengine.fields import ReferenceField, GenericReferenceField

        """Lookup a field based on its attribute and return a list containing
        the field's parents and the field.
        """
        if not isinstance(parts, (list, tuple)):
            parts = [parts]
        fields = []
        field = None

        for field_name in parts:
            # Handle ListField indexing:
            if field_name.isdigit():
                new_field = field.field
                fields.append(field_name)
                continue

            if field is None:
                # Look up first field from the document
                if field_name == 'pk':
                    # Deal with "primary key" alias
                    field_name = cls._meta['id_field']
                if field_name in cls._fields:
                    field = cls._fields[field_name]
                #elif cls._dynamic:
                #    DynamicField = _import_class('DynamicField')
                #    field = DynamicField(db_field=field_name)
                else:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
            else:
                if isinstance(field, (ReferenceField, GenericReferenceField)):
                    raise LookUpError('Cannot perform join in mongoDB: %s' %
                                      '__'.join(parts))
                if hasattr(getattr(field, 'field', None), 'lookup_member'):
                    new_field = field.field.lookup_member(field_name)
                else:
                   # Look up subfield on the previous field
                    new_field = field.lookup_member(field_name)
                if not new_field and isinstance(field, ComplexBaseField):
                    fields.append(field_name)
                    continue
                elif not new_field:
                    raise LookUpError('Cannot resolve field "%s"'
                                      % field_name)
                field = new_field  # update field to the new field type
            fields.append(field)
        return fields

    @classmethod
    def _from_son(cls, son, _auto_dereference=False):
        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get('_cls', cls._class_name)

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        return cls(_son=son)

    def __init__(self, _son=None, **kwargs):
        _set(self, '_db_data', _son or {})
        _set(self, '_created', _son is not None)
        _set(self, '_lazy', False)
        _set(self, '_internal_data', {})
        if kwargs:
            pk = kwargs.pop('pk', None)
            for field in set(self._fields.keys()).intersection(kwargs.keys()):
                setattr(self, field, kwargs[field])
            if pk != None:
                self.pk = pk

    def __unicode__(self):
        return u'%s object' % self.__class__.__name__

    def __repr__(self):
        return u'<%s: %s>' % (self.__class__.__name__, unicode(self))

    def __eq__(self, other):
        if isinstance(other, self.__class__) and hasattr(other, 'pk'):
            if self.pk == other.pk:
                return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_dict(self):
        return dict((field, getattr(self, field)) for field in self._fields)

    def _delta(self, full=False):
        sets = {}
        unsets = {}

        if full:
            data = ((field, getattr(self, field)) for field in self._fields)
        else:
            # TODO: Be smarter about this and figure out which fields have
            # actually changed.
            data = self._internal_data.iteritems()

        for attr, value in data:
            db_field = self._rename_to_db.get(attr, attr)
            value = self._fields[attr].to_mongo(value)
            if value == None:
                unsets[db_field] = 1
            else:
                sets[db_field] = value

        return sets, unsets

    def _to_son(self):
        sets, unsets = self._delta(full=True)
        son = SON(**sets)
        #son['_id'] = None
        if self._meta['allow_inheritance']:
            son['_cls'] = self._class_name
        return son

    def __dir__(self):
        return dir(object) + ['_internal_data', '_db_data'] + self._fields.keys()

    def clean(self):
        """
        Hook for doing document level data cleaning before validation is run.

        Any ValidationError raised by this method will not be associated with
        a particular field; it will have a special-case association with the
        field defined by NON_FIELD_ERRORS.
        """
        pass

    def validate(self, clean=True):
        """Ensure that all fields' values are valid and that required fields
        are present.
        """
        from mongoengine.fields import EmbeddedDocumentField, GenericEmbeddedDocumentField

        # Ensure that each field is matched to a valid value
        errors = {}
        if clean:
            try:
                self.clean()
            except ValidationError, error:
                errors[NON_FIELD_ERRORS] = error

        # Get a list of tuples of field names and their current values
        fields = [(field, getattr(self, name))
                  for name, field in self._fields.iteritems()]
        #if self._dynamic:
        #    fields += [(field, self._data.get(name))
        #               for name, field in self._dynamic_fields.items()]

        for field, value in fields:
            if value is not None:
                try:
                    if isinstance(field, (EmbeddedDocumentField,
                                          GenericEmbeddedDocumentField)):
                        field._validate(value, clean=clean)
                    else:
                        field._validate(value)
                except ValidationError, error:
                    errors[field.name] = error.errors or error
                except (ValueError, AttributeError, AssertionError), error:
                    errors[field.name] = error
            elif field.required and not getattr(field, '_auto_gen', False):
                errors[field.name] = ValidationError('Field is required',
                                                     field_name=field.name)

        if errors:
            pk = "None"
            if hasattr(self, 'pk'):
                pk = self.pk
            #elif self._instance:
            #    pk = self._instance.pk
            message = "ValidationError (%s:%s) " % (self._class_name, pk)
            raise ValidationError(message, errors=errors)
