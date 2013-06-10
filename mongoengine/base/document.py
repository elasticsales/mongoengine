import numbers

from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned
from mongoengine.connection import get_db, DEFAULT_CONNECTION_NAME
from bson import SON
import pymongo

from mongoengine.base.fields import BaseField
from mongoengine.base.fields import ComplexBaseField
from mongoengine.base.common import _all_subclasses, get_document, ALLOW_INHERITANCE
from mongoengine.errors import ValidationError, LookUpError
from mongoengine.common import _import_class

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


_set = object.__setattr__

class BaseDocument(object):
    _dynamic = False

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
    def _translate_field_name(cls, field, sep='.'):
        """Translate a field attribute name to a database field name.
        """
        parts = field.split(sep)
        parts = [f.db_field for f in cls._lookup_field(parts)]
        return '.'.join(parts)

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

    def _mark_as_changed(self, key):
        """Marks a key as explicitly changed by the user
        """
        if not key:
            return
        key = self._db_field_map.get(key, key)
        if (hasattr(self, '_changed_fields') and
           key not in self._changed_fields):
            self._changed_fields.append(key)

    def _clear_changed_fields(self):
        self._changed_fields = []
        EmbeddedDocumentField = _import_class("EmbeddedDocumentField")
        for field_name, field in self._fields.iteritems():
            if (isinstance(field, ComplexBaseField) and
               isinstance(field.field, EmbeddedDocumentField)):
                field_value = getattr(self, field_name, None)
                if field_value:
                    for idx in (field_value if isinstance(field_value, dict)
                                else xrange(len(field_value))):
                        field_value[idx]._clear_changed_fields()
            elif isinstance(field, EmbeddedDocumentField):
                field_value = getattr(self, field_name, None)
                if field_value:
                    field_value._clear_changed_fields()

    def _get_changed_fields(self, key='', inspected=None):
        """Returns a list of all fields that have explicitly been changed.
        """
        EmbeddedDocument = _import_class("EmbeddedDocument")
        DynamicEmbeddedDocument = _import_class("DynamicEmbeddedDocument")
        _changed_fields = []
        _changed_fields += getattr(self, '_changed_fields', [])

        inspected = inspected or set()
        if hasattr(self, 'id'):
            if self.id in inspected:
                return _changed_fields
            inspected.add(self.id)

        field_list = self._fields.copy()
        if self._dynamic:
            field_list.update(self._dynamic_fields)

        for field_name in field_list:

            db_field_name = self._db_field_map.get(field_name, field_name)
            key = '%s.' % db_field_name
            field = getattr(self, field_name) #self._data.get(field_name, None)
            if hasattr(field, 'id'):
                if field.id in inspected:
                    continue
                inspected.add(field.id)

            if (isinstance(field, (EmbeddedDocument, DynamicEmbeddedDocument))
               and db_field_name not in _changed_fields):
                 # Find all embedded fields that have been changed
                changed = field._get_changed_fields(key, inspected)
                _changed_fields += ["%s%s" % (key, k) for k in changed if k]
            elif (isinstance(field, (list, tuple, dict)) and
                    db_field_name not in _changed_fields):
                # Loop list / dict fields as they contain documents
                # Determine the iterator to use
                if not hasattr(field, 'items'):
                    iterator = enumerate(field)
                else:
                    iterator = field.iteritems()
                for index, value in iterator:
                    if not hasattr(value, '_get_changed_fields'):
                        continue
                    list_key = "%s%s." % (key, index)
                    changed = value._get_changed_fields(list_key, inspected)
                    _changed_fields += ["%s%s" % (list_key, k)
                                        for k in changed if k]
        return _changed_fields

    def _delta(self):
        """Returns the delta (set, unset) of the changes for a document.
        Gets any values that have been explicitly changed.
        """
        # Handles cases where not loaded from_son but has _id
        #doc = self.to_mongo()
        doc = self._full_delta()[0] #dict((field, getattr(self, field)) for field in self._fields)

        set_fields = self._get_changed_fields()
        set_data = {}
        unset_data = {}
        parts = []
        if hasattr(self, '_changed_fields'):
            set_data = {}
            # Fetch each set item from its path
            for path in set_fields:
                parts = path.split('.')
                d = doc
                new_path = []
                for p in parts:
                    if isinstance(d, DBRef):
                        break
                    elif isinstance(d, list) and p.isdigit():
                        d = d[int(p)]
                    elif hasattr(d, 'get'):
                        d = d.get(p)
                    new_path.append(p)
                path = '.'.join(new_path)
                set_data[path] = d
        else:
            set_data = doc
            if '_id' in set_data:
                del(set_data['_id'])

        # Determine if any changed items were actually unset.
        for path, value in set_data.items():
            if value or isinstance(value, (numbers.Number, bool)):
                continue

            # If we've set a value that ain't the default value dont unset it.
            default = None
            if (self._dynamic and len(parts) and parts[0] in
               self._dynamic_fields):
                del(set_data[path])
                unset_data[path] = 1
                continue
            elif path in self._fields:
                default = self._fields[path].default
            else:  # Perform a full lookup for lists / embedded lookups
                d = self
                parts = path.split('.')
                db_field_name = parts.pop()
                for p in parts:
                    if isinstance(d, list) and p.isdigit():
                        d = d[int(p)]
                    elif (hasattr(d, '__getattribute__') and
                          not isinstance(d, dict)):
                        real_path = d._reverse_db_field_map.get(p, p)
                        d = getattr(d, real_path)
                    else:
                        d = d.get(p)

                if hasattr(d, '_fields'):
                    field_name = d._reverse_db_field_map.get(db_field_name,
                                                             db_field_name)
                    if field_name in d._fields:
                        default = d._fields.get(field_name).default
                    else:
                        default = None

            if default is not None:
                if callable(default):
                    default = default()

            if default != value:
                continue

            del(set_data[path])
            unset_data[path] = 1

        return set_data, unset_data

    def _full_delta(self):
        sets = {}
        unsets = {}

        data = ((field, getattr(self, field)) for field in self._fields)

        for attr, value in data:
            db_field = self._db_field_map.get(attr, attr)
            value = self._fields[attr].to_mongo(value)
            if value == None:
                unsets[db_field] = 1
            else:
                sets[db_field] = value

        return sets, unsets

    def _to_son(self):
        sets, unsets = self._full_delta()
        son = SON(**sets)
        allow_inheritance = self._meta.get('allow_inheritance',
                                          ALLOW_INHERITANCE)
        if allow_inheritance:
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
