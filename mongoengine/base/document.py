from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned
from mongoengine.connection import get_db, DEFAULT_CONNECTION_NAME
from bson import SON

from mongoengine.base.common import _all_subclasses, get_document
from mongoengine.base.fields import BaseField

__all__ = ('BaseDocument', 'NON_FIELD_ERRORS')

NON_FIELD_ERRORS = '__all__'


_set = object.__setattr__
#_get = object.__getattr__

class BaseDocument(object):
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
            'id_field': 'id',
            'abstract': True,
            'allow_inheritance': True,
        }

        is_base_class = cls in (BaseDocument, Document, EmbeddedDocument)

        class_name = [cls.__name__]

        bases = cls.__bases__

        def populate_fields(cls, fields):
            for field_name in dir(cls):
                # hasattr() will return False if it's a property since we don't want to evaluate them.
                if hasattr(cls, field_name) and field_name[0] != '_' and isinstance(getattr(cls, field_name), BaseField):
                    field = getattr(cls, field_name)
                    fields[field_name] = field
                    delattr(cls, field_name)

        def register_mixin(cls):
            bases = cls.__bases__
            cls._fields = fields = {}
            for base in bases:
                if base == object:
                    continue
                if not base in _registered_documents:
                    register_mixin(base)
                fields.update(base._fields)
            populate_fields(cls, fields)
            _registered_documents.add(cls)

        collection = None

        for base in bases:
            if issubclass(base, Document):
                if not base in _registered_documents:
                    base.register()

                base_meta = base._meta

                if not base_meta['allow_inheritance'] and not base_meta['abstract']:
                    raise ValueError('Error registering %s: Document %s may not be subclassed' % (cls.__name__, base.__name__))

                if not is_base_class and base_meta['allow_inheritance'] and not base_meta['abstract']:
                    collection = base_meta['collection']

                cls._meta.update(base_meta)
                if not base_meta.get('abstract', True):
                    class_name.append(base.__name__)

            elif base != object:
                # Quick registration of mixins
                # TODO: don't register if they have no fields
                if not base in _registered_documents:
                    register_mixin(base)
            else:
                continue

            fields.update(base._fields)
                    

        if not is_base_class:
            cls._meta.update({
                'abstract': False,
                'allow_inheritance': False,
                'collection': collection,
            })
        if 'meta' in cls.__dict__:
            cls._meta.update(getattr(cls, 'meta'))
        cls._class_name = '.'.join(reversed(class_name))

        populate_fields(cls, fields)

        for field_name, field in fields.iteritems():
            if field.primary_key:
                cls._meta['id_field'] = field_name
                break

        cls._register_default_fields()

        for field_name, field in fields.iteritems():
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
    def _lookup_field(cls, parts):
        # TODO
        fields = []
        for part in parts:
            if part == 'pk':
                part = cls._meta['id_field']
            field = cls._fields[part]
            fields.append(field)
        #print cls, parts
        return fields

    @classmethod
    def _from_son(cls, son, _auto_dereference=False):
        # get the class name from the document, falling back to the given
        # class if unavailable
        class_name = son.get('_cls', cls._class_name)

        # Return correct subclass for document type
        if class_name != cls._class_name:
            cls = get_document(class_name)

        self = cls()
        _set(self, '_db_data', son)
        _set(self, '_created', True)
        return self

    def __init__(self, **kwargs):
        # TODO: filter by keys
        pk = kwargs.pop('pk', None)
        _set(self, '_created', False)
        _set(self, '_lazy', False)
        _set(self, '_internal_data', kwargs)
        _set(self, '_db_data', {})
        if pk != None: # TODO: slow
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
        return dict((field, self._get(field)) for field in self._fields)

    def _delta(self, full=False):
        sets = {}
        unsets = {}

        if full:
            data = ((field, self._get(field)) for field in self._fields)
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

    def _get(self, attr):
        data = self._internal_data
        if not attr in data:
            if self._lazy and attr != self._meta['id_field']:
                # We need to fetch the doc from the database.
                self.reload()
            db_field = self._rename_to_db.get(attr, attr)
            field = self._fields[attr]
            try:
                data[attr] = field.to_python(self._db_data[db_field])
            except KeyError:
                data[attr] = field.default() if callable(field.default) else field.default

        return data[attr]

    def __getattr__(self, attr):
        if attr in object.__getattribute__(self, '_fields'):
            return self._get(attr)
        else:
            raise AttributeError(attr)
        return self._internal_data[attr]

    def __setattr__(self, attr, val):
        field = self._fields.get(attr, None)
        if field:
            self._internal_data[attr] = field.from_python(val)
        else:
            super(BaseDocument, self).__setattr__(attr, val)
