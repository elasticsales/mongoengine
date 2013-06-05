import pymongo
from bson import SON, DBRef

from mongoengine import signals
from mongoengine.base import BaseDocument
from mongoengine.base.fields import ObjectIdField
from mongoengine.connection import get_db, DEFAULT_CONNECTION_NAME
from mongoengine.queryset import OperationError, NotUniqueError, QuerySet, DoesNotExist
from mongoengine.queryset.manager import QuerySetManager

__all__ = ('Document', 'EmbeddedDocument', 'DynamicDocument',
           'DynamicEmbeddedDocument', 'OperationError',
           'InvalidCollectionError', 'NotUniqueError', 'MapReduceDocument')

_set = object.__setattr__

class InvalidCollectionError(Exception):
    pass


class EmbeddedDocument(BaseDocument):
    """A :class:`~mongoengine.Document` that isn't stored in its own
    collection.  :class:`~mongoengine.EmbeddedDocument`\ s should be used as
    fields on :class:`~mongoengine.Document`\ s through the
    :class:`~mongoengine.EmbeddedDocumentField` field type.

    A :class:`~mongoengine.EmbeddedDocument` subclass may be itself subclassed,
    to create a specialised version of the embedded document that will be
    stored in the same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To disable this behaviour and remove the dependence on the presence of
    `_cls` set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
    dictionary.
    """

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.to_dict() == other.to_dict()
        return False


class Document(BaseDocument):
    """The base class used for defining the structure and properties of
    collections of documents stored in MongoDB. Inherit from this class, and
    add fields as class attributes to define a document's structure.
    Individual documents may then be created by making instances of the
    :class:`~mongoengine.Document` subclass.

    By default, the MongoDB collection used to store documents created using a
    :class:`~mongoengine.Document` subclass will be the name of the subclass
    converted to lowercase. A different collection may be specified by
    providing :attr:`collection` to the :attr:`meta` dictionary in the class
    definition.

    A :class:`~mongoengine.Document` subclass may be itself subclassed, to
    create a specialised version of the document that will be stored in the
    same collection. To facilitate this behaviour a `_cls`
    field is added to documents (hidden though the MongoEngine interface).
    To disable this behaviour and remove the dependence on the presence of
    `_cls` set :attr:`allow_inheritance` to ``False`` in the :attr:`meta`
    dictionary.

    A :class:`~mongoengine.Document` may use a **Capped Collection** by
    specifying :attr:`max_documents` and :attr:`max_size` in the :attr:`meta`
    dictionary. :attr:`max_documents` is the maximum number of documents that
    is allowed to be stored in the collection, and :attr:`max_size` is the
    maximum size of the collection in bytes. If :attr:`max_size` is not
    specified and :attr:`max_documents` is, :attr:`max_size` defaults to
    10000000 bytes (10MB).

    Indexes may be created by specifying :attr:`indexes` in the :attr:`meta`
    dictionary. The value should be a list of field names or tuples of field
    names. Index direction may be specified by prefixing the field names with
    a **+** or **-** sign.

    Automatic index creation can be disabled by specifying
    attr:`auto_create_index` in the :attr:`meta` dictionary. If this is set to
    False then indexes will not be created by MongoEngine.  This is useful in
    production systems where index creation is performed as part of a
    deployment system.

    By default, _cls will be added to the start of every index (that
    doesn't contain a list) if allow_inheritance is True. This can be
    disabled by either setting cls to False on the specific index or
    by setting index_cls to False on the meta dictionary for the document.
    """

    @classmethod
    def register(cls):
        super(Document, cls).register()
        cls._collection = None
        cls.objects = QuerySetManager()
        id_field = cls._meta['id_field']
        cls._db_id_field = cls._rename_to_db.get(id_field, id_field)
        if not cls._meta.get('collection'):
            cls._meta['collection'] = ''.join('_%s' % c if c.isupper() else c
                                         for c in cls.__name__).strip('_').lower()

    @classmethod
    def _register_default_fields(cls):
        meta = cls._meta
        id_field = meta['id_field']
        if id_field == 'id' and 'id' not in cls._fields and not meta['abstract']:
            field = ObjectIdField(primary_key=True, db_field='_id')
            field._auto_gen = True
            cls._fields['id'] = field

    def pk():
        """Primary key alias
        """
        def fget(self):
            return getattr(self, self._meta['id_field'])

        def fset(self, value):
            return setattr(self, self._meta['id_field'], value)
        return property(fget, fset)
    pk = pk()

    @classmethod
    def drop_collection(cls):
        cls._collection = None
        db = cls._get_db()
        db.drop_collection(cls._get_collection_name())

    @classmethod
    def _get_collection_name(cls):
        return cls._meta['collection']

    @classmethod
    def _get_db(cls):
        return get_db(cls._meta.get('db_alias', DEFAULT_CONNECTION_NAME))

    @property
    def _qs(self):
        if not hasattr(self, '__objects'):
            self.__objects = QuerySet(self, self._get_collection())
        return self.__objects

    @property
    def _object_key(self):
        return {'pk': self.pk}

    @property
    def _db_object_key(self):
        field = self._fields[self._meta['id_field']]
        return {field.db_field: field.to_mongo(self.pk)}

    @classmethod
    def _get_collection(cls):
        if not cls._collection:
            db = cls._get_db()
            collection_name = cls._get_collection_name()
            cls._collection = db[collection_name]
        return cls._collection

    def save(self, validate=True, clean=True, write_concern=None, **kwargs):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: optional kwargs dictionary to be passed throw
            to cascading saves
        :param _refs: A list of processed references used in cascading saves

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Cascade saves are optional = defaults to True, if you want
            fine grain control then you can turn off using document
            meta['cascade'] = False  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values
        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate(clean=clean)

        if not write_concern:
            write_concern = {'w': 1}

        collection = self._get_collection()

        if self._created:
            # Update: Get delta.
            sets, unsets = self._delta()

            db_id_field = self._fields[self._meta['id_field']].db_field
            sets.pop(db_id_field, None)

            update_query = {}
            if sets:
                update_query['$set'] = sets
            if unsets:
                update_query['$unset'] = unsets

            if update_query:
                last_error = collection.update(self._db_object_key, update_query, **write_concern)
                # TODO: evaluate last_error

            created = False
        else:
            # Insert: Get full SON.
            doc = self._to_son()
            object_id = collection.insert(doc, **write_concern)
            self._created = True

            id_field = self._meta['id_field']
            del self._internal_data[id_field]
            self._db_data[self._db_id_field] = object_id

            created = True

        signals.post_save.send(self.__class__, document=self, created=created)
        return self

    def reload(self):
        id_field = self._meta['id_field']
        collection = self._get_collection()
        son = collection.find_one(self._db_object_key)
        if son == None:
            raise DoesNotExist('Document has been deleted.')
        _set(self, '_db_data', son)
        _set(self, '_internal_data', {})
        _set(self, '_created', True)
        _set(self, '_lazy', False)
        return self

    def update(self, **kwargs):
        # TODO: invalidate local fields?

        if not self.pk:
            raise OperationError('attempt to update a document not yet saved')

        return self._qs.filter(**self._object_key).update_one(**kwargs)

    def delete(self, write_concern=None):
        """Delete the :class:`~mongoengine.Document` from the database. This
        will only take effect if the document has been previously saved.

        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        """
        signals.pre_delete.send(self.__class__, document=self)

        if not write_concern:
            write_concern = {'w': 1}

        try:
            self._qs.filter(**self._object_key).delete(write_concern=write_concern)
        except pymongo.errors.OperationFailure, err:
            message = u'Could not delete document (%s)' % err.message
            raise OperationError(message)

        signals.post_delete.send(self.__class__, document=self)

    def to_dbref(self):
        """Returns an instance of :class:`~bson.dbref.DBRef` useful in
        `__raw__` queries."""
        if not self.pk:
            msg = "Only saved documents can have a valid dbref"
            raise OperationError(msg)
        return DBRef(self.__class__._get_collection_name(), self.pk)

class DynamicDocument(Document):
    # TODO
    meta = {
        'abstract': True
    }


class DynamicEmbeddedDocument(EmbeddedDocument):
    pass # TODO


class MapReduceDocument(object):
    pass # TODO
