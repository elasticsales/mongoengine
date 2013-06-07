from bson import SON, DBRef
import pymongo
import re

from mongoengine import signals
from mongoengine.base import BaseDocument, get_document
from mongoengine.base.common import ALLOW_INHERITANCE
from mongoengine.base.fields import ObjectIdField
from mongoengine.base.metaclasses import DocumentMetaclass, TopLevelDocumentMetaclass
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

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

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

    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    @classmethod
    def register(cls):
        super(Document, cls).register()
        meta = cls._meta
        meta['index_specs'] = cls._build_index_specs(meta['indexes'])
        cls._collection = None

        # hasattr/getattr don't work since it's a property
        if not 'objects' in dir(cls):
            cls.objects = QuerySetManager()

        id_field = meta['id_field']
        cls._db_id_field = cls._rename_to_db.get(id_field, id_field)
        if not meta.get('collection'):
            meta['collection'] = ''.join('_%s' % c if c.isupper() else c
                                         for c in cls.__name__).strip('_').lower()

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
    def register_delete_rule(cls, document_cls, field_name, rule):
        """This method registers the delete rules to apply when removing this
        object.
        """
        classes = [get_document(class_name)
                    for class_name in cls._subclasses
                    if class_name != cls.__name__] + [cls]
        documents = [get_document(class_name)
                     for class_name in document_cls._subclasses
                     if class_name != document_cls.__name__] + [document_cls]

        for cls in classes:
            for document_cls in documents:
                delete_rules = cls._meta.get('delete_rules') or {}
                delete_rules[(document_cls, field_name)] = rule
                cls._meta['delete_rules'] = delete_rules

    @classmethod
    def drop_collection(cls):
        """Drops the entire collection associated with this
        :class:`~mongoengine.Document` type from the database.
        """
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
        """Dict to identify object in collection
        """
        select_dict = {'pk': self.pk}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            select_dict[k] = getattr(self, k)
        return select_dict

    @property
    def _db_object_key(self):
        field = self._fields[self._meta['id_field']]
        select_dict = {field.db_field: field.to_mongo(self.pk)}
        shard_key = self.__class__._meta.get('shard_key', tuple())
        for k in shard_key:
            actual_key = self._db_field_map.get(k, k)
            select_dict[actual_key] = doc[actual_key]
        return select_dict

    @classmethod
    def _get_collection(cls):
        """Returns the collection for the document.
        Ensures indexes are created. """
        if not cls._collection:
            db = cls._get_db()
            collection_name = cls._get_collection_name()
            # Create collection as a capped collection if specified
            if cls._meta['max_size'] or cls._meta['max_documents']:
                # Get max document limit and max byte size from meta
                max_size = cls._meta['max_size'] or 10000000  # 10MB default
                max_documents = cls._meta['max_documents']

                if collection_name in db.collection_names():
                    cls._collection = db[collection_name]
                    # The collection already exists, check if its capped
                    # options match the specified capped options
                    options = cls._collection.options()
                    if options.get('max') != max_documents or \
                       options.get('size') != max_size:
                        msg = (('Cannot create collection "%s" as a capped '
                               'collection as it already exists')
                               % cls._collection)
                        raise InvalidCollectionError(msg)
                else:
                    # Create the collection as a capped collection
                    opts = {'capped': True, 'size': max_size}
                    if max_documents:
                        opts['max'] = max_documents
                    cls._collection = db.create_collection(
                        collection_name, **opts
                    )
            else:
                cls._collection = db[collection_name]
            if cls._meta.get('auto_create_index', True):
                cls.ensure_indexes()
        return cls._collection

    @classmethod
    def ensure_index(cls, key_or_list, drop_dups=False, background=False,
        **kwargs):
        """Ensure that the given indexes are in place.

        :param key_or_list: a single index key or a list of index keys (to
            construct a multi-field index); keys may be prefixed with a **+**
            or a **-** to determine the index ordering
        """
        index_spec = cls._build_index_spec(key_or_list)
        index_spec = index_spec.copy()
        fields = index_spec.pop('fields')
        index_spec['drop_dups'] = drop_dups
        index_spec['background'] = background
        index_spec.update(kwargs)

        return cls._get_collection().ensure_index(fields, **index_spec)

    @classmethod
    def ensure_indexes(cls):
        """Checks the document meta data and ensures all the indexes exist.

        .. note:: You can disable automatic index creation by setting
                  `auto_create_index` to False in the documents meta data
        """
        background = cls._meta.get('index_background', False)
        drop_dups = cls._meta.get('index_drop_dups', False)
        index_opts = cls._meta.get('index_opts') or {}
        index_cls = cls._meta.get('index_cls', True)

        collection = cls._get_collection()

        # determine if an index which we are creating includes
        # _cls as its first field; if so, we can avoid creating
        # an extra index on _cls, as mongodb will use the existing
        # index to service queries against _cls
        cls_indexed = False
        def includes_cls(fields):
            first_field = None
            if len(fields):
                if isinstance(fields[0], basestring):
                    first_field = fields[0]
                elif isinstance(fields[0], (list, tuple)) and len(fields[0]):
                    first_field = fields[0][0]
            return first_field == '_cls'

        # Ensure document-defined indexes are created
        if cls._meta['index_specs']:
            index_spec = cls._meta['index_specs']
            for spec in index_spec:
                spec = spec.copy()
                fields = spec.pop('fields')
                cls_indexed = cls_indexed or includes_cls(fields)
                opts = index_opts.copy()
                opts.update(spec)
                collection.ensure_index(fields, background=background,
                                        drop_dups=drop_dups, **opts)

        # If _cls is being used (for polymorphism), it needs an index,
        # only if another index doesn't begin with _cls
        if (index_cls and not cls_indexed and
           cls._meta.get('allow_inheritance', ALLOW_INHERITANCE) is True):
            collection.ensure_index('_cls', background=background,
                                    **index_opts)

    def save(self, validate=True, clean=True, write_concern=None,
             cascade=None, cascade_kwargs=None, _refs=None, **kwargs):
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
            Cascade saves are optional = defaults to False, if you want
            fine grain control then you can turn on using document
            meta['cascade'] = True  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values
        """
        signals.pre_save.send(self.__class__, document=self)

        if validate:
            self.validate(clean=clean)

        if not write_concern:
            write_concern = {'w': 1}

        collection = self._get_collection()

        try:
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
                # Fix pymongo's "return return_one and ids[0] or ids":
                # If the ID is 0, pymongo wraps it in a list.
                if isinstance(object_id, list) and not object_id[0]:
                    object_id = object_id[0]
                self._created = True

                id_field = self._meta['id_field']
                del self._internal_data[id_field]
                self._db_data['_id'] = object_id

                created = True

            cascade = (self._meta.get('cascade', False)
                       if cascade is None else cascade)
            if cascade:
                kwargs = {
                    #"force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs['_refs'] = _refs
                self.cascade_save(**kwargs)

        except pymongo.errors.OperationFailure, err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', unicode(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = u'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % unicode(err))
            raise OperationError(message % unicode(err))

        signals.post_save.send(self.__class__, document=self, created=created)
        return self

    def cascade_save(self, *args, **kwargs):
        """Recursively saves any references /
           generic references on an objects"""
        import fields
        _refs = kwargs.get('_refs', []) or []

        for name, cls in self._fields.items():
            if not isinstance(cls, (fields.ReferenceField,
                                    fields.GenericReferenceField)):
                continue

            ref = getattr(self, name)
            if not ref or isinstance(ref, DBRef):
                continue

            if not getattr(ref, '_changed_fields', True):
                continue

            ref_id = "%s,%s" % (ref.__class__.__name__, str(ref.to_dict()))
            if ref and ref_id not in _refs:
                _refs.append(ref_id)
                kwargs["_refs"] = _refs
                ref.save(**kwargs)
                ref._changed_fields = []

    def select_related(self, max_depth=1):
        from mongoengine.dereference import fetch_related
        f = lambda n: {'__all__': f(n-1) if n > 1 else True}
        fetch_related([self], f(max_depth))
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
    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = TopLevelDocumentMetaclass
    __metaclass__ = TopLevelDocumentMetaclass

    # TODO
    meta = {
        'abstract': True
    }


class DynamicEmbeddedDocument(EmbeddedDocument):
    # The __metaclass__ attribute is removed by 2to3 when running with Python3
    # my_metaclass is defined so that metaclass can be queried in Python 2 & 3
    my_metaclass  = DocumentMetaclass
    __metaclass__ = DocumentMetaclass

    pass # TODO


class MapReduceDocument(object):
    """A document returned from a map/reduce query.

    :param collection: An instance of :class:`~pymongo.Collection`
    :param key: Document/result key, often an instance of
                :class:`~bson.objectid.ObjectId`. If supplied as
                an ``ObjectId`` found in the given ``collection``,
                the object can be accessed via the ``object`` property.
    :param value: The result(s) for this key.

    .. versionadded:: 0.3
    """

    def __init__(self, document, collection, key, value):
        self._document = document
        self._collection = collection
        self.key = key
        self.value = value

    @property
    def object(self):
        """Lazy-load the object referenced by ``self.key``. ``self.key``
        should be the ``primary_key``.
        """
        id_field = self._document()._meta['id_field']
        id_field_type = type(id_field)

        if not isinstance(self.key, id_field_type):
            try:
                self.key = id_field_type(self.key)
            except:
                raise Exception("Could not cast key as %s" % \
                                id_field_type.__name__)

        if not hasattr(self, "_key_object"):
            self._key_object = self._document.objects.with_id(self.key)
            return self._key_object
        return self._key_object
