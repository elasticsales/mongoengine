import copy
import itertools
import operator
import pprint
import re
import warnings

import pymongo
from bson import json_util
from bson.code import Code
from pymongo.collection import ReturnDocument
from pymongo.common import validate_read_preference
from pymongo.read_concern import ReadConcern

from mongoengine import signals
from mongoengine.common import _import_class
from mongoengine.context_managers import set_read_write_concern, set_write_concern
from mongoengine.errors import InvalidQueryError, NotUniqueError, OperationError
from mongoengine.pymongo_support import LEGACY_JSON_OPTIONS
from mongoengine.queryset import transform
from mongoengine.queryset.field_list import QueryFieldList
from mongoengine.queryset.visitor import Q, QNode

__all__ = ('QuerySet', 'DO_NOTHING', 'NULLIFY', 'CASCADE', 'DENY', 'PULL')

# The maximum number of items to display in a QuerySet.__repr__
REPR_OUTPUT_SIZE = 20
ITER_CHUNK_SIZE = 100

# Delete rules
DO_NOTHING = 0
NULLIFY = 1
CASCADE = 2
DENY = 3
PULL = 4

RE_TYPE = type(re.compile(''))


class QuerySet(object):
    """A set of results returned from a query. Wraps a MongoDB cursor,
    providing :class:`~mongoengine.Document` objects as the results.
    """
    __dereference = False
    _auto_dereference = True

    def __init__(self, document, collection):
        self._document = document
        self._collection_obj = collection
        self._mongo_query = None
        self._query_obj = Q()
        self._initial_query = {}
        self._loaded_fields = QueryFieldList()
        self._ordering = None
        self._timeout = True
        self._class_check = True
        self._read_preference = None
        self._read_concern = None
        self._iter = False
        self._scalar = []
        self._none = False
        self._as_pymongo = False
        self._as_pymongo_coerce = False
        self._result_cache = []
        self._has_more = True
        self._len = None

        # If inheritance is allowed, only return instances and instances of
        # subclasses of the class being used
        if document._meta.get('allow_inheritance') is True:
            if len(self._document._subclasses) == 1:
                self._initial_query = {"_cls": self._document._subclasses[0]}
            else:
                self._initial_query = {"_cls": {"$in": self._document._subclasses}}
            self._loaded_fields = QueryFieldList(always_include=['_cls'])
        self._cursor_obj = None
        self._limit = None
        self._skip = None
        self._hint = -1  # Using -1 as None is a valid value for hint
        self._batch_size = None

    def __call__(self, q_obj=None, class_check=True, slave_okay=False,
                 read_preference=None, **query):
        """Filter the selected documents by calling the
        :class:`~mongoengine.queryset.QuerySet` with a query.

        :param q_obj: a :class:`~mongoengine.queryset.Q` object to be used in
            the query; the :class:`~mongoengine.queryset.QuerySet` is filtered
            multiple times with different :class:`~mongoengine.queryset.Q`
            objects, only the last one will be used
        :param class_check: If set to False bypass class name check when
            querying collection
        :params slave_okay: a deprecated no-op, not removed at the moment
            as to not break the signature of this method.
        :params read_preference: if set, overrides the connection-level
            read preference.
        :param query: Django-style query keyword arguments
        """
        query = Q(**query)
        if q_obj:
            # make sure proper query object is passed
            if not isinstance(q_obj, QNode):
                msg = ("Not a query object: %s. "
                       "Did you intend to use key=value?" % q_obj)
                raise InvalidQueryError(msg)
            query &= q_obj

        if read_preference is None:
            queryset = self.clone()
        else:
            # Use the clone provided when setting read_preference
            queryset = self.read_preference(read_preference)

        queryset._query_obj &= query
        queryset._mongo_query = None
        queryset._cursor_obj = None
        queryset._class_check = class_check

        return queryset

    def __len__(self):
        """Since __len__ is called quite frequently (for example, as part of
        list(qs) we populate the result cache and cache the length.
        """
        if self._len is not None:
            return self._len
        if self._has_more:
            # populate the cache
            list(self._iter_results())

        self._len = len(self._result_cache)
        return self._len

    def __iter__(self):
        """Iteration utilises a results cache which iterates the cursor
        in batches of ``ITER_CHUNK_SIZE``.

        If ``self._has_more`` the cursor hasn't been exhausted so cache then
        batch.  Otherwise iterate the result_cache.
        """
        self._iter = True
        if self._has_more:
            return self._iter_results()

        # iterating over the cache.
        return iter(self._result_cache)

    def _iter_results(self):
        """A generator for iterating over the result cache.

        Also populates the cache if there are more possible results to yield.
        Raises StopIteration when there are no more results"""
        pos = 0
        while True:
            upper = len(self._result_cache)
            while pos < upper:
                yield self._result_cache[pos]
                pos = pos + 1
            if not self._has_more:
                return
            if len(self._result_cache) <= pos:
                self._populate_cache()

    def _populate_cache(self):
        """
        Populates the result cache with ``ITER_CHUNK_SIZE`` more entries
        (until the cursor is exhausted).
        """
        if self._has_more:
            try:
                for i in range(ITER_CHUNK_SIZE):
                    self._result_cache.append(next(self))
            except StopIteration:
                self._has_more = False

    def __getitem__(self, key):
        """Support skip and limit using getitem and slicing syntax.
        """
        queryset = self.clone()

        # Slice provided
        if isinstance(key, slice):
            try:
                queryset._cursor_obj = queryset._cursor[key]
                queryset._skip, queryset._limit = key.start, key.stop
                if key.start and key.stop:
                    queryset._limit = key.stop - key.start
            except IndexError as err:
                # PyMongo raises an error if key.start == key.stop, catch it,
                # bin it, kill it.
                start = key.start or 0
                if start >= 0 and key.stop >= 0 and key.step is None:
                    if start == key.stop:
                        queryset.limit(0)
                        queryset._skip = key.start
                        queryset._limit = key.stop - start
                        return queryset
                raise err
            # Allow further QuerySet modifications to be performed
            return queryset
        # Integer index provided
        elif isinstance(key, int):
            if queryset._scalar:
                return queryset._get_scalar(
                    queryset._document._from_son(queryset._cursor[key],
                                                 _auto_dereference=self._auto_dereference))
            if queryset._as_pymongo:
                return queryset._get_as_pymongo(next(queryset._cursor))
            return queryset._document._from_son(queryset._cursor[key],
                                                _auto_dereference=self._auto_dereference)
        raise AttributeError

    def __repr__(self):
        """Provides the string representation of the QuerySet
        """

        if self._iter:
            return '.. queryset mid-iteration ..'

        self._populate_cache()
        data = self._result_cache[:REPR_OUTPUT_SIZE + 1]
        if len(data) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."
        return repr(data)

    # Core functions

    def all(self):
        """Returns all documents."""
        return self.__call__()

    def filter(self, *q_objs, **query):
        """An alias of :meth:`~mongoengine.queryset.QuerySet.__call__`
        """
        return self.__call__(*q_objs, **query)

    def get(self, *q_objs, **query):
        """Retrieve the the matching object raising
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` exception if multiple results
        and :class:`~mongoengine.queryset.DoesNotExist` or
        `DocumentName.DoesNotExist` if no results are found.

        .. versionadded:: 0.3
        """
        queryset = self.__call__(*q_objs, **query)
        queryset = queryset.order_by().limit(2)
        try:
            result = next(queryset)
        except StopIteration:
            msg = ("%s matching query does not exist."
                   % queryset._document._class_name)
            raise queryset._document.DoesNotExist(msg)
        try:
            next(queryset)
        except StopIteration:
            return result

        queryset.rewind()
        message = '%d items returned, instead of 1' % queryset.count()
        raise queryset._document.MultipleObjectsReturned(message)

    def create(self, **kwargs):
        """Create new object. Returns the saved object instance.

        .. versionadded:: 0.4
        """
        return self._document(**kwargs).save()

    def get_or_create(self, write_concern=None, auto_save=True,
                      *q_objs, **query):
        """Retrieve unique object or create, if it doesn't exist. Returns a
        tuple of ``(object, created)``, where ``object`` is the retrieved or
        created object and ``created`` is a boolean specifying whether a new
        object was created. Raises
        :class:`~mongoengine.queryset.MultipleObjectsReturned` or
        `DocumentName.MultipleObjectsReturned` if multiple results are found.
        A new document will be created if the document doesn't exists; a
        dictionary of default values for the new document may be provided as a
        keyword argument called :attr:`defaults`.

        .. note:: This requires two separate operations and therefore a
            race condition exists.  Because there are no transactions in
            mongoDB other approaches should be investigated, to ensure you
            don't accidently duplicate data when using this method.  This is
            now scheduled to be removed before 1.0

        :param write_concern: optional extra keyword arguments used if we
            have to create a new document.
            Passes any write_concern onto :meth:`~mongoengine.Document.save`

        :param auto_save: if the object is to be saved automatically if
            not found.

        .. deprecated:: 0.8
        .. versionchanged:: 0.6 - added `auto_save`
        .. versionadded:: 0.3
        """
        msg = ("get_or_create is scheduled to be deprecated.  The approach is "
               "flawed without transactions. Upserts should be preferred.")
        warnings.warn(msg, DeprecationWarning)

        defaults = query.get('defaults', {})
        if 'defaults' in query:
            del query['defaults']

        try:
            doc = self.get(*q_objs, **query)
            return doc, False
        except self._document.DoesNotExist:
            query.update(defaults)
            doc = self._document(**query)

            if auto_save:
                doc.save(write_concern=write_concern)
            return doc, True

    def first(self):
        """Retrieve the first object matching the query.
        """
        queryset = self.clone()
        try:
            result = queryset[0]
        except IndexError:
            result = None
        return result

    def insert(self, doc_or_docs, load_bulk=True, write_concern=None,
               signal_kwargs=None):
        """bulk insert documents

        :param docs_or_doc: a document or list of documents to be inserted
        :param load_bulk (optional): If True returns the list of document
            instances.
        :param write_concern: Write concern of this operation.
        :parm signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        By default returns document instances, set ``load_bulk`` to False to
        return just ``ObjectIds``

        .. versionadded:: 0.5
        """
        Document = _import_class('Document')

        if write_concern is None:
            write_concern = {}

        docs = doc_or_docs
        return_one = False
        if isinstance(docs, Document) or issubclass(docs.__class__, Document):
            return_one = True
            docs = [docs]

        for doc in docs:
            if not isinstance(doc, self._document):
                msg = ("Some documents inserted aren't instances of %s"
                       % str(self._document))
                raise OperationError(msg)
            if doc.pk and doc._created:
                msg = 'Some documents have ObjectIds, use doc.update() instead'
                raise OperationError(msg)

        signal_kwargs = signal_kwargs or {}
        signals.pre_bulk_insert.send(self._document,
                                     documents=docs, **signal_kwargs)

        raw = [doc.to_mongo() for doc in docs]

        with set_write_concern(self._collection, write_concern) as collection:
            insert_func = collection.insert_many
            if return_one:
                raw = raw[0]
                insert_func = collection.insert_one

        try:
            inserted_result = insert_func(raw)
            ids = [inserted_result.inserted_id] if return_one else inserted_result.inserted_ids
        except pymongo.errors.DuplicateKeyError as err:
            message = 'Could not save document (%s)'
            raise NotUniqueError(message % str(err))
        except pymongo.errors.BulkWriteError as err:
            # inserting documents that already have an _id field will
            # give huge performance debt or raise
            message = 'Document must not have _id value before bulk write (%s)'
            raise NotUniqueError(message % str(err))
        except pymongo.errors.OperationFailure as err:
            message = 'Could not save document (%s)'
            if re.match('^E1100[01] duplicate key', str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = 'Tried to save duplicate unique keys (%s)'
                raise NotUniqueError(message % str(err))
            raise OperationError(message % str(err))

        # Apply inserted_ids to documents
        for doc, doc_id in zip(docs, ids):
            doc.pk = doc_id

        if not load_bulk:
            signals.post_bulk_insert.send(
                self._document, documents=docs, loaded=False)
            return return_one and ids[0] or ids

        documents = self.in_bulk(ids)
        results = [documents.get(obj_id) for obj_id in ids]
        signals.post_bulk_insert.send(
            self._document, documents=results, loaded=True, **signal_kwargs)
        return results[0] if return_one else results

    def count(self, with_limit_and_skip=True):
        """Count the selected elements in the query.

        :param with_limit_and_skip (optional): take any :meth:`limit` or
            :meth:`skip` that has been applied to this cursor into account when
            getting the count
        """
        if self._limit == 0:
            return 0

        if self._none:
            return 0

        options = {}

        if with_limit_and_skip:
            if self._limit is not None:
                options["limit"] = self._limit
            if self._skip is not None:
                options["skip"] = self._skip
        if self._hint not in (-1, None):
            options["hint"] = self._hint

        if with_limit_and_skip and self._len is not None:
            return self._len

        count = self._cursor.collection.estimated_document_count(
            query=self._query, **options
        )

        if with_limit_and_skip:
            self._len = count
        return count

    def delete(self, write_concern=None, _from_doc_delete=False):
        """Delete the documents matched by the query.

        :param write_concern: Write concern of this operation.
        :param _from_doc_delete: True when called from document delete therefore
            signals will have been triggered so don't loop.
        """
        queryset = self.clone()
        doc = queryset._document

        if write_concern is None:
            write_concern = {}

        # Handle deletes where skips or limits have been applied or
        # there is an untriggered delete signal
        has_delete_signal = signals.signals_available and (
            signals.pre_delete.has_receivers_for(self._document) or
            signals.post_delete.has_receivers_for(self._document))

        call_document_delete = (queryset._skip or queryset._limit or
                                has_delete_signal) and not _from_doc_delete

        if call_document_delete:
            for doc in queryset:
                doc.delete(write_concern=write_concern)
            return

        delete_rules = doc._meta.get('delete_rules') or {}
        # Check for DENY rules before actually deleting/nullifying any other
        # references
        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == DENY and document_cls.objects(
                    **{field_name + '__in': self}).count() > 0:
                msg = ("Could not delete document (%s.%s refers to it)"
                       % (document_cls.__name__, field_name))
                raise OperationError(msg)

        for rule_entry in delete_rules:
            document_cls, field_name = rule_entry
            rule = doc._meta['delete_rules'][rule_entry]
            if rule == CASCADE:
                ref_q = document_cls.objects(**{field_name + '__in': self})
                ref_q_count = ref_q.count()
                if (doc != document_cls and ref_q_count > 0
                   or (doc == document_cls and ref_q_count > 0)):
                    ref_q.delete(write_concern=write_concern)
            elif rule == NULLIFY:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern, **{'unset__%s' % field_name: 1})
            elif rule == PULL:
                document_cls.objects(**{field_name + '__in': self}).update(
                    write_concern=write_concern,
                    **{'pull_all__%s' % field_name: self})

        with set_write_concern(queryset._collection, write_concern) as coll:
            coll.delete_many(queryset._query)

    def update(
        self, upsert=False, multi=True, write_concern=None, read_concern=None, **update
    ):
        """Perform an atomic update on the fields matched by the query.

        :param upsert: Any existing document with that "_id" is overwritten.
        :param multi: Update multiple documents.
        :param write_concern: Write concern of this operation.
        :param read_concern: Override the read concern for the operation
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        if not update and not upsert:
            raise OperationError("No update parameters, would remove data")

        if write_concern is None:
            write_concern = {}

        queryset = self.clone()
        query = queryset._query
        update = transform.update(queryset._document, **update)

        # If doing an atomic upsert on an inheritable class
        # then ensure we add _cls to the update operation
        if upsert and '_cls' in query:
            if '$set' in update:
                update["$set"]["_cls"] = queryset._document._class_name
            else:
                update["$set"] = {"_cls": queryset._document._class_name}
        try:
            with set_read_write_concern(
                queryset._collection, write_concern, read_concern
            ) as collection:
                update_func = collection.update_one
                if multi:
                    update_func = collection.update_many
                result = update_func(query, update, upsert=upsert)
            if result.raw_result:
                return result.raw_result['n']
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError('Update failed (%s)' % str(err))
        except pymongo.errors.OperationFailure as err:
            if str(err) == 'multi not coded yet':
                message = 'update() method requires MongoDB 1.1.3+'
                raise OperationError(message)
            raise OperationError('Update failed (%s)' % str(err))

    def update_one(self, upsert=False, write_concern=None, **update):
        """Perform an atomic update on first field matched by the query.

        :param upsert: Any existing document with that "_id" is overwritten.
        :param write_concern: Extra keyword arguments are passed down which
            will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.2
        """
        return self.update(
            upsert=upsert,
            multi=False,
            write_concern=write_concern,
            **update
        )

    def modify(self, upsert=False, full_response=False, remove=False, new=False, **update):
        """Update and return the updated document.

        Returns either the document before or after modification based on `new`
        parameter. If no documents match the query and `upsert` is false,
        returns ``None``. If upserting and `new` is false, returns ``None``.

        If the full_response parameter is ``True``, the return value will be
        the entire response object from the server, including the 'ok' and
        'lastErrorObject' fields, rather than just the modified document.
        This is useful mainly because the 'lastErrorObject' document holds
        information about the command's execution.

        :param upsert: insert if document doesn't exist (default ``False``)
        :param full_response: return the entire response object from the
            server (default ``False``, not available for PyMongo 3+)
        :param remove: remove rather than updating (default ``False``)
        :param new: return updated rather than original document
            (default ``False``)
        :param update: Django-style update keyword arguments

        .. versionadded:: 0.9
        """

        if remove and new:
            raise OperationError("Conflicting parameters: remove and new")

        if not update and not upsert and not remove:
            raise OperationError(
                "No update parameters, must either update or remove")

        queryset = self.clone()
        query = queryset._query
        if not remove:
            update = transform.update(queryset._document, **update)
        sort = queryset._ordering

        try:
            if full_response:
                msg = 'With PyMongo 3+, it is not possible anymore to get the full response.'
                warnings.warn(msg, DeprecationWarning)
            if remove:
                result = queryset._collection.find_one_and_delete(
                    query, sort=sort, **self._cursor_args)
            else:
                if new:
                    return_doc = ReturnDocument.AFTER
                else:
                    return_doc = ReturnDocument.BEFORE
                result = queryset._collection.find_one_and_update(
                    query, update, upsert=upsert, sort=sort, return_document=return_doc,
                    **self._cursor_args)
        except pymongo.errors.DuplicateKeyError as err:
            raise NotUniqueError('Update failed (%s)' % err)
        except pymongo.errors.OperationFailure as err:
            raise OperationError('Update failed (%s)' % err)

        if full_response:
            if result["value"] is not None:
                result["value"] = self._document._from_son(result["value"])
        else:
            if result is not None:
                result = self._document._from_son(result)

        return result

    def with_id(self, object_id):
        """Retrieve the object matching the id provided.  Uses `object_id` only
        and raises InvalidQueryError if a filter has been applied. Returns
        `None` if no document exists with that id.

        :param object_id: the value for the id of the document to look up

        .. versionchanged:: 0.6 Raises InvalidQueryError if filter has been set
        """
        queryset = self.clone()
        if not queryset._query_obj.empty:
            msg = "Cannot use a filter whilst using `with_id`"
            raise InvalidQueryError(msg)
        return queryset.filter(pk=object_id).first()

    def in_bulk(self, object_ids):
        """Retrieve a set of documents by their ids.

        :param object_ids: a list or tuple of ``ObjectId``\ s
        :rtype: dict of ObjectIds as keys and collection-specific
                Document subclasses as values.

        .. versionadded:: 0.3
        """
        doc_map = {}

        docs = self._collection.find({'_id': {'$in': object_ids}},
                                     **self._cursor_args)
        if self._scalar:
            for doc in docs:
                doc_map[doc['_id']] = self._get_scalar(
                    self._document._from_son(doc))
        elif self._as_pymongo:
            for doc in docs:
                doc_map[doc['_id']] = self._get_as_pymongo(doc)
        else:
            for doc in docs:
                doc_map[doc['_id']] = self._document._from_son(doc)

        return doc_map

    def none(self):
        """Helper that just returns a list"""
        queryset = self.clone()
        queryset._none = True
        return queryset

    def no_sub_classes(self):
        """
        Only return instances of this document and not any inherited documents
        """
        if self._document._meta.get('allow_inheritance') is True:
            self._initial_query = {"_cls": self._document._class_name}

        return self

    def only_classes(self, *classes):
        doc = self._document
        if doc._meta.get('allow_inheritance') is True:
            queryset = self.clone()
            class_names = [cls._class_name for cls in classes]
            allowed_class_names = [name for name in self._document._subclasses if name in class_names]
            if len(allowed_class_names) == 1:
                queryset._initial_query = {"_cls": allowed_class_names[0]}
            else:
                queryset._initial_query = {"_cls": {"$in": allowed_class_names}}
            return queryset
        else:
            return self

    def exclude_classes(self, *classes):
        doc = self._document
        if doc._meta.get('allow_inheritance') is True:
            queryset = self.clone()
            class_names = [cls._class_name for cls in classes]
            allowed_class_names = [name for name in self._document._subclasses if name in class_names]
            if len(allowed_class_names) == 1:
                queryset._initial_query = {"_cls": {"$ne": allowed_class_names[0]}}
            else:
                queryset._initial_query = {"_cls": {"$nin": allowed_class_names}}
            return queryset
        else:
            return self

    def clone(self):
        """Creates a copy of the current
          :class:`~mongoengine.queryset.QuerySet`

        .. versionadded:: 0.5
        """
        c = self.__class__(self._document, self._collection_obj)

        copy_props = (
            '_mongo_query', '_initial_query', '_none', '_query_obj',
            '_loaded_fields', '_ordering', '_timeout',
            '_class_check', '_read_preference', '_iter', '_scalar',
            '_as_pymongo', '_as_pymongo_coerce', '_limit', '_skip', '_hint',
            '_batch_size', '_auto_dereference'
        )

        for prop in copy_props:
            val = getattr(self, prop)
            setattr(c, prop, copy.copy(val))

        if self._cursor_obj:
            c._cursor_obj = self._cursor_obj.clone()

        return c

    def select_related(self, max_depth=1):
        """Handles dereferencing of :class:`~bson.dbref.DBRef` objects or
        :class:`~bson.object_id.ObjectId` a maximum depth in order to cut down
        the number queries to mongodb.

        .. versionadded:: 0.5
        """
        # Make select related work the same for querysets
        max_depth += 1
        queryset = self.clone()
        return queryset._dereference(queryset, max_depth=max_depth)

    def limit(self, n):
        """Limit the number of returned documents to `n`. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[:5]``).

        :param n: the maximum number of objects to return
        """
        queryset = self.clone()
        if n == 0:
            queryset._cursor.limit(1)
        else:
            queryset._cursor.limit(n)
        queryset._limit = n
        # Return self to allow chaining
        return queryset

    def skip(self, n):
        """Skip `n` documents before returning the results. This may also be
        achieved using array-slicing syntax (e.g. ``User.objects[5:]``).

        :param n: the number of objects to skip before returning results
        """
        queryset = self.clone()
        queryset._cursor.skip(n)
        queryset._skip = n
        return queryset

    def hint(self, index=None):
        """Added 'hint' support, telling Mongo the proper index to use for the
        query.

        Judicious use of hints can greatly improve query performance. When
        doing a query on multiple fields (at least one of which is indexed)
        pass the indexed field as a hint to the query.

        Hinting will not do anything if the corresponding index does not exist.
        The last hint applied to this cursor takes precedence over all others.

        .. versionadded:: 0.5
        """
        queryset = self.clone()
        queryset._cursor.hint(index)
        queryset._hint = index
        return queryset

    def batch_size(self, size):
        queryset = self.clone()
        queryset._cursor.batch_size(size)
        queryset._batch_size = size
        return queryset

    def distinct(self, field, dereference=True):
        """Return a list of distinct values for a given field.

        :param field: the field to select distinct values from
        :param dereference: specify if the returned distinct values should be
            dereferenced. `False` improves the performance greatly for
            ReferenceFields (if you only need the ids, not objects).

        .. note:: This is a command and won't take ordering or limit into
           account.

        .. versionadded:: 0.4
        .. versionchanged:: 0.5 - Fixed handling references
        .. versionchanged:: 0.6 - Improved db_field refrence handling
        """
        queryset = self.clone()
        try:
            field = self._fields_to_dbfields([field]).pop()
        finally:
            values = queryset._cursor.distinct(field)
            if dereference:
                values = self._dereference(values, 1, name=field,
                                           instance=self._document)
            return values

    def only(self, *fields):
        """Load only a subset of this document's fields. ::

            post = BlogPost.objects(...).only("title", "author.name")

        .. note :: `only()` is chainable and will perform a union ::
            So with the following it will fetch both: `title` and `author.name`::

                post = BlogPost.objects.only("title").only("author.name")

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to include

        .. versionadded:: 0.3
        .. versionchanged:: 0.5 - Added subfield support
        """
        fields = dict([(f, QueryFieldList.ONLY) for f in fields])
        return self.fields(True, **fields)

    def exclude(self, *fields):
        """Opposite to .only(), exclude some document's fields. ::

            post = BlogPost.objects(...).exclude("comments")

        .. note :: `exclude()` is chainable and will perform a union ::
            So with the following it will exclude both: `title` and `author.name`::

                post = BlogPost.objects.exclude("title").exclude("author.name")

        :func:`~mongoengine.queryset.QuerySet.all_fields` will reset any
        field filters.

        :param fields: fields to exclude

        .. versionadded:: 0.5
        """
        fields = dict([(f, QueryFieldList.EXCLUDE) for f in fields])
        return self.fields(**fields)

    def fields(self, _only_called=False, **kwargs):
        """Manipulate how you load this document's fields.  Used by `.only()`
        and `.exclude()` to manipulate which fields to retrieve.  Fields also
        allows for a greater level of control for example:

        Retrieving a Subrange of Array Elements:

        You can use the $slice operator to retrieve a subrange of elements in
        an array. For example to get the first 5 comments::

            post = BlogPost.objects(...).fields(slice__comments=5)

        :param kwargs: A dictionary identifying what to include

        .. versionadded:: 0.5
        """

        # Check for an operator and transform to mongo-style if there is
        operators = ["slice"]
        cleaned_fields = []
        for key, value in list(kwargs.items()):
            parts = key.split('__')
            op = None
            if parts[0] in operators:
                op = parts.pop(0)
                value = {'$' + op: value}
            key = '.'.join(parts)
            cleaned_fields.append((key, value))

        fields = sorted(cleaned_fields, key=operator.itemgetter(1))
        queryset = self.clone()
        for value, group in itertools.groupby(fields, lambda x: x[1]):
            fields = [field for field, value in group]
            fields = queryset._fields_to_dbfields(fields)
            queryset._loaded_fields += QueryFieldList(fields, value=value, _only_called=_only_called)

        return queryset

    def all_fields(self):
        """Include all fields. Reset all previously calls of .only() or
        .exclude(). ::

            post = BlogPost.objects.exclude("comments").all_fields()

        .. versionadded:: 0.5
        """
        queryset = self.clone()
        queryset._loaded_fields = QueryFieldList(
            always_include=queryset._loaded_fields.always_include)
        return queryset

    def order_by(self, *keys):
        """Order the :class:`~mongoengine.queryset.QuerySet` by the keys. The
        order may be specified by prepending each of the keys by a + or a -.
        Ascending order is assumed.

        :param keys: fields to order the query results by; keys may be
            prefixed with **+** or **-** to determine the ordering direction
        """
        queryset = self.clone()
        queryset._ordering = queryset._get_order_by(keys)
        return queryset

    def clear_cls_query(self):
        """ Sometimes we don't want the default _cls filter to be included in
        the query. This is a method to clear it.
        """
        queryset = self.clone()
        queryset._initial_query = {}
        return queryset

    def explain(self, format=False):
        """Return an explain plan record for the
        :class:`~mongoengine.queryset.QuerySet`\ 's cursor.

        :param format: format the plan before returning it
        """
        plan = self._cursor.explain()
        if format:
            plan = pprint.pformat(plan)
        return plan

    def timeout(self, enabled):
        """Enable or disable the default mongod timeout when querying.

        :param enabled: whether or not the timeout is used

        ..versionchanged:: 0.5 - made chainable
        """
        queryset = self.clone()
        queryset._timeout = enabled
        return queryset

    def read_preference(self, read_preference):
        """Change the read_preference when querying.

        :param read_preference: read preference to use instead of the
            connection-level preference.
        """
        validate_read_preference('read_preference', read_preference)
        queryset = self.clone()
        queryset._read_preference = read_preference
        queryset._cursor_obj = None  # we need to re-create the cursor object whenever we apply read_preference
        return queryset

    def read_concern(self, read_concern):
        """Change the read_concern when querying.

        :param read_concern: override ReplicaSetConnection-level
            preference.
        """
        if read_concern is not None and not isinstance(read_concern, ReadConcern):
            raise TypeError("%r is not a read concern." % (read_concern,))

        queryset = self.clone()
        queryset._read_concern = read_concern
        queryset._cursor_obj = None  # we need to re-create the cursor object whenever we apply read_concern
        return queryset

    def scalar(self, *fields):
        """Instead of returning Document instances, return either a specific
        value or a tuple of values in order.

        Can be used along with
        :func:`~mongoengine.queryset.QuerySet.no_dereference` to turn off
        dereferencing.

        .. note:: This effects all results and can be unset by calling
                  ``scalar`` without arguments. Calls ``only`` automatically.

        :param fields: One or more fields to return instead of a Document.
        """
        queryset = self.clone()
        queryset._scalar = list(fields)

        if fields:
            queryset = queryset.only(*fields)
        else:
            queryset = queryset.all_fields()

        return queryset

    def values_list(self, *fields):
        """An alias for scalar"""
        return self.scalar(*fields)

    def as_pymongo(self, coerce_types=False):
        """Instead of returning Document instances, return raw values from
        pymongo.

        :param coerce_type: Field types (if applicable) would be use to
            coerce types.
        """
        queryset = self.clone()
        queryset._as_pymongo = True
        queryset._as_pymongo_coerce = coerce_types
        return queryset

    # JSON Helpers

    def to_json(self, json_options=None):
        """Converts a queryset to JSON"""
        if json_options is None:
            warnings.warn(
                "No 'json_options' are specified! Falling back to "
                "LEGACY_JSON_OPTIONS with uuid_representation=PYTHON_LEGACY. "
                "For use with other MongoDB drivers specify the UUID "
                "representation to use. This will be changed to "
                "uuid_representation=UNSPECIFIED in a future release.",
                DeprecationWarning,
                stacklevel=2,
            )
            json_options = LEGACY_JSON_OPTIONS
        return json_util.dumps(self.as_pymongo(), json_options=json_options)

    def from_json(self, json_data):
        """Converts json data to unsaved objects"""
        son_data = json_util.loads(json_data)
        return [self._document._from_son(data) for data in son_data]

    # Basic aggregations

    def sum(self, field):
        """Sum over the values of the specified field.

        :param field: the field to sum over; use dot notation to refer to
            embedded document fields
        """
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {'$match': self._query},
            {'$group': {'_id': 'sum', 'total': {'$sum': '$' + db_field}}}
        ]
        result = tuple(self._document._get_collection().aggregate(pipeline))
        if result:
            return result[0]['total']
        return 0

    def average(self, field):
        db_field = self._fields_to_dbfields([field]).pop()
        pipeline = [
            {'$match': self._query},
            {'$group': {'_id': 'avg', 'total': {'$avg': '$' + db_field}}}
        ]
        result = tuple(self._document._get_collection().aggregate(pipeline))
        if result:
            return result[0]['total']
        return 0

    # Iterator helpers

    def __next__(self):
        """Wrap the result in a :class:`~mongoengine.Document` object.
        """
        if self._limit == 0 or self._none:
            raise StopIteration

        raw_doc = next(self._cursor)
        if self._as_pymongo:
            return self._get_as_pymongo(raw_doc)

        doc = self._document._from_son(raw_doc)
        if self._scalar:
            return self._get_scalar(doc)

        return doc

    def rewind(self):
        """Rewind the cursor to its unevaluated state.

        .. versionadded:: 0.3
        """
        self._iter = False
        self._cursor.rewind()

    # Properties

    @property
    def _collection(self):
        """Property that returns the collection object. This allows us to
        perform operations only if the collection is accessed.
        """
        return self._collection_obj

    @property
    def _cursor_args(self):
        cursor_args = {}
        if not self._timeout:
            cursor_args['no_cursor_timeout'] = True
        if self._loaded_fields:
            cursor_args['projection'] = self._loaded_fields.as_dict()
        return cursor_args

    @property
    def _cursor(self):
        if self._cursor_obj is None:

            # Create a new PyMongo cursor.
            # XXX In PyMongo 3+, we define the read preference on a collection
            # level, not a cursor level. Thus, if read preference is defined,
            # we need to get a cloned collection object using `with_options`
            # first.
            if self._read_preference is not None or self._read_concern is not None:
                self._cursor_obj = (
                    self._collection
                        .with_options(read_preference=self._read_preference, read_concern=self._read_concern)
                        .find(self._query, **self._cursor_args)
                )
            else:
                self._cursor_obj = self._collection.find(self._query,
                                                         **self._cursor_args)

            if self._ordering:
                # Apply query ordering
                self._cursor_obj.sort(self._ordering)
            elif self._ordering == None and self._document._meta['ordering']:
                # Otherwise, apply the ordering from the document model
                order = self._get_order_by(self._document._meta['ordering'])
                self._cursor_obj.sort(order)

            if self._limit is not None:
                self._cursor_obj.limit(self._limit)

            if self._skip is not None:
                self._cursor_obj.skip(self._skip)

            if self._hint != -1:
                self._cursor_obj.hint(self._hint)

            if self._batch_size is not None:
                self._cursor_obj.batch_size(self._batch_size)

        return self._cursor_obj

    def __deepcopy__(self, memo):
        """Essential for chained queries with ReferenceFields involved"""
        return self.clone()

    @property
    def _query(self):
        if self._mongo_query is None:
            self._mongo_query = self._query_obj.to_query(self._document)
            if self._class_check:
                self._mongo_query.update(self._initial_query)

        # Simplify a { '$and': [...], ... } query if possible.
        if '$and' in self._mongo_query:
            parent_keys_set = set(self._mongo_query.keys()) - set(['$and'])
            children_keys = [key for child in self._mongo_query['$and'] for key in list(child.keys())]

            # We can simplify if there are no collisions between the keys, i.e.
            # no duplicates in children_keys and no intersection between the
            # children and parent keys.
            if len(parent_keys_set | set(children_keys)) == len(list(parent_keys_set) + children_keys):
                # OK to simplify.
                and_query = self._mongo_query.pop('$and')
                for child in and_query:
                    self._mongo_query.update(child)
        return self._mongo_query

    @property
    def _dereference(self):
        if not self.__dereference:
            self.__dereference = _import_class('DeReference')()
        return self.__dereference

    def no_dereference(self):
        """Turn off any dereferencing for the results of this queryset.
        """
        queryset = self.clone()
        queryset._auto_dereference = False
        return queryset

    # Helper Functions

    def _fields_to_dbfields(self, fields):
        """Translate fields paths to its db equivalents"""
        ret = []
        for field in fields:
            field = ".".join(f.db_field for f in
                             self._document._lookup_field(field.split('.')))
            ret.append(field)
        return ret

    def _get_order_by(self, keys):
        """Creates a list of order by fields
        """
        key_list = []
        for key in keys:
            if not key:
                continue
            direction = pymongo.ASCENDING
            if key[0] == '-':
                direction = pymongo.DESCENDING
            if key[0] in ('-', '+'):
                key = key[1:]
            key = key.replace('__', '.')
            try:
                key = self._document._translate_field_name(key)
            except:
                pass
            key_list.append((key, direction))

        if self._cursor_obj:
            self._cursor_obj.sort(key_list)

        return key_list

    def _get_scalar(self, doc):

        def lookup(obj, name):
            chunks = name.split('__')
            for chunk in chunks:
                obj = getattr(obj, chunk)
            return obj

        data = [lookup(doc, n) for n in self._scalar]
        if len(data) == 1:
            return data[0]

        return tuple(data)

    def _get_as_pymongo(self, row):
        # Extract which fields paths we should follow if .fields(...) was
        # used. If not, handle all fields.
        if not getattr(self, '__as_pymongo_fields', None):
            self.__as_pymongo_fields = []
            for field in self._loaded_fields.fields - set(['_cls']):
                self.__as_pymongo_fields.append(field)
                while '.' in field:
                    field, _ = field.rsplit('.', 1)
                    self.__as_pymongo_fields.append(field)

        all_fields = not self.__as_pymongo_fields

        def clean(data, path=None):
            path = path or ''

            if isinstance(data, dict):
                new_data = {}
                for key, value in data.items():
                    new_path = '%s.%s' % (path, key) if path else key

                    if all_fields:
                        include_field = True
                    elif self._loaded_fields.value == QueryFieldList.ONLY:
                        include_field = new_path in self.__as_pymongo_fields
                    else:
                        include_field = new_path not in self.__as_pymongo_fields

                    if include_field:
                        new_data[key] = clean(value, path=new_path)
                data = new_data
            elif isinstance(data, list):
                data = [clean(d, path=path) for d in data]
            else:
                if self._as_pymongo_coerce:
                    # If we need to coerce types, we need to determine the
                    # type of this field and use the corresponding
                    # .to_python(...)
                    from mongoengine.fields import EmbeddedDocumentField
                    obj = self._document
                    for chunk in path.split('.'):
                        obj = getattr(obj, chunk, None)
                        if obj is None:
                            break
                        elif isinstance(obj, EmbeddedDocumentField):
                            obj = obj.document_type
                    if obj and data is not None:
                        data = obj.to_python(data)
            return data
        return clean(row)

    def _sub_js_fields(self, code):
        """When fields are specified with [~fieldname] syntax, where
        *fieldname* is the Python name of a field, *fieldname* will be
        substituted for the MongoDB name of the field (specified using the
        :attr:`name` keyword argument in a field's constructor).
        """
        def field_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split('.')
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return '["%s"]' % fields[-1].db_field

        def field_path_sub(match):
            # Extract just the field name, and look up the field objects
            field_name = match.group(1).split('.')
            fields = self._document._lookup_field(field_name)
            # Substitute the correct name for the field into the javascript
            return ".".join([f.db_field for f in fields])

        code = re.sub('\[\s*~([A-z_][A-z_0-9.]+?)\s*\]', field_sub, code)
        code = re.sub('\{\{\s*~([A-z_][A-z_0-9.]+?)\s*\}\}', field_path_sub,
                      code)
        return code

    # Deprecated
    def ensure_index(self, **kwargs):
        """Deprecated use :func:`Document.ensure_index`"""
        msg = ("Doc.objects()._ensure_index() is deprecated. "
               "Use Doc.ensure_index() instead.")
        warnings.warn(msg, DeprecationWarning)
        self._document.__class__.ensure_index(**kwargs)
        return self

    def _ensure_indexes(self):
        """Deprecated use :func:`~Document.ensure_indexes`"""
        msg = ("Doc.objects()._ensure_indexes() is deprecated. "
               "Use Doc.ensure_indexes() instead.")
        warnings.warn(msg, DeprecationWarning)
        self._document.__class__.ensure_indexes()
