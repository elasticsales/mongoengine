"""
Helper functions, constants, and types to aid with PyMongo support.
"""

import pymongo

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])


def list_collection_names(db, include_system_collections=False):
    """Pymongo>3.7 deprecates collection_names in favour of list_collection_names"""
    if PYMONGO_VERSION >= (3, 7):
        collections = db.list_collection_names()
    else:
        collections = db.collection_names()

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
