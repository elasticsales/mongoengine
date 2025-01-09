"""
Helper functions, constants, and types to aid with PyMongo support.
"""

import pymongo
from bson import binary, json_util

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])

# This will be changed to UuidRepresentation.UNSPECIFIED in a future
# (breaking) release.
if PYMONGO_VERSION >= (4,):
    LEGACY_JSON_OPTIONS = json_util.LEGACY_JSON_OPTIONS.with_options(
        uuid_representation=binary.UuidRepresentation.PYTHON_LEGACY,
    )
else:
    LEGACY_JSON_OPTIONS = json_util.DEFAULT_JSON_OPTIONS


def list_collection_names(db, include_system_collections=False):
    """Pymongo>3.7 deprecates collection_names in favour of list_collection_names"""
    if PYMONGO_VERSION >= (3, 7):
        collections = db.list_collection_names()
    else:
        collections = db.collection_names()

    if not include_system_collections:
        collections = [c for c in collections if not c.startswith("system.")]

    return collections
