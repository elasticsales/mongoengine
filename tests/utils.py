import functools
import operator

import pymongo
import pytest

from mongoengine.mongodb_support import get_mongodb_version

PYMONGO_VERSION = tuple(pymongo.version_tuple[:2])


def get_as_pymongo(doc):
    """Fetch the pymongo version of a certain Document"""
    return doc.__class__.objects.as_pymongo().get(id=doc.id)


def requires_mongodb_lt_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.lt)


def requires_mongodb_gte_40(func):
    return _decorated_with_ver_requirement(func, (4, 0), oper=operator.ge)


def requires_mongodb_gte_42(func):
    return _decorated_with_ver_requirement(func, (4, 2), oper=operator.ge)


def requires_mongodb_gte_44(func):
    return _decorated_with_ver_requirement(func, (4, 4), oper=operator.ge)


def requires_mongodb_gte_50(func):
    return _decorated_with_ver_requirement(func, (5, 0), oper=operator.ge)


def requires_mongodb_gte_60(func):
    return _decorated_with_ver_requirement(func, (6, 0), oper=operator.ge)


def requires_mongodb_gte_70(func):
    return _decorated_with_ver_requirement(func, (7, 0), oper=operator.ge)

try:
    from PIL import Image as _
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

def requires_pil(func):
    @functools.wraps(func)
    def _inner(*args, **kwargs):
        if HAS_PIL:
            return func(*args, **kwargs)
        else:
            pytest.skip("PIL not installed")

def _decorated_with_ver_requirement(func, mongo_version_req, oper):
    """Return a MongoDB version requirement decorator.

    The resulting decorator will skip the test if the current
    MongoDB version doesn't match the provided version/operator.

    For example, if you define a decorator like so:

        def requires_mongodb_gte_36(func):
            return _decorated_with_ver_requirement(
                func, (3.6), oper=operator.ge
            )

    Then tests decorated with @requires_mongodb_gte_36 will be skipped if
    ran against MongoDB < v3.6.

    :param mongo_version_req: The mongodb version requirement (tuple(int, int))
    :param oper: The operator to apply (e.g. operator.ge)
    """

    @functools.wraps(func)
    def _inner(*args, **kwargs):
        mongodb_v = get_mongodb_version()
        if oper(mongodb_v, mongo_version_req):
            return func(*args, **kwargs)
        else:
            pretty_version = ".".join(str(n) for n in mongo_version_req)
            pytest.skip(f"Needs MongoDB {oper.__name__} v{pretty_version}")

    return _inner

