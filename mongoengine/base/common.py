from mongoengine.errors import NotRegistered

__all__ = ('ALLOW_INHERITANCE', 'get_document', '_document_registry')

ALLOW_INHERITANCE = False

_registered_documents = set() # set of classes
_document_registry = {} # mapping name -> class

def _all_subclasses(cls):
    return cls.__subclasses__() + [g for s in cls.__subclasses__()
                                       for g in _all_subclasses(s)]

def register_all():
    from mongoengine.base.document import BaseDocument
    #global _registered_documents
    for cls in reversed(_all_subclasses(BaseDocument)):
        if cls not in _registered_documents:
            cls.register()

def get_document(name):
    doc = _document_registry.get(name, None)
    if not doc:
        raise NotRegistered("""
            `%s` has not been registered in the document registry.
            Importing the document class automatically registers it, has it
            been imported?
        """.strip() % name)
    return doc
