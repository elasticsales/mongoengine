from bson import json_util, binary

LEGACY_JSON_OPTIONS = json_util.LEGACY_JSON_OPTIONS.with_options(
    uuid_representation=binary.UuidRepresentation.PYTHON_LEGACY,
)

