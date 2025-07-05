from dataclasses import dataclass

from marshmallow import Schema
from marshmallow import fields

from openfabric_pysdk.service.hash_service import HashService
from openfabric_pysdk.service.resource_service import ResourceService


class Resource(fields.Field):
    __type: str = None
    __encoding: str = None
    __schema: Schema = None

    # ------------------------------------------------------------------------
    def __init__(self, resource_type='data', resource_encoding='blob', schema: Schema = None, *args, **kwargs):
        self.__encoding = resource_encoding
        self.__schema = schema
        self.__type = resource_type
        super().__init__(*args, **kwargs)

    # ------------------------------------------
    def _jsonschema_type_mapping(self):
        title = self.__dict__.get('name', '')
        metadata = {}
        if self.metadata:
            metadata = self.metadata
            title = metadata.get('title', title)

        schema = {
            'title': title,
            'resource_type': self.__type,
            'type': ['string', 'null'] if self.allow_none else 'string',
            'is_resource': True,
            'resource_encoding': self.__encoding,
        }
        # other metadata
        schema.update(metadata)
        return schema

    # ------------------------------------------------------------------------
    def _serialize(self, value, attr, obj, **kwargs):
        resource_hash = HashService.compute_hash(value)
        content = self.__schema.dumps(value) if self.__schema is not None else value
        return ResourceService.write(content, resource_hash, self.__type, self.__encoding)

    # ------------------------------------------------------------------------
    def _deserialize(self, value, attr, data, **kwargs):
        content, mime = ResourceService.read(value)
        return content
