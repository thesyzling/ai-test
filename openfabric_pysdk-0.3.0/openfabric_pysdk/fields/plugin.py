from marshmallow import fields, Schema


class PluginField(fields.Field):
    def __init__(self, selector='string', path=None, schema: Schema = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.selector = selector
        self.path = path

    def _jsonschema_type_mapping(self):
        title = self.__dict__.get('name', '')
        metadata = {}
        if self.metadata:
            metadata = self.metadata
            title = metadata.get('title', title)

        schema = {
            'title': title,
            'selector': self.selector,
            'path': self.path,
            'type': ['string', 'null'] if self.allow_none else 'string',
        }
        # other metadata
        schema.update(metadata)
        return schema