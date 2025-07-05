import base64
from marshmallow import fields, ValidationError


class Base64Field(fields.Field):
    # ------------------------------------------------------------------------
    def _serialize(self, value, attr, obj, **kwargs):
        if value is None:
            return None
        try:
            # Encode the value to Base64
            encoded_data = base64.b64encode(value.encode()).decode("utf-8")
            return f"data:image/jpeg;base64,{encoded_data}"

        except Exception as e:
            raise ValidationError("Could not encode value to Base64") from e

    # ------------------------------------------------------------------------
    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return None
        try:
            # Decode the Base64 value
            return base64.b64decode(value).decode("utf-8")
        except Exception as e:
            raise ValidationError("Could not decode Base64 value") from e
