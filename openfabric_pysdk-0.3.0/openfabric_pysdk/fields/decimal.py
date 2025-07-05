from decimal import Decimal
from marshmallow import fields, ValidationError


class DecimalField(fields.Field):
    
    def _serialize(self, value, attr, obj, **kwargs):
        return str(value) if value is not None else None

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return Decimal(value) if value is not None else None
        except:
            raise ValidationError("Invalid decimal number")