from openfabric_pysdk.fields import JApiSchema, japifields, post_load
from openfabric_pysdk.utility import SchemaUtil


#######################################################
# Property
#######################################################
class Property:
    name: str = None
    value: str = None

    def __init__(self, name: str = None, value: str = None):
        self.name = name
        self.value = value

    def __str__(self):
        return f"Property(name={self.name}, value={self.value})"

    def __repr__(self):
        return self.__str__()


#######################################################
#  Property Schema
#######################################################
class PropertySchema(JApiSchema):
    class Meta:
        type_ = "appProperty"
        strict = True
        unknown = 'exclude'

    id = japifields.Str(dump_only=True)
    name = japifields.String(description="Property name", allow_none=False)
    value = japifields.String(description="Property value", allow_none=True)

    def __init__(self):
        super().__init__(many=False)

    @post_load
    def create(self, data, **kwargs):
        return SchemaUtil.create(Property(), data)
