from marshmallow import fields
from marshmallow_jsonschema import JSONSchema

from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.transport import ResourceDescriptor

from .rest_api import WebApi


#######################################################
#  SchemaApi API
#######################################################
class SchemaApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Get APP schema", tags=["Developer"])
    @use_kwargs({'type': fields.String(required=True)}, location='query')
    def get(self, type: str):
        self.check_user()

        with measure_block_time("SchemaApi::get"):
            if type == 'input':
                return JSONSchema().dump(getSchemaInst("in"))
            elif type == 'output':
                return JSONSchema().dump(getSchemaInst("out"))
            elif type == 'config':
                return JSONSchema().dump(getSchemaInst("config"))
            else:
                return f"Invalid schema type {type}", 400
