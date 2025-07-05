import uuid
from datetime import datetime, timedelta
from typing import Any

from flask import Response, jsonify, make_response, request
from marshmallow import ValidationError, fields

from openfabric_pysdk.engine import engine
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.service.resource_service import ResourceService
from openfabric_pysdk.transport import ResourceDescriptor
from .rest_api import WebApi


#######################################################
#  Execution API
#######################################################
class ExecutionApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Execute execution and get response", tags=["App"])
    @use_kwargs(InputSchema, location='json')
    @marshal_with(OutputSchema)
    def post(self, *args) -> OutputClass:
        uid = self.check_user()
        schema = getSchemaInst('in')
        schemaOut = getSchemaInst('out')

        data = request.get_json()
        if not data:
            return jsonify({"error": "No payload received"}), 400

        validated_data = None

        try:
            validated_data = schema.load(data)
        except ValidationError as err:
            return jsonify({"error": err.messages}), 400

        # Leaving this so that api can still work with no authentication
        if uid is None:
            uid = "undefined"

        sid = uuid.uuid4().hex
        app = self._descriptor.app
        qid = engine.prepare(app, schema.dump(validated_data), sid=sid)
        return Response(str(schemaOut.dump(engine.process(qid))), mimetype='application/json')


class ResourceApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    @doc(description="Get the blob content for the indicated hash", tags=["App"])
    @use_kwargs({'reid': fields.String(required=True)}, location='query')
    def get(self, reid: str) -> Any:
        self.check_user()

        content, mime = ResourceService.read(reid)
        if content is None:
            response = make_response({"error": "File not found"}, 404)
            response.headers['Cache-Control'] = 'no-store, must-revalidate'  # Prevent caching
        else:
            response = make_response(content)
            response.mimetype = mime if mime is not None else 'text/plain'
            response.headers['Cache-Control'] = 'public, max-age=86400, must-revalidate'
            response.headers['CDN-Cache-Control'] = 'no-store'  # Prevent Cloudflare caching
            response.headers['CF-Cache-Status'] = 'BYPASS'  # Ensure Cloudflare does not cache
            response.headers['Expires'] = (datetime.utcnow() + timedelta(days=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")
            response.headers['ETag'] = reid  # Ensures client-side cache validation
            response.headers['Last-Modified'] = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        return response
