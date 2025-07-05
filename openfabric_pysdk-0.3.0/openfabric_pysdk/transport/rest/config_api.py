from typing import List, Union
from marshmallow import ValidationError
from flask import request, jsonify

from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service.config_service import ConfigService
from openfabric_pysdk.transport import ResourceDescriptor
from openfabric_pysdk.transport.schema import UserId, UserIdSchema

from .rest_api import WebApi


#######################################################
#  Config API
#######################################################
class ConfigApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Get APP configuration", tags=["App"])
    @use_kwargs(UserIdSchema, location='query')
    def get(self, uid: UserId):
        self.check_user()

        with measure_block_time("ConfigRestApi::get"):
            app = self._descriptor.app
            return ConfigService.read(app, uid)

    # ------------------------------------------------------------------------
    @doc(description="Set APP configuration", tags=["App"])
    @use_kwargs(UserIdSchema, location='query')
    @use_kwargs(ConfigSchema, location='json')
    @marshal_with(ConfigSchema)
    def post(self, uid: UserId, config: Union[ConfigClass, List[ConfigClass]]) -> ConfigClass:
        self.check_user()
        schema = getSchemaInst('config')

        '''
        data = request.get_json()
        if not data:
            logger.error("No input data provided")
            return jsonify({"error": "No payload received"}), 400

        config = None

        try:
            config = schema.load(data)
        except ValidationError as err:
            logger.error(f"Validation error: {err.messages}")
            return jsonify({"error": err.messages}), 400
        '''

        with measure_block_time("ConfigRestApi::post"):
            app = self._descriptor.app
            return ConfigService.write(app, uid, config)
