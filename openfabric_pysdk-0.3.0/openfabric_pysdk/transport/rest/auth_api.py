from flask import request
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.logger import logger
from openfabric_pysdk.transport import ResourceDescriptor

from openfabric_pysdk.auth import session_manager

from .rest_api import WebApi

import uuid

#######################################################
#  Challenge API
#######################################################
class ChallengeApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Challenge", tags=["App"])
    def post(self) -> str:
        logger.debug(f"onChallengeRequest: {request.json}")
        errorMsg = None
        def set_error(error):
            nonlocal errorMsg
            errorMsg = error

        response = session_manager.update_challenge(request.json, set_error)
        if errorMsg:
            return errorMsg
        if response:
            return response


#######################################################
#  Auth by challenge API
#######################################################
class AuthApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="AuthApi", tags=["App"])
    def post(self) -> str:
        logger.debug(f"onAuthenticationByChallenge: {request.json}")
        sid = uuid.uuid4().hex
        return session_manager.authorize(sid, request.json)