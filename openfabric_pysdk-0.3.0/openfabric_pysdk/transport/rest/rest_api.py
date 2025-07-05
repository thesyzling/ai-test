import uuid

from flask import abort, request

from openfabric_pysdk.auth import session_manager
from openfabric_pysdk.flask.rest import MethodResource, Resource
from openfabric_pysdk.transport import ResourceDescriptor


#######################################################
#  Base web API class
#######################################################
class WebApi(MethodResource, Resource):
    _descriptor: ResourceDescriptor = None

    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        self._descriptor = descriptor

    # ------------------------------------------------------------------------
    def check_user(self) -> str:
        if session_manager.is_locked():
            headers = request.headers
            token = headers.get("token", None)
            user = session_manager.get_user_by_token(token)
            if user is None:
                abort(401)
            return user
        return None
