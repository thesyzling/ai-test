from openfabric_pysdk.loader import *
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.service.environment_service import EnvironmentService
from openfabric_pysdk.transport import ResourceDescriptor
from openfabric_pysdk.transport.schema import ManifestSchema

from .rest_api import WebApi


#######################################################
#  Manifest API
#######################################################
class ManifestApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Get APP manifest", tags=["Developer"])
    @marshal_with(ManifestSchema)
    def get(self):
        self.check_user()

        with measure_block_time("ManifestRestApi::get"):
            _manifest = self._descriptor.app.get_manifest().all()
            _manifest["dos"] = EnvironmentService.get("DOS_CONNECTION")
            _manifest["dev"] = True if EnvironmentService.get("DEV_MODE") else False
            return _manifest
