from openfabric_pysdk.benchmark import measure_block_time, timer_manager
from openfabric_pysdk.flask.rest import doc, marshal_with
from openfabric_pysdk.transport import ResourceDescriptor
from openfabric_pysdk.transport.schema import BenchmarkSchema

from .rest_api import WebApi


#######################################################
#  Benchmark API
#######################################################
class BenchmarkApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    # ------------------------------------------------------------------------
    @doc(description="Get APP benchmarks", tags=["Developer"])
    @marshal_with(BenchmarkSchema)
    def get(self):
        self.check_user()

        with measure_block_time("BenchmarkRestApi::get"):
            return timer_manager.get_all_timings_json()
