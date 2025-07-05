from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.context import State


#######################################################
#  Resource descriptor
#######################################################
class ResourceDescriptor:
    app: Supervisor
    remote: str
    handler: type
    endpoint: str
