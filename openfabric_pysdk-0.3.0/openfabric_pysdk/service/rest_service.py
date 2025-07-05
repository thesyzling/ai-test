from openfabric_pysdk.flask.rest import WebserverRestDoc, WebserverRestAPI
from openfabric_pysdk.logger import logger
from openfabric_pysdk.transport import ResourceDescriptor


#######################################################
#  Rest service
#######################################################
class RestService:

    # ------------------------------------------------------------------------
    @staticmethod
    def install(descriptor: ResourceDescriptor, rest: WebserverRestAPI = None, docs: WebserverRestDoc = None):

        kwargs = dict(descriptor=descriptor)

        if rest is not None:
            logger.info(f"Openfabric - install {descriptor.handler} REST endpoints on {descriptor.endpoint}")
            rest.add_resource(descriptor.handler, descriptor.endpoint, resource_class_kwargs=kwargs)

        if docs is not None:
            logger.info(f"Openfabric - install {descriptor.handler} DOCS endpoints on {descriptor.endpoint}")
            docs.register(descriptor.handler, resource_class_kwargs=kwargs)
