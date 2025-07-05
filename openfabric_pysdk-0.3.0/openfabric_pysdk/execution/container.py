from typing import Any

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.context import StateStatus
from openfabric_pysdk.flask.core import Webserver
from openfabric_pysdk.flask.rest import WebserverRestAPI, WebserverRestDoc
from openfabric_pysdk.loader import ConfigSchema
from openfabric_pysdk.transport import ResourceDescriptor
from openfabric_pysdk.transport.socket import ExecutionSocket

from openfabric_pysdk.execution.profile import Profile
from openfabric_pysdk.service.config_service import ConfigService
from openfabric_pysdk.service.rest_service import RestService
from openfabric_pysdk.service.swagger_service import SwaggerService
from openfabric_pysdk.service.socket_service import SocketService


#######################################################
#  Container
#######################################################
class Container:
    __profile: Profile = None
    __websocket: [ExecutionSocket] = None
    __webserver: Webserver = None
    __rest: WebserverRestAPI = None
    __docs: WebserverRestDoc = None

    # ------------------------------------------------------------------------
    def __init__(self, profile: Profile, webserver: Webserver):
        self.__profile = profile
        self.__webserver = webserver
        self.__rest = WebserverRestAPI(webserver)
        self.__docs = WebserverRestDoc(webserver)

    # ------------------------------------------------------------------------
    def start(self, supervisor: Supervisor):
        # Expose services
        self.__expose_swagger(supervisor)
        self.__expose_rest(supervisor)
        self.__expose_socket(supervisor)
        # Apply configuration
        ConfigService.apply(supervisor)

        # Running state will be signaled by the worker
        # supervisor.set_status(StateStatus.RUNNING)

        supervisor.set_notification_service(self.__websocket)

        # Start container
        # this will start eventing
        self.__websocket.run(
            debug=self.__profile.debug,
            host=self.__profile.host,
            port=self.__profile.port
        )

    # ------------------------------------------------------------------------
    def __expose_swagger(self, supervisor: Supervisor):
        descriptor = self.__descriptor(app=supervisor)
        SwaggerService.install(descriptor, webserver=self.__webserver)

    # ------------------------------------------------------------------------
    def __expose_socket(self, supervisor: Supervisor):
        from openfabric_pysdk.transport.socket import ExecutionSocket
        descriptor = self.__descriptor(handler=ExecutionSocket, endpoint='/app', app=supervisor)
        self.__websocket = SocketService.install(descriptor, webserver=self.__webserver)

    # ------------------------------------------------------------------------
    def __expose_rest(self, supervisor: Supervisor):

        from openfabric_pysdk.transport.rest import \
            ConfigApi, \
            ExecutionApi, \
            ResourceApi, \
            ManifestApi, \
            SchemaApi, \
            BenchmarkApi, \
            AuthApi, \
            ChallengeApi, \
            QueueGetApi, QueuePostApi, QueueListApi, QueueDeleteApi

        self.__install_rest(ExecutionApi, '/execution', supervisor)
        self.__install_rest(ResourceApi, '/resource', supervisor)
        self.__install_rest(SchemaApi, '/schema', supervisor)
        self.__install_rest(ManifestApi, '/manifest', supervisor)
        self.__install_rest(BenchmarkApi, '/benchmark', supervisor)
        self.__install_rest(QueueGetApi, '/queue/get', supervisor)
        self.__install_rest(QueueListApi, '/queue/list', supervisor)
        self.__install_rest(QueuePostApi, '/queue/post', supervisor)
        self.__install_rest(QueueDeleteApi, '/queue/delete', supervisor)
        self.__install_rest(ChallengeApi, '/challenge', supervisor)
        self.__install_rest(AuthApi, '/auth', supervisor)
        if ConfigSchema is not None:
            self.__install_rest(ConfigApi, '/config', supervisor)

    # ------------------------------------------------------------------------
    def __install_rest(self, handler: type, endpoint: str, supervisor: Supervisor):
        descriptor = self.__descriptor(handler=handler, endpoint=endpoint, app=supervisor)
        RestService.install(descriptor, rest=self.__rest, docs=self.__docs)

    # ------------------------------------------------------------------------
    def __descriptor(self, handler=None, endpoint=None, app=None):
        descriptor = ResourceDescriptor()
        descriptor.app = app
        descriptor.handler = handler
        descriptor.endpoint = endpoint
        return descriptor
