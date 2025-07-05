from typing import Dict, Literal, Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.flask.core import Webserver, request
from openfabric_pysdk.flask.socket import Namespace, SocketIOServer
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service.socket_service import SocketService
from openfabric_pysdk.transport import ResourceDescriptor
from openfabric_pysdk.transport.socket.handlers.assets import AssetsController
from openfabric_pysdk.transport.socket.handlers.capability import CapabilityController
from openfabric_pysdk.transport.socket.handlers.configure import ConfigureController
from openfabric_pysdk.transport.socket.handlers.delete import DeleteController
from openfabric_pysdk.transport.socket.handlers.execute import ExecuteController
from openfabric_pysdk.transport.socket.handlers.restore import RestoreController
from openfabric_pysdk.transport.socket.handlers.resume import ResumeController
from openfabric_pysdk.transport.socket.handlers.state import StateController
from openfabric_pysdk.transport.socket.handlers.watch import WatchController

from openfabric_pysdk.auth import session_manager, session_link


#######################################################
#  Execution Socket
#######################################################
class ExecutionSocket(Namespace):
    __supervisor: Supervisor = None
    __sessions: Set[str] = None
    __watchers: Dict[str, str] = None
    __webserver: Webserver = None
    __sioserver: SocketIOServer = None

    # ------------------------------------------------------------------------
    def __init__(self, webserver: Webserver, descriptor: ResourceDescriptor):
        super().__init__(descriptor.endpoint)
        self.__sessions = set()
        self.__watchers = dict()
        self.__supervisor = descriptor.app
        self.__webserver = webserver

    # ------------------------------------------------------------------------
    def is_authorized(self, sid: str) -> bool:
        status = session_manager.is_authorized(sid)
        if not status:
            logger.warning(f'Openfabric - Unauthorized request from {sid}')
            self.emit('error', {"status": "UNAUTHORIZED", "message": f"Unauthorized"}, room=sid)

        return status

    # ------------------------------------------------------------------------
    def run(self, debug: bool, host: str, port: int):
        self.__sioserver = SocketService.server(self.__webserver)
        self.__sioserver.on_namespace(self)
        self.__sioserver.run(host=host, debug=debug, port=port)

    # ------------------------------------------------------------------------
    def on_configure(self, config: bytes):
        if self.is_authorized(request.sid):
            ConfigureController(self.__supervisor, self, self.__sessions).process(config)

    # ------------------------------------------------------------------------
    def on_execute(self, data: bytes, background: bool):
        if self.is_authorized(request.sid):
            ExecuteController(self.__supervisor, self, self.__sessions).process(data, background)

    # ------------------------------------------------------------------------
    def on_resume(self, uid: str):
        if self.is_authorized(request.sid):
            session_link.register_user_session(uid, request.sid)
            ResumeController(self.__supervisor, self, self.__sessions).process(uid)

    # ------------------------------------------------------------------------
    def on_sync(self, qid: str, data: bytes):
        if self.is_authorized(request.sid):
            ExecuteController(self.__supervisor, self, self.__sessions).sync(qid, data)

    # ------------------------------------------------------------------------
    def on_restore(self, qid: str):
        if self.is_authorized(request.sid):
            RestoreController(self.__supervisor, self, self.__sessions).process(qid)

    # ------------------------------------------------------------------------
    def on_assets(self, qid: str):
        if self.is_authorized(request.sid):
            AssetsController(self.__supervisor, self, self.__sessions).process(qid)

    # ------------------------------------------------------------------------
    def on_watch(self, qid: str):
        if self.is_authorized(request.sid):
            WatchController(self.__supervisor, self, self.__sessions, self.__watchers).send_partial(qid)

    # ------------------------------------------------------------------------
    def on_reset_watch(self, qid: str):
        if self.is_authorized(request.sid):
            WatchController(self.__supervisor, self, self.__sessions, self.__watchers).reset_partial(qid)

    # ------------------------------------------------------------------------
    def on_state(self, uid: str):
        if self.is_authorized(request.sid):
            StateController(self.__supervisor, self, self.__sessions).process(uid)

    # ------------------------------------------------------------------------
    def on_delete(self, qid: str):
        if self.is_authorized(request.sid):
            DeleteController(self.__supervisor, self, self.__sessions).process(qid)

    # ------------------------------------------------------------------------
    def on_connect(self):
        sid = request.sid
        session_link.register_session(sid)
        logger.debug(f'Openfabric - client connected {sid} on {request.host}')
        self.__sessions.add(sid)
        CapabilityController(self.__supervisor, self, self.__sessions).process()

    # ------------------------------------------------------------------------
    def on_disconnect(self):
        sid = request.sid
        session_manager.unlink(sid)
        session_link.unregister_session(sid)
        logger.debug(f'Openfabric - client disconnected {sid} on {request.host}')
        self.__sessions.remove(sid)

    # ------------------------------------------------------------------------
    def on_challenge(self, challenge: Dict[str, str]):
        logger.debug(f"onChallengeRequest: {challenge}")
        response = session_manager.update_challenge(challenge, lambda error: self.emit("error", error))
        if response:
            self.emit("challenge", response, room=request.sid)

    # ------------------------------------------------------------------------
    def on_auth_by_challenge(self, challenge: Dict[str, str]):
        sid = request.sid
        logger.debug(f"onAuthenticationByChallenge: {challenge}")
        token = session_manager.authorize(sid, challenge)
        self.emit("token", token, room=request.sid)

    # ------------------------------------------------------------------------
    def on_auth_by_token(self, token: str):
        sid = request.sid
        logger.debug(f"onAuthenticationByToken: {token}")
        auth_status = session_manager.authenticate(sid, token)
        self.emit("auth", auth_status, room=request.sid)

    # ------------------------------------------------------------------------
    def on_permission(self, uid: str, permission: Literal['grant', 'revoke']):
        sid = request.sid
        logger.debug(f"onPermissionUpdate: {uid} {permission}")
        if self.is_authorized(sid):
            error = session_manager.update_user_permissions(sid, uid, permission)
            if error["status"] != "OK":
                self.emit("error", error, room=request.sid)