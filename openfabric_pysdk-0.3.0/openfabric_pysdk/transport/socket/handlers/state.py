from typing import Set

import gevent

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import StateSchema
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.utility import ChangeUtil


#################################################
#  StateController
#################################################
class StateController:
    __engine: Engine = None
    __sessions: Set[str] = None
    __namespace: Namespace = None
    __supervisor: Supervisor = None

    def __init__(self, supervisor: Supervisor, namespace: Namespace, sessions: Set[str]):
        self.__supervisor = supervisor
        self.__namespace = namespace
        self.__sessions = sessions
        self.__engine = _engine

    # --------------------------------------------------------------------------------
    def process(self, uid: str):
        sid = request.sid
        with measure_block_time("Socket::state"):
            while sid in self.__sessions:
                changed = ChangeUtil.is_changed('execution::state' + sid, self.__supervisor.state, StateSchema().dump)
                if changed is True:
                    self.__namespace.emit('state', StateSchema().dump(self.__supervisor.state), room=request.sid)
                gevent.sleep(0.1)
