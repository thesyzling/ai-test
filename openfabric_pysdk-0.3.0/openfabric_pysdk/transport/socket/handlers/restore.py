from typing import Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.service import PersistenceService


#################################################
#  RestoreController
#################################################
class RestoreController:
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
    def process(self, qid: str):
        sid = request.sid
        ray_dump = PersistenceService.get_asset(qid, 'ray')
        input_dump = PersistenceService.get_asset(qid, 'in')
        output_dump = PersistenceService.get_asset(qid, 'out')
        self.__namespace.emit('restore', dict(input=input_dump, output=output_dump, ray=ray_dump), room=request.sid)
