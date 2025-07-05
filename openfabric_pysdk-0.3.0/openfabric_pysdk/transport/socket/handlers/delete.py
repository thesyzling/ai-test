from typing import Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace


#################################################
#  DeleteController
#################################################
class DeleteController:
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
        with measure_block_time("OpenfabricSocket::delete"):
            self.__engine.delete(qid, self.__supervisor)
