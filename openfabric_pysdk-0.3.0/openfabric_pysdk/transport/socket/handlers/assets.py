from typing import Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.service import PersistenceService


#################################################
#  AssetsController
#################################################
class AssetsController:
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
        # Would prefer to use nonce, but for that it would be good to update the contents of the ray
        # in that case, it would not be necesary to request assets, but receive the state all the time.
        input_ts = PersistenceService.get_asset_timestamp(qid, 'in')
        output_ts = PersistenceService.get_asset_timestamp(qid, 'out')
        self.__namespace.emit('assets', dict(input_ts=input_ts, output_ts=output_ts, ray=ray_dump), room=request.sid)
