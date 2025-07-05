from typing import Set

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import Ray
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace


#################################################
#  ResumeController
#################################################
class ResumeController:
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
        with measure_block_time("OpenfabricSocket::restore"):
            def criteria(_ray: Ray):
                # is deleted ?
                if _ray is None:
                    return False
                # is different user ?
                if _ray.uid != uid:
                    return False
                # is an active session available ?
                if _ray.sid in self.__sessions and _ray.sid != sid:
                    return False
                return True

            # Filter rays
            rays = self.__engine.pending_rays(criteria)
            for ray in rays:
                self.__namespace.emit('progress', RaySchemaInst.dump(ray), room=sid)
