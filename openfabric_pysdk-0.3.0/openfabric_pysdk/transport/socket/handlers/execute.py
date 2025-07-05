import uuid
from datetime import datetime, timedelta
from typing import Any, Set

import gevent

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.auth import session_link
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import Ray, RayStatus
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.engine.engine import Engine, engine as _engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService
from openfabric_pysdk.utility import ZipUtil


class NotificationManager:
    maximum_update_interval: int = 1
    health_check_interval: int = 3

    def __init__(self, ray) -> None:
        self.ray = ray
        self.published_ray_hash = None
        self.finished = False
        # Notify on first run
        self.next_ray_update = datetime.now()
        self.next_health_update = datetime.now()

    def is_finished(self):
        return self.ray.finished

    def get_updates(self):
        updates = []

        time = datetime.now()

        if time > self.next_health_update:
            updates.append(('pulse', dict(rid=self.ray.rid)))
            self.next_health_update = time + timedelta(seconds=self.health_check_interval)

        return updates


#################################################
#  ExecuteController
#################################################
class ExecuteController:
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
    def sync(self, qid: str, data: bytes) -> bool:
        sid = request.sid
        dictionary = ZipUtil.decompress(data)
        data = dictionary.get('body', None)
        header = dictionary.get('header', dict())
        uid: str = header.get('uid', None)
        if uid is None:
            logger.info("Socket::execute: uid is None")
            return False
        PersistenceService.set_asset(qid, 'in', data)

        self.__supervisor.sync(qid)
        return True

    # --------------------------------------------------------------------------------
    def process(self, data: bytes, background: bool):
        with measure_block_time("Socket::execute"):
            sid = request.sid
            dictionary = ZipUtil.decompress(data)
            data = dictionary.get('body', None)
            header = dictionary.get('header', dict())
            uid: str = header.get('uid', None)
            rid: str = header.get('rid', uuid.uuid4().hex)

            logger.info(f"Socket::execute: sid={sid}, uid={uid}, rid={rid}, background={background}")
            if background is True:
                self.__execute_background(data, sid, uid, rid)
            else:
                self.__execute_foreground(data, sid, uid, rid)

    # --------------------------------------------------------------------------------
    def __execute_foreground(self, data: Any, sid: str, uid: str, rid: str):
        # Setup

        ray: Ray = self.__engine.ray(sid)

        # Skip the same request while pending
        if ray.status != RayStatus.UNKNOWN and ray.status != RayStatus.REMOVED:
            return

        qid = self.__engine.prepare(self.__supervisor, data, qid=sid, sid=sid, uid=uid, rid=rid)
        ray = self.__engine.ray(qid)
        # Execute in foreground
        with measure_block_time("Socket::callback"):
            self.__engine.process(qid)
            output = PersistenceService.get_asset(qid, 'out')
            output_ts = PersistenceService.get_asset_timestamp(qid, 'out')
            ray_dump = RaySchemaInst.dump(ray)
            self.__engine.delete(qid, self.__supervisor)
            self.__namespace.emit('response', dict(output=output, ray=ray_dump, output_ts=output_ts), room=request.sid)

    # --------------------------------------------------------------------------------
    def __execute_background(self, data: Any, sid: str, uid: str, rid: str):
        # Setup
        qid = self.__engine.prepare(self.__supervisor, data, sid=sid, uid=uid, rid=rid)
        ray = self.__engine.ray(qid)
        data = RaySchemaInst.dump(ray)
        data["input_ts"] = PersistenceService.get_asset_timestamp(qid, 'in')

        self.__namespace.emit('submitted', data, room=request.sid)

        nm = NotificationManager(ray)

        # Execute in background
        with measure_block_time("Socket::callback"):
            while sid in self.__sessions and not nm.is_finished():

                for update in nm.get_updates():
                    self.__namespace.emit(update[0], update[1], room=request.sid)

                if not nm.is_finished():
                    gevent.sleep(0.1)
