import threading
import uuid
from time import sleep
from typing import Dict, List, Optional

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import Ray, RaySchema, RayStatus
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.loader import getSchemaInst
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService
from openfabric_pysdk.task import Task
from openfabric_pysdk.utility import LRUCacheMap


#######################################################
#  Engine
#######################################################
class Engine:
    __supervisor: Supervisor = None
    __rays: Dict[str, Ray] = None
    __partial_outputs: LRUCacheMap = None
    __reported_partial_outputs: LRUCacheMap = None
    __task: Task = None
    __instances: int = 0
    __running: bool = False
    __current_qid: Optional[str] = None
    __worker: threading.Thread = None
    __lock: threading.Condition = threading.Condition()

    # ------------------------------------------------------------------------
    def __init__(self):
        self.__rays = dict()
        self.__partial_outputs = LRUCacheMap(3)
        self.__reported_partial_outputs = LRUCacheMap(3)
        self.__lock.acquire()
        if self.__instances == 0:
            self.__task = Task()

            # Populate rays on startup
            for qid in self.__task.all():
                ray = PersistenceService.get_asset(qid, 'ray', RaySchemaInst.load)
                if ray is None:
                    continue
                self.__rays[qid] = ray

            self.__worker = threading.Thread(target=self.__process, name="engine", args=())
            self.__worker.start()

        self.__instances = self.__instances + 1
        self.__lock.release()

        # Wait for processing thread to start
        while not self.__running:
            sleep(0.1)

    # ------------------------------------------------------------------------
    def __del__(self):
        self.__lock.acquire()
        if self.__instances > 0:
            self.__lock.release()
            return

        self.__running = False

        self.__lock.notify_all()
        self.__lock.release()

    # ------------------------------------------------------------------------
    def __process(self):
        self.__running = True
        while self.__running:
            self.__lock.acquire()
            self.__current_qid = None
            while self.__running and self.__task.empty():
                self.__lock.wait()
            try:
                self.__current_qid = self.__task.next()
            except Exception as e:
                logger.warning(f"Openfabric - queue empty! {e}")
            finally:
                self.__lock.release()

            if self.__running and self.__current_qid is not None:
                self.process(self.__current_qid)

    # ------------------------------------------------------------------------
    def prepare(self, supervisor: Supervisor, data: str, qid=None, sid=None, uid=None, rid=None) -> str:
        self.__lock.acquire()
        if qid is None:
            qid: str = uuid.uuid4().hex
        ray = self.ray(qid)
        ray.status = RayStatus.QUEUED
        ray.sid = sid
        ray.uid = uid
        ray.rid = rid

        PersistenceService.set_asset(qid, 'ray', ray, RaySchemaInst.dump)
        PersistenceService.set_asset(qid, 'in', data)

        self.__supervisor = supervisor
        self.__task.add(qid)
        self.__lock.notify_all()
        self.__lock.release()
        return qid

    # ------------------------------------------------------------------------
    def ray(self, qid: str) -> Ray:
        if self.__rays.get(qid) is None:
            ray = Ray(qid=qid)
            self.__rays[qid] = ray
        return self.__rays[qid]

    # ------------------------------------------------------------------------
    def rays(self, criteria=None) -> List[Ray]:
        rays: List[Ray] = []
        for qid, ray in self.__rays.items():
            if criteria is None or criteria(ray):
                rays.append(ray)
        return rays

    # ------------------------------------------------------------------------
    def pending_rays(self, criteria=None) -> List[Ray]:
        rays: List[Ray] = []
        for qid, ray in self.__rays.items():
            if criteria is None or criteria(ray):
                rays.append(ray)
        rays.sort(key=lambda r: r.created_at)
        return rays

    # ------------------------------------------------------------------------
    def process(self, qid):

        if self.__supervisor is None:
            logger.error("Openfabric - no app configured!")
            return

        with measure_block_time("Engine::execution_callback_function"):
            ray = self.ray(qid)
            self.__supervisor.execution_callback_function(None, ray)
        output = PersistenceService.get_asset(qid, 'out', getSchemaInst('out').load)
        return output

    # ------------------------------------------------------------------------
    def delete(self, qid: str, supervisor: Supervisor = None) -> Ray:
        self.__lock.acquire()
        self.__task.rem(qid)
        ray = self.ray(qid)
        if supervisor is not None:
            supervisor.cancel_execution(ray)
        else:
            self.__supervisor.cancel_execution(ray)
        PersistenceService.drop_assets(qid)
        self.__rays.pop(qid)
        ray.status = RayStatus.REMOVED
        self.__lock.notify_all()
        self.__lock.release()
        return ray

    # ------------------------------------------------------------------------
    def partial_output(self, qid: str, partial: str):
        logger.debug(f"Openfabric - partial output: {partial}")
        self.__partial_outputs.put(qid, getSchemaInst('out').load(partial, partial=True))

    def get_partial_output(self, qid: str):
        return self.__partial_outputs.get(qid)

    def get_partial_output_ts(self, qid: str):
        return self.__partial_outputs.get_update_timestamp(qid)

    def reported_partial_output(self, qid: str, hash: str):
        logger.debug(f"Openfabric - reported partial output: {hash}")
        self.__reported_partial_outputs.put(qid, hash)

    def get_reported_partial_output(self, qid: str):
        return self.__reported_partial_outputs.get(qid)


engine = Engine()
