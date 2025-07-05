import copy
import json
import time
from typing import Dict, Set

import gevent

from openfabric_pysdk.app import Supervisor
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import Ray, RaySchema
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.engine import engine as _engine
from openfabric_pysdk.engine.engine import Engine
from openfabric_pysdk.flask.core import request
from openfabric_pysdk.flask.socket import Namespace
from openfabric_pysdk.loader import getSchemaInst
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService
from openfabric_pysdk.utility import JsonUtil

from openfabric_pysdk.service.resource_service import ResourceService


#################################################
#  WatchController
#################################################
class WatchController:
    __engine: Engine = None
    __sessions: Set[str] = None
    __namespace: Namespace = None
    __supervisor: Supervisor = None
    __watchers: Dict[str, str] = None

    last_output_update = -1
    output_update_ts = None
    prev_partial_output = None

    def __init__(self, supervisor: Supervisor, namespace: Namespace, sessions: Set[str], watchers: Dict[str, str]):
        self.__supervisor = supervisor
        self.__namespace = namespace
        self.__sessions = sessions
        self.__engine = _engine
        self.__watchers = watchers

    # --------------------------------------------------------------------------------
    def reset_partial(self, qid: str):
        if self.__engine.get_reported_partial_output(qid) is not None:
            logger.debug(f'Openfabric - clearing partial output for {qid}')
            self.__engine.reported_partial_output(qid, None)

    # --------------------------------------------------------------------------------
    def send_partial(self, qid: str):
        sid = request.sid
        self.__watchers[sid] = qid
        ray = self.__engine.ray(qid)

        logger.info(f"Socket::watch:  {time.time()} {sid}/{qid} started.")
        self.__engine.reported_partial_output(qid, None)

        # Execute in background
        with measure_block_time("Socket::watch"):
            while sid in self.__sessions and self.__watchers.get(sid) is qid:

                if ray.finished is True:
                    logger.info(f"Socket::watch: {time.time()} {sid}/{qid} finished - execution ended.")
                    break

                # Handle partial  ----------------
                partial_output = self.__handle_partial(ray)
                if partial_output is not None:
                    try:
                        logger.info(f"Socket::watch: {time.time()} {sid}/{qid} partial.")
                        self.__namespace.emit('partial', partial_output, room=sid)
                    except Exception as e:
                        logger.error(
                            f"Partial error: \n {e} "
                            f"\n {partial_output}")

                gevent.sleep(0.1)

                # TODO: might be worth to periodically send the full
                #  object to make sure we are in sync?
                #  or store hash of final object and compare?

        logger.info(f"Socket::watch:  {time.time()} {sid}/{qid} finished - replaced or session finished.")

    # ------------------------------------------------------------------------
    def __handle_partial(self, ray: Ray):
        qid = ray.qid

        if self.__engine.get_reported_partial_output(qid) is None:
            self.last_output_update = -1
            self.prev_partial_output = None

        self.output_update_ts = self.__engine.get_partial_output_ts(qid)
        if self.output_update_ts is None or (
                self.last_output_update is not None and self.output_update_ts == self.last_output_update):
            return None

        current_partial_update = self.__engine.get_partial_output(qid)
        if current_partial_update is not None:
            try:
                schema = getSchemaInst('out')

                refresh = self.last_output_update == -1
                base_json = json.loads('[]' if schema.many else '{}') if refresh else self.prev_partial_output
                ResourceService.lock(qid)
                try:
                    current_partial_update = schema.dump(current_partial_update)
                finally:
                    ResourceService.unlock()
                target_json = current_partial_update
                partial = JsonUtil.find_differences(base_json, target_json)
                partial['refresh'] = refresh

                # Don't send empty object even if change was published by the worker
                if partial["new_hash"] == self.__engine.get_reported_partial_output(qid):
                    return None

                self.prev_partial_output = copy.deepcopy(current_partial_update)
                self.last_output_update = self.output_update_ts
                partial["qid"] = qid
                self.__engine.reported_partial_output(qid, partial["new_hash"])

                # ------------------
                # Save partial output to store
                # TODO - very ugly hack !!! - need to fix
                PersistenceService.set_asset(qid, 'out', current_partial_update)

                return dict(output=partial, ray=RaySchemaInst.dump(ray))

            except BaseException as e:
                logger.error(f"Openfabric - failed to send partial output: {e}")
                pass

            return None
