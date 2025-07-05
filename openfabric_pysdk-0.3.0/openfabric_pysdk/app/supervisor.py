import json
import os
import subprocess
import gevent
import random
from typing import Any, Dict
from pathlib import Path

from openfabric_pysdk.app.execution import ActionDecoder, ActionEncoder, ExecutionContext
from openfabric_pysdk.auth import session_link
from openfabric_pysdk.context import Ray, State, StateStatus, RaySchema, RayStatus, StateSchema
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.helper import json_schema_to_marshmallow
from openfabric_pysdk.loader import ConfigClass, InputClass, OutputClass
from openfabric_pysdk.loader import InputSchema, OutputSchema, ConfigSchema, setSchemas
from openfabric_pysdk.loader.config import manifest, state_config
from openfabric_pysdk.logger import logger, logger_worker
from openfabric_pysdk.store import KeyValueDB
from openfabric_pysdk.service import PersistenceService

my_env = os.environ.copy()
if "PYTHONPATH" in my_env:
    my_env["PYTHONPATH"] = f"{os.getcwd()}:{my_env['PYTHONPATH']}"
else:
    my_env["PYTHONPATH"] = os.getcwd()


#######################################################
#  Supervisor
#######################################################
class Supervisor:
    state: State = None
    __process = None
    __script_dir = Path(__file__).parent.absolute()

    # ------------------------------------------------------------------------
    def __init__(self):
        self.state = State()
        self.notificationSocket = None
        self.publisher_port = random.randint(5001, 9999)
        self.subscriber_port = self.publisher_port + 1
        self.executionContext = ExecutionContext(publisher_port=self.publisher_port,
                                                 subscriber_port=self.subscriber_port)
        self.executionContext.register(self.scheduleAction)
        self.actionDecoder = ActionDecoder(self)
        self.__update_worker_state()

    # ------------------------------------------------------------------------
    def __del__(self):
        # TODO: could signal 
        #    self.executionContext.publish(DispatchMessage.exit("closing"))
        # in order to have a graceful shutdown
        # if no response after a period of time, kill self.__process
        # stop publisher and subscriber
        pass

    # ------------------------------------------------------------------------
    def scheduleAction(self, message):
        self.actionDecoder.decode(message)

    def onFetch(self, data):
        if data == "queue":
            self.dispatch(ActionEncoder.fetch(data), False)
        else:
            logger.error("Unknown fetch request: " + str(data))

    def onUpdate(self, data):
        from openfabric_pysdk.engine import engine
        response: Dict[str, Any] = data

        if 'qid' not in response:
            logger.error("Missing qid in message")
            return

        qid = response["qid"]
        logger.debug("UPDATE: " + str(qid))

        flush_data = False
        if 'partial' in response:
            engine.partial_output(qid, response['partial'])

        if 'input' in response:
            PersistenceService.set_asset(qid, "in", response['input'])

        if 'ray' in response:
            ray = RaySchemaInst.load(response['ray'])
            engine.ray(ray.qid).update(ray)
            PersistenceService.set_asset(qid, "ray", response['ray'])
        else:
            ray = engine.ray(qid)


        # TODO: maybe extract this to a function or something
        # TODO: In order not to block this, it might be better for the emit to be performed from a separate thread
        # Note that we do not require a delay here for progress beacause the worker will be sending it with a delay
        if self.notificationSocket != None:
            if ray != None:
                user_sessions = session_link.get_user_sessions(ray.uid)
                active_session = ray.sid in session_link.get_active_sessions()

                if len(user_sessions) > 0 or active_session:
                    # No point in sending both response and progress, since response contains progress
                    output = PersistenceService.get_asset(qid, "out")
                    output_ts = PersistenceService.get_asset_timestamp(qid, "out")
                    # Reworked to send a response message instead of only progress even on failure
                    # event = 'response' if ray.finished and output != None else 'progress'
                    event = 'response' if ray.finished else 'progress'

                    try:
                        data = dict(output=output, ray=RaySchemaInst.dump(ray), output_ts=output_ts) if event == 'response' else RaySchemaInst.dump(ray)

                        for session in user_sessions:
                            self.notificationSocket.emit(event, data, room=session)
                        # In case the worker has not registered a user session, but has made the request,
                        #    we will assume he wants updates.
                        if active_session and ray.sid not in user_sessions:
                            self.notificationSocket.emit(event, data, room=ray.sid)
                    except Exception as e:
                        logger.error(f"Failed to send notification: {e}")

    def onSchemaUpdate(self, data):
        global InputSchema
        global OutputSchema
        global ConfigSchema
        response: Dict[str, Any] = data
        input = InputSchema
        output = OutputSchema
        config = ConfigSchema

        if 'input' in response and response['input'] != None:
            input = json_schema_to_marshmallow(json.loads(response['input']))

        if 'output' in response and response['output'] != None:
            output = json_schema_to_marshmallow(json.loads(response['output']))

        if 'config' in response and response['config'] != None:
            config = json_schema_to_marshmallow(json.loads(response['config']))

        setSchemas(input=input, config=config, output=output)

        self.notificationSocket.emit('schema_update', response)

    def onAppState(self, data):
        receivedState = StateSchema().load(data)
        logger.info(f"State update: {self.state.status} -> {receivedState['status']}")
        self.state.status = receivedState["status"]

    def onLog(self, data):
        logger_worker.log(data['level'], data['message'])

    def onExit(self, data):
        logger.info("Worker exited with reason: " + str(data))
        # Temporary workaround
        self.__process.kill()
        self.__process = None

        if str(data) == "suspend":
            self.state.status = StateStatus.PAUSED

    def onUnsupportedAction(self, action):
        # logger.error("Unexpected action: " + str(action))
        pass

    # ------------------------------------------------------------------------
    def set_status(self, status: StateStatus):
        self.state.status = status
        # status should come from the worker, so we should not send it back

    # ------------------------------------------------------------------------
    def set_notification_service(self, notificationSocket):
        # notificationSocket: from openfabric_pysdk.transport.socket import ExecutionSocket
        # circular inclusion
        self.notificationSocket = notificationSocket

    # ------------------------------------------------------------------------
    def dispatch(self, data="", start_worker=True):
        logger.debug(f"Dispatching {data}")

        if start_worker:
            self.__update_worker_state()

        self.executionContext.publish(data)

    # ------------------------------------------------------------------------
    def __update_worker_state(self):
        if self.__process is None or self.__process.poll() is not None:
            if "OPENFABRIC_DEBUG" in my_env:
                # Note: remove stdout/stderr redirection to debug app
                self.__process = subprocess.Popen(
                    ["python3", self.__script_dir / "executor.py", "--publisher_port", str(self.subscriber_port),
                     "--subscriber_port", str(self.publisher_port)], env=my_env)
            else:
                self.__process = subprocess.Popen(
                    ["python3", self.__script_dir / "executor.py", "--publisher_port", str(self.subscriber_port),
                     "--subscriber_port", str(self.publisher_port)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                    env=my_env)

    # ------------------------------------------------------------------------
    def execution_callback_function(self, input: InputClass, ray: Ray) -> OutputClass:
        self.dispatch(ActionEncoder.add(ray.qid))
        counter = 0

        while True:
            if not ray.finished:
                gevent.sleep(0.1)
                counter += 1
                # Make sure the worker was not closing while we
                # were proposing a new entry, or that it died.
                if counter % 10 == 0:
                    self.dispatch(ActionEncoder.check_request(ray.qid))

                # If the app crashed, cancel all ongoing rays.
                if self.state.status == StateStatus.CRASHED:
                    ray.status = RayStatus.FAILED
                    ray.finished = True
                    break
            else:
                break

        return None

    # ------------------------------------------------------------------------
    def cancel_execution(self, ray: Ray):
        ray.status = RayStatus.CANCELED
        ray.complete()
        self.dispatch(ActionEncoder.remove(ray.qid), False)

    # ------------------------------------------------------------------------
    def config_callback_function(self, config: Dict[str, ConfigClass]):
        self.dispatch(ActionEncoder.configure(), False)

    # ------------------------------------------------------------------------
    def sync(self, qid):
        self.dispatch(ActionEncoder.sync(qid))

    # ------------------------------------------------------------------------
    def get_manifest(self) -> KeyValueDB:
        return manifest

    # ------------------------------------------------------------------------
    def get_state_config(self) -> KeyValueDB:
        return state_config
