import json
import threading
import time
import traceback

from marshmallow_jsonschema import JSONSchema

from openfabric_pysdk.app.execution.execution_context import ExecutionContext
from openfabric_pysdk.app.execution.ipc.action_decoder import ActionDecoder
from openfabric_pysdk.app.execution.ipc.action_encoder import ActionEncoder
from openfabric_pysdk.benchmark import measure_block_time
from openfabric_pysdk.context import AppModel, MessageType, RayStatus, State
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.loader import registerOnSchemaUpdateCb, getSchemaInst
from openfabric_pysdk.loader.config import state_config
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService

from openfabric_pysdk.service.resource_service import ResourceService

# ActionDispatcher class
# This class is responsible for dispatching actions to the execution engine
# It is responsible queueing actions and dispatching them in order


class ActionDispatcher:

    __hara_kiri_timeout_seconds: float = 1.0

    def __init__(self, context: ExecutionContext):
        self.workerContext = context
        self.state = State()
        self.__runner = None
        self.running = False
        self.lock: threading.Condition = threading.Condition()
        self.currentQid = None
        self.queue = []
        self.actionDecoder = ActionDecoder(self)
        self.workerContext.register(self.scheduleAction)
        self.activeSessionModel: AppModel = None
        self.ray = None
        registerOnSchemaUpdateCb(self.updateSchema)

    def updateSchema(self, input=None, output=None, config=None):
        if input is not None:
            input=str(json.dumps(JSONSchema().dump(input())))
        if output is not None:
            output=str(json.dumps(JSONSchema().dump(output())))
        if config is not None:
            config=str(json.dumps(JSONSchema().dump(config())))
        self.workerContext.publish(ActionEncoder.schema_update(input=input, output=output, config=config))
            

    def start(self):
        if not self.running:
            self.__runner = threading.Thread(target=self.run, name="action_dispatcher")
            self.__runner.setDaemon(True)
            self.running = True
            self.onConfigure(None)  # reload configurations
            self.__runner.start()
            self.workerContext.notifier.start()

    def stop(self):
        self.running = False
        if self.__runner is not None:
            # Thread is launched as daemon, as we don't want to wait on it
            # self.__runner.join()
            self.__runner = None
            self.workerContext.notifier.stop()

    def __hara_kiri(self, qid):
        # Check if the current ray is the one we want to cancel
        # Mainly to see that it did not complete before we could cancel it
        if self.ray != None and self.ray.qid == qid:
            # We're doing it! It's happening! We're cancelling the ray!
            # We're cancelling the app! Brace for impact!
            # Ohhhhhhhhh ....

            # Cancel all active timer threads
            for thread in threading.enumerate():
                if isinstance(thread, threading.Timer):
                    thread.cancel()

            self.running = False
            from openfabric_pysdk.helper import Proxy

            # Stop all the proxies so that they don't hang around
            Proxy.stop_all()

    # ------------------------------------------------------------------------
    def cancel(self, qid):
        # Start a timer to cancel the execution if it takes too long
        timer = threading.Timer(ActionDispatcher.__hara_kiri_timeout_seconds, self.__hara_kiri, args=[qid])
        timer.start()

        # As the ray is being cancelled, we don't want to send any more updates
        # Thereforem we explicitly request to stop posting additional updates.
        self.workerContext.notifier.onRayUpdate(None)
        if self.ray != None:
            self.ray.on_update(None)

        # Request cancel to the application
        if not self.workerContext.execution.cancel(self.ray):
            # The application does not implement cancel, or has not accepted our request

            # No point in waiting any longer until the timeout
            self.__hara_kiri(qid)

    # ------------------------------------------------------------------------
    def process(self, qid):

        with measure_block_time("Engine::execution_callback_function"):
            output = None
            self.ray = PersistenceService.get_asset(qid, 'ray', RaySchemaInst.load)
            if self.ray is None:
                return

            if self.ray.finished == True and self.ray.status != RayStatus.REMOVED:
                output = PersistenceService.get_asset(qid, 'out')
                self.workerContext.publish(ActionEncoder.state_update(qid, output=output))
                return

            logger.info(f'Processing {qid}')

            self.ray.on_update(self.workerContext.notifier.onRayUpdate)

            data = PersistenceService.get_asset(qid, 'in', getSchemaInst('in').load)
            if data is None:
                error = f"process - failed to load input data on request[{qid}]"
                logger.error(error)
                self.ray.message(MessageType.ERROR, error)
                self.ray.status = RayStatus.FAILED
                self.ray.complete()
                return

            try:
                self.ray.status = RayStatus.RUNNING
                update = self.__session_context
                output = self.workerContext.execution.execute(data, self.ray, self.state, update)

                self.ray.on_update(None)
                # Extra precotion to not send the same ray twice
                self.workerContext.notifier.onRayUpdate(None)
                self.ray.status = RayStatus.COMPLETED

                # TODO: rework
                self.lock.acquire()
                cancelled = self.currentQid != qid
                self.lock.release()

                # No need to store response if cancelled
                if cancelled:
                    return

                ResourceService.lock(qid)
                PersistenceService.set_asset(qid, "out", getSchemaInst('out').dump(output))
                ResourceService.unlock()
            except:                
                self.ray.on_update(None)
                # Extra precotion to not send the same ray twice
                self.workerContext.notifier.onRayUpdate(None)
                error = f"process - failed executing: [{qid}]\n{traceback.format_exc()}"
                logger.error(error)
                self.ray.message(MessageType.ERROR, error)
                self.ray.status = RayStatus.FAILED

            # TODO: find better way to signal that it is cancelled . we aquire the lock for currentQid access
            self.lock.acquire()
            cancelled = self.currentQid != qid
            self.lock.release()

            # No need to send ray if canceled
            if cancelled:
                return

            self.ray.complete()
            self.workerContext.publish(ActionEncoder.state_update(qid, ray=RaySchemaInst.dump(self.ray)))
            self.ray = None

        logger.info(f'completed {qid}')

    # ------------------------------------------------------------------------
    # would be the main thread
    def run(self):
        self.running = True

        suspendPeriod = self.workerContext.execution.getSuspendPeriodS()
        retryCounterResetValue = 10 * suspendPeriod
        ticksUntilSuspend = retryCounterResetValue

        while self.running:
            self.lock.acquire()
            self.currentQid = None
            if len(self.queue) > 0:
                self.currentQid = self.queue.pop(0)
            self.lock.release()

            if self.currentQid is not None:
                try:
                    self.process(self.currentQid)
                except Exception as e:
                    # TODO: fix this.
                    # try catch should be in process, but it's relating to reading the db
                    logger.error(f"Failed to execute {self.currentQid} {e}\n{traceback.format_exc()}")
                    pass
                ticksUntilSuspend = retryCounterResetValue
            elif ticksUntilSuspend <= 0:
                state = State()
                if self.workerContext.execution.isSuspendAllowed(state):
                    logger.info(f"Openfabric - suspend allowed by the application")
                    self.workerContext.publish(ActionEncoder.exit("suspend"))
                    self.running = False
                    continue
                else:
                    ticksUntilSuspend = 10 if self.workerContext.execution.isSuspendEnabled() else retryCounterResetValue
                    logger.debug(
                        f"Openfabric - suspend denied by the application, retrying in {ticksUntilSuspend / 10} seconds")
            else:
                ticksUntilSuspend -= 1

            time.sleep(0.1)

        self.running = False
        self.workerContext.notifier.stop()

    def isRunning(self):
        return self.running

    # Yes, this workaround seems like a crime!
    def __session_context(self, app_model: AppModel):
        self.lock.acquire()
        self.workerContext.notifier.onPartialUpdate(app_model)
        self.activeSessionModel = app_model
        self.lock.release()

    def scheduleAction(self, message: str):
        self.actionDecoder.decode(message)

    def onAdd(self, qid):
        self.lock.acquire()
        if qid != self.currentQid and qid not in self.queue:
            self.queue.append(qid)
        self.lock.release()

    def onCheck(self, qid):
        self.onAdd(qid)

    def onSync(self, qid):
        self.lock.acquire()
        if qid == self.currentQid:
            if self.activeSessionModel is not None:
                self.activeSessionModel.request = PersistenceService.get_asset(qid, 'in', getSchemaInst('in').load)
        self.lock.release()

    def onRemove(self, qid):
        self.lock.acquire()
        try:
            while qid in self.queue:
                self.queue.remove(qid)

            if qid == self.currentQid:
                self.currentQid = None
                self.cancel(qid)
        except:
            pass
        self.lock.release()

    def onConfigure(self, data):
        state = State()

        state_config.reload()
        items = state_config.all().items()
        config = dict(map(lambda kv: (kv[0], getSchemaInst('config').load(kv[1])), items))

        self.workerContext.execution.configure(config, state)

    def onExit(self, reason):
        self.running = False

    def onUnsupportedAction(self, action):
        # logger.error("Unexpected action: " + str(action))
        pass
