import threading
import time

from openfabric_pysdk.app.execution.ipc.action_encoder import ActionEncoder
from openfabric_pysdk.context import AppModel, Ray
from openfabric_pysdk.context.ray_schema import RaySchemaInst
from openfabric_pysdk.loader import getSchemaInst
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service.hash_service import HashService

from openfabric_pysdk.service.resource_service import ResourceService


# UpdatePublisher class
# This class is responsible for updating the state of the application
# by publishing the state updates to the dispatcher.

# It acts as a throttler for state updates, in order to avoid flooding the dispatcher


class UpdatePublisher:
    def __init__(self, publisher):
        self.ray = None
        self.output = None
        self.qid = None
        self.lock = threading.Condition()
        self.publisher = publisher
        self.running = False
        self.__runner = None
        self.publisingPeriodinS = 0.1
        self.last_output_hash = None

    def start(self):
        self.__runner = threading.Thread(target=self.run, name="sdk_publisher")
        self.__runner.start()

    def stop(self):
        if self.__runner is None:
            return
        self.running = False
        self.__runner.join()
        self.__runner = None

    def onRayUpdate(self, ray: Ray):
        logger.debug(f'onRayUpdate: {ray}')
        self.lock.acquire()
        self.ray = ray
        self.lock.release()

    def onPartialUpdate(self, app_model: AppModel):
        logger.debug(f'onPartialUpdate: {app_model}')
        self.lock.acquire()
        if app_model is None:
            self.qid = None
            self.output = None
        else:
            self.qid = app_model.ray.qid
            self.output = app_model.response
        self.last_output_hash = None
        self.lock.release()

    def onLogMessage(self, level, msg):
        self.publisher.publish(ActionEncoder.log(level, msg))

    def run(self):
        self.running = True

        while self.running:
            publisher = self.publisher

            if publisher is None:
                break

            self.lock.acquire()

            if self.ray is not None:
                publisher.publish(ActionEncoder.state_update(self.ray.qid, ray=RaySchemaInst.dump(self.ray)))
                self.ray = None

            if self.qid is not None and self.output is not None:
                try:
                    current_output_hash = HashService.fast_hash(self.output)
                    if self.last_output_hash != current_output_hash:
                        ResourceService.lock(self.qid)
                        partial = None
                        try:
                            partial = getSchemaInst('out').dump(self.output)
                        finally:
                            ResourceService.unlock()
                        publisher.publish(ActionEncoder.state_update(self.qid, partial=partial))
                        self.last_output_hash = current_output_hash
                except BaseException as e:
                    logger.error(f"Exception occurred - {e}")
            self.lock.release()

            time.sleep(self.publisingPeriodinS)

        self.running = False
