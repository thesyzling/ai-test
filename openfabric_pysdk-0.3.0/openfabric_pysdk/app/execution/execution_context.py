from openfabric_pysdk.app.execution.ipc.subscriber import Subscriber
from openfabric_pysdk.app.execution.ipc.publisher import Publisher
from openfabric_pysdk.app.execution.update_pulblisher import UpdatePublisher

import os

# ExecutionContext class
# This class is responsible for managing the execution context of the executor
# It provides an interface to interact with the dispatcher

class ExecutionContext:
    def __init__(self, **kwargs):
        publisher_port = kwargs.get('publisher_port', None)
        subscriber_port = kwargs.get('subscriber_port', None)

        self.datastore_path =  kwargs.get('datastore', f"{os.getcwd()}/datastore")
        self.execution = kwargs.get('execution', None)
        self.__publisher = Publisher(f"tcp://127.0.0.1:{publisher_port}")
        self.__subscriber = Subscriber(f"tcp://127.0.0.1:{subscriber_port}")
        self.notifier = UpdatePublisher(self.__publisher)

    def publish(self, message):
        self.__publisher.publish(message)

    def register(self, handler):
        self.__subscriber.register_callback(handler)

    def destroy(self):
        self.__publisher.close()
        self.__subscriber.close()
        self.notifier.stop()
