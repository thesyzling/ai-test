import pickle
from typing import Any, Dict

from openfabric_pysdk.app.execution.ipc.actions import DispatchActions
from openfabric_pysdk.logger import logger


# ActionDecoder class
# This class is responsible for decoding the messages from the dispatcher
# and dispatching them to the appropriate handler


class ActionDecoder:

    _mappings: Dict[DispatchActions, str] = {
        DispatchActions.ADD: "onAdd",
        DispatchActions.CHECK: "onCheck",
        DispatchActions.CONFIGURE: "onConfigure",
        DispatchActions.EXIT: "onExit",
        DispatchActions.FETCH: "onFetch",
        DispatchActions.LOG: "onLog",
        DispatchActions.REMOVE: "onRemove",
        DispatchActions.APP_STATE: "onAppState",
        DispatchActions.UPDATE: "onUpdate",
        DispatchActions.SCHEMA_UPDATE: "onSchemaUpdate",
        DispatchActions.SYNC: "onSync"
    }

    # ------------------------------------------------------------------------
    def __init__(self, handler):
        self.handler = handler

    # ------------------------------------------------------------------------
    @staticmethod
    def deserialize(input: Any):
        data = pickle.loads(input)
        return data['action'], data['data'] if 'data' in data else None

    # ------------------------------------------------------------------------
    def decode(self, message: str):
        action, data = ActionDecoder.deserialize(message)

        if action is None:
            logger.error("Missing action in message")
            method_name = "onInvalidMessage"
        elif action in self._mappings:
            method_name = self._mappings[action]
        else:
            data = action
            method_name = "onUnsupportedAction"

        if hasattr(self.handler, method_name):
            logger.debug(str(action) + ": " + str(data))
            method = getattr(self.handler, method_name)
            method(data)
        else:
            logger.error(f"Handler does not have method {method_name}")
