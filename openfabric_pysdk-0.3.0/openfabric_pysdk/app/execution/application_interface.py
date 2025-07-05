import os
from typing import Any

from openfabric_pysdk.context import AppModel, Ray, State
from openfabric_pysdk.loader import getSchemaInst
from openfabric_pysdk.logger import logger

my_env = os.environ.copy()

# ApplicationInterface class
# This class is the interface between the application and the OpenFabric SDK.


class ApplicationInterface:
    def __init__(self, **kwargs):
        api_version = 1
        cancel_api_version = 1

        # The goal is to load it where we first load the main script
        if "OPENFABRIC_DEBUG" in my_env and my_env["OPENFABRIC_DEBUG"] == "debugpy":
            # Added here to avoid adding an extra dependency to the project
            import debugpy
            debugpy.listen(("localhost", 5678))
            debugpy.wait_for_client()

        from openfabric_pysdk.api import config_callback_function, \
            execution_callback_function, \
            execution_callback_function_params, \
            suspend_callback_function, \
            cancel_callback_function, \
            cancel_callback_function_params

        # TODO: deprecate version 1
        if cancel_callback_function_params is not None:
            parameters = len(cancel_callback_function_params.items())
            cancel_api_version = 2 if (parameters == 1 and "ray" in str(
                next(iter(cancel_callback_function_params.items()))[1]).lower()) else 1

        if execution_callback_function_params is not None:
            parameters = len(execution_callback_function_params.items())
            api_version = 2 if (parameters == 1 and "context" in str(
                next(iter(execution_callback_function_params.items()))[1]).lower()) else 1
        logger.debug(f'detected api version set to: {api_version}')

        self.config_function = kwargs.get('config_function', config_callback_function)
        self.execution_function = kwargs.get('execution_function', execution_callback_function)
        self.suspend_function = kwargs.get('suspend_function', suspend_callback_function)
        self.cancel_function = kwargs.get('cancel_function', cancel_callback_function)

        self.api_version = kwargs.get('api_version', api_version)
        self.cancel_api_version = kwargs.get('cancel_api_version', cancel_api_version)
        self.suspend_request_time_s = kwargs.get('suspend_request_time_s', 5)

    def execute(self, input: Any, ray: Ray, state: State, on_partial_update_callback):

        # Callback execution method
        # keeping previous prototype for backward compatibility
        if self.api_version == 1:
            return self.execution_function(input, ray, state)
        elif self.api_version == 2:
            schema = getSchemaInst('out')

            output = schema.load([] if schema.many is True else {}, partial=True)
            model = AppModel(ray=ray, state=state, output=output, input=input)
            on_partial_update_callback(model)
            self.execution_function(model)
            on_partial_update_callback(None)
            return model.response
        else:
            raise Exception("Invalid API version")

    def cancel(self, ray: Ray):

        if self.isCancelEnabled():
            try:
                if self.cancel_api_version == 1:
                    return self.cancel_function(ray.qid)
                elif self.cancel_api_version == 2:
                    return self.cancel_function(ray)
                # TODO: do we wait for the process to exit?
                # technically we don't want to block the main thread on this ....
                # so this cancel function should run on a different looper.
                # suspend below should be ok for now, as there is no more work from the main app when it is called.
            except Exception as e:
                logger.error(f"Openfabric - cancel ended with error : {e}")
                # we could accept it, or we could ... sepuku!
                # exit() # we would likely end up in limbo if we do this, let's handle it somewhere else
        else:
            # we might as well brutally murder it
            # technically we can't kill the thread,
            # because the app might use a global state which would become dirty
            # best would be to simply exit the worker by forcing exit ....
            # issue would be that the main app might wait for multiple app executions to finish
            # and they could be scheduled in a new order.
            # exit() # we would likely end up in libo if we do this, let's handle it somewhere else
            pass

        return False

    def isSuspendAllowed(self, state: State):

        # In practice, we could implement a suspend_prepare and a suspend function
        # The suspend_prepare would be called before the suspend function once
        # the queue is empty.
        # After a predefined/fixed period we call the suspend function.
        # But the application could return False on the current suspend on the first
        # few calls and True on a later call. Basically the application could decide
        # to suspend, after a predefined period.
        if self.isSuspendEnabled():
            try:
                # If the suspend function returns True, we exit the application.
                if self.suspend_function(state):
                    return True
            except Exception as e:
                logger.error(f"Openfabric - suspend ended with error : {e}")

        return False

    def isCancelEnabled(self):
        return self.cancel_function is not None

    def isSuspendEnabled(self):
        return self.suspend_function is not None

    def getSuspendPeriodS(self):
        if self.suspend_request_time_s < 1:
            return 1
        elif self.isSuspendEnabled():
            return self.suspend_request_time_s
        else:
            return int(999999999 / 10)

    def configure(self, config, state):
        if self.config_function is not None:
            try:
                self.config_function(config, state)
            except Exception as e:
                logger.error(f"Openfabric - invalid configuration can\'t restored : {e}")
        else:
            logger.warning(f"Openfabric - no configuration callback available")
