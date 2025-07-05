#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
import traceback

sys.path.append(str({os.getcwd()}))

from openfabric_pysdk.app.execution import ActionDispatcher, ActionEncoder, ApplicationInterface, ExecutionContext, \
    LogsHandler
from openfabric_pysdk.context import State, StateStatus, StateSchema
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService

# Executor application

# Main entry point for the executor application
if __name__ == "__main__":
    # Disable socketio and engineio logger
    # Reason: would cause recursion error as we are using the proxy to forward logs

    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('--debug', action='store_true', help='debug mode')
    parser.add_argument('--publisher_port', type=int,  default=5556, help='publisher port')
    parser.add_argument('--subscriber_port', type=int, default=5555, help='subscriber port')
    args = parser.parse_args()

    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG

    # ---------------------------------------------------------------------------------------------------

    context = ExecutionContext(publisher_port=args.publisher_port, subscriber_port=args.subscriber_port)
    worker = ActionDispatcher(context)

    PersistenceService.set_store_path(f"{context.datastore_path}")

    # Configure custom logger
    customLogger = LogsHandler()
    customLogger.install(context, loglevel)

    try:
        state = State()
        state.status = StateStatus.STARTING
        context.publish(ActionEncoder.app_state(StateSchema().dump(state)))
        context.execution = ApplicationInterface()
        state.status = StateStatus.RUNNING
        context.publish(ActionEncoder.app_state(StateSchema().dump(state)))
    except Exception as inst:
        startup_error = ''.join(traceback.TracebackException.from_exception(inst).format())
        time.sleep(1)
        logger.error(f'Exception while starting app: {startup_error}')
        state = State()
        state.status = StateStatus.CRASHED
        context.publish(ActionEncoder.app_state(StateSchema().dump(state)))
        time.sleep(1)
        context.destroy()
        exit(-1)

    logger.info(f'started')

    worker.start()

    while worker.isRunning():
        time.sleep(0.1)

    logger.info(f'exiting')
    worker.stop()

    customLogger.uninstall()
    context.destroy()

    # Need to close socket connection otherwise the app hands in state "sleeping"
    worker = None
    logger.info(f'stopped')
    exit(0)
