import collections
import json
import logging
import socketio
import threading
import time
import uuid
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict


# forward declaration
class Proxy: pass


from deepdiff import Delta


class ExecutionResult:
    def __init__(self, proxy: Proxy):
        self.__progress = None
        self.__proxy = proxy
        self.__rid = uuid.uuid4().hex
        self.__finished = False
        self.last_update = datetime.now()
        self.__result = None
        self.__watch = False
        logging.info(f"Created rid" + str(self.__rid))

    def __del__(self):
        self.cancel()

    def on_progress(self, data):
        self.__progress = data
        self.last_update = datetime.now()

        # The source of the potential problem
        # We will mitigate this by checking the status of the execution
        # and activate it only if it is running
        if self.__watch and self.status() == "RUNNING":
            self.__proxy.watch(self.request_qid())
            self.__watch = False

    def on_pulse(self):
        self.last_update = datetime.now()

    def on_response(self, data):
        if data != None and "ray" in data:
            self.on_progress(data["ray"])

        if data != None and "output" in data:
            self.__result = data["output"]

        self.__finished = True
        self.cancel()

    def on_restore(self, data):
        if data != None and "ray" in data:
            self.on_progress(data["ray"])

        if data != None and "output" in data:
            # TODO: if output != None: force finish?
            self.__result = data["output"]
            self.__finished = True
            self.cancel()

    def request_id(self):
        return self.__rid

    def request_qid(self):
        if self.__progress is not None and \
                "qid" in self.__progress:
            return self.__progress["qid"]
        return None

    def messages(self):
        if self.__progress != None and "messages" in self.__progress:
            return self.__progress["messages"]

        return []

    def progress(self, name="default"):
        if self.__progress != None and "bars" in self.__progress:
            if name in self.__progress["bars"]:
                return self.__progress["bars"][name]

        return None

    def status(self):
        if self.__progress != None and "status" in self.__progress:
            return self.__progress["status"]

        return "UNKNOWN"

    def cancel(self):
        if self.__proxy != None:
            self.__proxy.cancel(self)
            self.__proxy = None

        if self.__finished == False:
            if self.__progress == None:
                self.__progress = {}
            self.__progress["status"] = "CANCELLED"

    def wait(self, timeout=0):
        remaining = timeout

        while self.__proxy != None and (timeout == 0 or remaining > 0) and self.__finished == False:
            remaining -= 0.1
            time.sleep(0.1)

        return self.__finished

    def watch(self):
        # If qid is not available, then schedule it when it becomes available
        # Note: this might backfire if progress is sent later for different watch
        # issue is that we have only one watch active at a time.
        # Maybe just not add this as an option/method ...
        if self.__proxy is not None and self.request_qid() is not None and self.status() == "RUNNING":
            self.__proxy.watch(self.request_qid())
        else:
            self.__watch = True

    def discard(self):
        self.__finished = True

    def data(self):
        return self.__result

    def __repr__(self):
        return f"ExecutionResult({self.__rid}, {self.__progress}, {self.__result})"

    def __str__(self):
        return self.__repr__()


#######################################################
#  Proxy
#######################################################
class Proxy:
    proxies = set()

    # ------------------------------------------------------------------------
    def __init__(self, url: str, tag: str = None, ssl_verify=True, connection_timeout=15):
        '''
        url: str
            The URL of the server to connect to.
        tag: str
            A tag to identify the proxy.
        ssl_verify: bool
            Whether to verify the SSL certificate.
        connection_timeout: int
            The connection timeout in seconds. Default is 15 seconds.
        '''
        self.maximum_no_respose_interval = 15  # seconds
        self.minimum_check_interval = 5  # seconds
        self.minimum_update_interval = 5  # seconds
        self.__results = {}
        self.__executions = {}
        self.__url = url
        self.__tag = url if tag is None else tag
        self.__cancel_next = False
        self.__pending_responses = 0
        self.__running = True
        self.__sio = socketio.Client(ssl_verify=ssl_verify)
        self.__lock: threading.RLock = threading.RLock()
        self.__runner = threading.Thread(target=self.__run, name=self.__tag + "_proxy", args=())
        self.__runner.start()
        self.__checker = threading.Thread(target=self.__check_state, name=self.__tag + "_checker", args=())
        self.__checker.start()
        self.__executor = ThreadPoolExecutor(max_workers=1)
        self.__callbacks = collections.defaultdict(list)
        self.__partials = {}
        self.__active_watch = None
        self.__active_watch_hash = None
        self.__setup_call_backs()

        maxRetries = connection_timeout * 10
        while not self.is_connected() and maxRetries > 0:
            time.sleep(0.1)
            maxRetries -= 1

        Proxy.proxies.add(self)

    def __del__(self):
        self.cleanup()

    def disconnect(self):
        self.cleanup()

    def cleanup(self):
        if hasattr(self, '_Proxy__lock'):
            self.__lock.acquire()
        if hasattr(self, '_Proxy__executions'):
            for k, v in self.__executions.items():
                v.discard()
            self.__executions.clear()
        if hasattr(self, '_Proxy__lock'):
            self.__lock.release()
        if hasattr(self, '_Proxy__running'):
            self.__running = False
        if hasattr(self, '_Proxy__executor'):
            self.__executor.shutdown(wait=False)
        if hasattr(self, '_Proxy__sio'):
            self.__sio.disconnect()

        Proxy.proxies.remove(self)

    @staticmethod
    def stop_all():
        logging.info(f"PROXY: Stopping all proxies: {len(Proxy.proxies)}")

        while len(Proxy.proxies) > 0:
            proxy = next(iter(Proxy.proxies))
            try:
                proxy.disconnect()
            except Exception as e:
                logging.info(f"{proxy.get_tag()}: Error while disconnecting {e}")
                pass

        logging.info(f"PROXY: all proxies have been disconnected")

    def get_tag(self):
        return self.__tag

    def __run(self):
        logging.info(f"{self.__tag}: Proxy started")
        # Wait until the connection with the server ends.
        while self.__running:
            try:
                self.__sio.connect(self.__url, transports='websocket', namespaces=["/app"])
                self.__sio.wait()
                break;
            except:
                logging.exception(f"{self.__tag}: Could not connect to server. Retrying in 15 seconds to {self.__url}")
                maxRetries = 150
                while self.__running and maxRetries > 0:
                    time.sleep(0.1)
                    maxRetries -= 1
                pass

        logging.info(f"{self.__tag}: Proxy exiting")

    def __check_state(self):
        last_update_check = datetime.now()

        logging.info(f"{self.__tag}: State checker started")
        while self.__running:
            # time.sleep(minimum_check_interval)
            while (datetime.now() - last_update_check).total_seconds() < self.minimum_check_interval and self.__running:
                time.sleep(0.1)

            last_update_check = datetime.now()

            executions_to_cancel = []
            executions_to_ping = []
            self.__lock.acquire()
            try:
                for rid, execution in self.__executions.items():
                    elapsedSinceLastUpdate = (datetime.now() - execution.last_update).total_seconds()

                    # TODO: check reference count of the execution so see if it is still in use, and cancel it otherwise
                    # import sys
                    # if sys.getrefcount(execution) < ??:
                    #     executions_to_cancel.append(rid)
                    #     continue

                    if elapsedSinceLastUpdate > self.maximum_no_respose_interval:
                        executions_to_cancel.append(rid)
                    elif elapsedSinceLastUpdate > self.minimum_update_interval:
                        executions_to_ping.append(rid)
            except:
                logging.exception(f"{self.__tag}: Exception in state checker")
                pass
            self.__lock.release()

            try:
                for rid in executions_to_cancel:
                    logging.error(f"{self.__tag}: Execution {rid} is not responding. Cancelling")
                    qid = self.__executions[rid].request_qid()
                    self.__executions[rid].cancel()
                    if qid != None:
                        self.delete(qid)

                for rid in executions_to_ping:
                    qid = self.__executions[rid].request_qid()
                    if qid != None:
                        logging.info(f"{self.__tag}: Execution {rid} is not responding. Requesting update")
                        self.restore(qid)
                    else:
                        logging.info(f"{self.__tag}: Execution {rid} is not responding. Queue id not yet available")
            except:
                logging.exception(f"{self.__tag}: Exception in state checker")
                pass

        logging.info(f"{self.__tag}: State checker exiting")

    def request(self, input, uid):
        access = True
        if (uid == None):
            uid = uuid.uuid4().hex

        execution = ExecutionResult(self)
        rid = execution.request_id()

        # TODO: emit is not threadsafe. We need to lock here.
        #       need also to see if we catch exceptions here.
        data = {}
        data["body"] = input
        data["header"] = {"uid": uid, "rid": rid}
        data = zlib.compress(json.dumps(data).encode('utf-8')), access
        self.__sio.emit('execute', data=data, namespace='/app')

        # Add execution to monitored executions. Assuming rid is unique.
        self.__lock.acquire()
        self.__executions[rid] = execution
        self.__lock.release()

        return execution

    def sync(self, qid, input, uid=None):
        access = True
        if (uid == None):
            uid = uuid.uuid4().hex

        execution = ExecutionResult(self)
        rid = execution.request_id()

        # TODO: emit is not threadsafe. We need to lock here.
        #       need also to see if we catch exceptions here.
        data = {}
        data["body"] = input
        data["header"] = {"uid": uid, "rid": rid}
        data = qid, zlib.compress(json.dumps(data).encode('utf-8'))
        self.__sio.emit('sync', data=data, namespace='/app')

    def cancel(self, execution: ExecutionResult):
        self.__lock.acquire()
        rid = execution.request_id()
        if rid in self.__executions:
            self.__executions.pop(rid)
        self.__lock.release()

    # TODO: remove
    def execute_async(self, input, uid=None, rid=None):
        access = True
        if (uid == None):
            uid = uuid.uuid4().hex
        if (rid == None):
            rid = uuid.uuid4().hex
        data = {}
        data["body"] = input
        data["header"] = {"uid": uid, "rid": rid}
        data = zlib.compress(json.dumps(data).encode('utf-8')), access
        self.__sio.emit('execute', data=data, namespace='/app')

        ++self.__pending_responses
        return self.__executor.submit(Proxy.get_response, self, rid)

    # TODO: remove
    def execute(self, input, uid=None, rid=None):
        return self.execute_async(input, uid=uid, rid=rid).result()

    def configure(self, config, uid):
        data = {}
        data["body"] = config
        data["header"] = {"uid": uid}
        data = zlib.compress(json.dumps(data).encode('utf-8'))
        self.__sio.emit('configure', data=data, namespace='/app')

    def is_connected(self):
        return self.__sio.connected

    def is_pending(self):
        return self.__pending_responses > 0

    def cancel_next(self):
        self.__cancel_next = True

    def get_response(self, rid: str):
        ++self.__pending_responses
        return self.__get_response(rid)

    # TODO: remove
    # TODO: should probably return also the error status
    def __get_response(self, rid: str):
        result = None
        print("Check state of rid: ", rid)
        while rid not in self.__results and not self.__cancel_next:
            # TODO: Given that we use a limited number of executors
            # per proxy, we should define a timeout.
            time.sleep(0.1)

        if self.__cancel_next:
            print("Canceled rid: ", rid)
        else:
            print("Got response for rid: ", rid)
            result = self.__results.pop(rid)

        self.__cancel_next = False
        --self.__pending_responses

        return result

    def register(self, action, callback, context=None):
        self.__callbacks[action].append((callback, context))

    def unregister(self, action, callback):
        self.__callbacks[action] = [(f, c) for f, c in self.__callbacks[action] if f != callback]

    def __notify(self, action, *args, **kwargs):
        for callback, context in self.__callbacks[action]:
            if context is not None:
                callback(context, *args, **kwargs)
            else:
                callback(*args, **kwargs)

    def resume(self, uid: str):
        self.__sio.emit('resume', data=uid, namespace='/app')

    def restore(self, qid: str):
        self.__sio.emit('restore', data=qid, namespace='/app')

    def assets(self, qid: str):
        self.__sio.emit('assets', data=qid, namespace='/app')

    def watch(self, qid: str):
        if self.__active_watch != qid:
            if self.__active_watch is not None:
                self.__partials.pop(self.__active_watch, None)
            self.__active_watch = qid
            self.__active_watch_hash = None
        self.__sio.emit('watch', data=qid, namespace='/app')

    def state(self, uid: str):
        self.__sio.emit('state', data=uid, namespace='/app')

    def reset_watch(self, qid: str):
        self.__sio.emit('reset_watch', data=qid, namespace='/app')

    def delete(self, qid: str):
        self.__sio.emit('delete', data=qid, namespace='/app')

    def challenge(self, challenge):
        self.__sio.emit('challenge', data=challenge, namespace='/app')

    def auth_by_challenge(self, challenge):
        self.__sio.emit('auth_by_challenge', data=challenge, namespace='/app')

    def auth_by_token(self, token: str):
        self.__sio.emit('auth_by_token', data=token, namespace='/app')

    def permission(self, userId: str, permission: str):
        data = {"userId": userId, "permission": permission}
        self.__sio.emit('permission', data=data, namespace='/app')

    def __setup_call_backs(self):
        @self.__sio.event
        def connect():

            logging.info(f"{self.__tag}: Connection established")
            self.__notify("connected", True)

        @self.__sio.event
        def connect_error(data):
            logging.error(f"{self.__tag}: Failed to establish connection")

        @self.__sio.event(namespace='/app')
        def response(data):
            response: Dict[str, Any] = data
            rid = None
            if "ray" in response:
                if "rid" in response["ray"]:
                    rid = response["ray"]["rid"]
                    # TODO: would be good to store results only for known/recent requests
                    #       in order to avoid a case with faulty server sending bad data
                    if "output" in response:
                        self.__results[rid] = (response["output"])

            self.__lock.acquire()
            if rid is not None and rid in self.__executions:
                self.__executions[rid].on_response(response)
            self.__lock.release()

            self.__notify("response", data)
            self.__notify("progress", response["ray"])

        @self.__sio.event(namespace='/app')
        def submitted(data):
            logging.debug(f"{self.__tag}: Data Received [submitted]: {data}")

            response: Dict[str, Any] = data
            rid = response["rid"]

            self.__lock.acquire()
            if rid in self.__executions:
                self.__executions[rid].on_progress(response)
            self.__lock.release()

            self.__notify("submitted", data)

        @self.__sio.event(namespace='/app')
        def settings(config):
            logging.debug(f"{self.__tag}: Data Received [settings]: {config}")
            response: Dict[str, Any] = config

            self.__notify("settings", response)

        @self.__sio.event(namespace='/app')
        def partial(data):
            # Currently there may be only one active watch
            # ignore data for unrecognized updates
            if self.__active_watch is None:
                return

            logging.debug(f"{self.__tag}: Partial data received [partial]: {data}")
            response: Dict[str, Any] = data

            if "output" in response:
                output = response["output"]
                old_hash = output.get("old_hash", None)
                new_hash = output.get("new_hash", None)
                refresh = output.get("refresh", True)
                qid = output.get("qid", None)

                if qid == self.__active_watch:
                    # Check consistency of updates
                    if refresh == False and old_hash != self.__active_watch_hash:
                        self.reset_watch(self.__active_watch)
                        return

                    current = {} if refresh == True else self.__partials.get(qid, None)

                    delta = Delta(output["delta"])

                    # Update object
                    self.__partials[qid] = delta + current

                    self.__active_watch_hash = new_hash
                    # We will pass as additional information to the user the qid
                    self.__notify("partial", self.__partials[qid], qid)

        # TODO: move functionality from sample app to here (without the remapping part)
        @self.__sio.event(namespace='/app')
        def progress(data):
            logging.debug(f"{self.__tag}: Data Received [progress]: {data}")

            response: Dict[str, Any] = data
            rid = response["rid"]

            self.__lock.acquire()
            if rid in self.__executions:
                self.__executions[rid].on_progress(response)
            self.__lock.release()

            self.__notify("progress", data)

        @self.__sio.event(namespace='/app')
        def restore(data):
            logging.debug(f"{self.__tag}: Data Received [restore]: {data}")
            response: Dict[str, Any] = data
            rid = None
            if response is not None and \
                    "ray" in response and response["ray"] is not None and \
                    "rid" in response["ray"]:
                rid = response["ray"]["rid"]
            else:
                return

            self.__lock.acquire()
            if rid in self.__executions:
                self.__executions[rid].on_restore(response)
            self.__lock.release()

            self.__notify("restore", data)

        @self.__sio.event(namespace='/app')
        def state(data):
            logging.debug(f"{self.__tag}: Data Received [state]: {data}")
            self.__notify("state", data)

        @self.__sio.event(namespace='/app')
        def assets(data):
            logging.debug(f"{self.__tag}: Data Received [assets]: {data}")
            self.__notify("assets", data)

        @self.__sio.event(namespace='/app')
        def pulse(data):
            logging.debug(f"{self.__tag}: Data Received [pulse]: {data}")

            response: Dict[str, Any] = data
            rid = response["rid"]

            self.__lock.acquire()
            if rid in self.__executions:
                self.__executions[rid].on_pulse()
            self.__lock.release()

            self.__notify("pulse", data)

        @self.__sio.event(namespace='/app')
        def challenge(data):
            logging.debug(f"{self.__tag}: Data Received [challenge]: {data}")
            # New challenge received
            self.__notify("challenge", data)

        @self.__sio.event(namespace='/app')
        def token(data):
            logging.debug(f"{self.__tag}: Data Received [token]: {data}")
            # New authentification token received
            self.__notify("token", data)
            if data is not None and data != "":
                # Notify authentification status
                self.__notify("authentified", True)

        @self.__sio.event(namespace='/app')
        def auth(data):
            logging.debug(f"{self.__tag}: Data Received [auth]: {data}")
            # Notify authentification status
            self.__notify("authentified", data)

        @self.__sio.event(namespace='/app')
        def error(data):
            logging.debug(f"{self.__tag}: Data Received [error]: {data}")
            # These are general errors, not related to a specific request
            self.__notify("error", data)

        @self.__sio.event
        def disconnect():
            logging.info(f"{self.__tag}: Disconnected from server")
            self.__notify("connected", False)
