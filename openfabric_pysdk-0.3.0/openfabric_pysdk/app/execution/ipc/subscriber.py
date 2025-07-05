import threading

import gevent
import zmq


class Subscriber:

    # ------------------------------------------------------------------------
    def __init__(self, address="tcp://127.0.0.1:5556", callback=None, is_gevent: bool = False):
        self.context = zmq.Context()

        self.subscriber_socket = self.context.socket(zmq.SUB)
        self.subscriber_socket.connect(address)
        self.subscriber_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        self.running = True

        self.on_message_received_callback = callback
        if is_gevent:
            self.subscriber_greenlet = gevent.spawn(self.__message_handler2, self.subscriber_socket)
        else:
            self.__execution = threading.Thread(target=self.__message_handler, name="zmq_subscriber", args=(self.subscriber_socket,))
            self.__execution.start()

    # ------------------------------------------------------------------------
    def __del__(self):
        try:
            self.close()
            if self.__execution is not None:
                self.__execution.join()
            self.__execution = None
        except Exception as e:
            # Object is deleted on close. Error would relate to python closing.
            print(e)
            pass

    # ------------------------------------------------------------------------
    def __message_handler(self, socket):
        while self.running:
            try:
                message = socket.recv(flags=zmq.NOBLOCK)
                if self.on_message_received_callback is not None:
                    self.on_message_received_callback(message)
            except zmq.Again:
                gevent.sleep(0.1)
                pass

    # ------------------------------------------------------------------------
    # Todo: try to switch to this message handler
    def __message_handler2(self, socket):
        while self.running:
            message = socket.recv()
            if self.on_message_received_callback is not None:
                self.on_message_received_callback(message)
            gevent.sleep(0.001)

    # ------------------------------------------------------------------------
    def register_callback(self, callback):
        self.on_message_received_callback = callback

    # ------------------------------------------------------------------------
    def close(self):
        self.running = False
        if self.subscriber_socket is not None:
            self.subscriber_socket.close()
        if self.context is not None:
            self.context.destroy()
        self.subscriber_socket = None
        self.context = None
