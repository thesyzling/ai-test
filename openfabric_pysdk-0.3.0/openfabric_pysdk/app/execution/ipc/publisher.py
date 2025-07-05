import zmq


class Publisher:
    # ------------------------------------------------------------------------
    def __init__(self, address="tcp://127.0.0.1:5556"):
        self.context = zmq.Context()
        self.publisher_socket = self.context.socket(zmq.PUB)
        self.publisher_socket.bind(address)

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            # Object is deleted on close. Error would relate to python closing.
            print(e)
            pass

    # ------------------------------------------------------------------------
    def publish(self, message):
        self.publisher_socket.send(message)

    # ------------------------------------------------------------------------
    def close(self):
        if self.publisher_socket is not None:
            self.publisher_socket.close()
        if self.context is not None:
            self.context.destroy()
        self.publisher_socket = None
        self.context = None
