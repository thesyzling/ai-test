import logging


# LogsHandler class

# This class is a subclass of StreamHandler from the logging module.
# It is used to handle logs and send them to the dispatcher.

class LogsHandler(logging.StreamHandler):
    level = logging.DEBUG

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        formatter = logging.Formatter("%(asctime)s: %(message)s")
        super().setFormatter(formatter)
        self.handler = None

    def setCustomHandler(self, handler):
        self.handler = handler

    def emit(self, record=None):
        if self.handler is None or record is None:
            return
        try:
            msg = self.format(record)

            self.handler(record.levelno, msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def install(self, context, level=logging.DEBUG):
        logger = logging.getLogger()
        logger.setLevel(level)
        logger.addHandler(self)
        self.setLevel(level)
        self.setCustomHandler(context.notifier.onLogMessage)

    def uninstall(self):
        self.setCustomHandler(None)
        # TODO: temporarily disabled as it hangs. to investigate and reenabled
        #logger = logging.getLogger()
        #logger.removeHandler(self)
        self.setLevel(logging.DEBUG)
