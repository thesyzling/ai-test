import logging, os, traceback

class OpenfabricLogger:
    COLORS = {
        "DEBUG": "\033[94m",     # Blue
        "INFO": "\033[92m",      # Green
        "WARNING": "\033[93m",   # Yellow
        "ERROR": "\033[91m",     # Red
        "CRITICAL": "\033[95m",  # Magenta
        "TRACEBACK": "\033[90m", # Gray
        "RESET": "\033[0m"       # Reset color
    }

    def __init__(self, name="sdk"):
        self.verbose = os.getenv("OPENFABRIC_SDK_DEBUG", "false").lower() == "true"
        self.include_stacktrace = self.verbose
        self.logger = logging.getLogger(name)

        # Custom log formatter
        custom_formatter = logging.Formatter(self._format_message())

        # Create a handler
        custom_handler = logging.StreamHandler()
        custom_handler.setFormatter(custom_formatter)

        # Attach handler
        self.logger.addHandler(custom_handler)
        self.logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        logging.getLogger('socketio').setLevel(logging.ERROR)
        logging.getLogger('engineio').setLevel(logging.ERROR)

    def _format_message(self):
        return (
            "%(asctime)s | %(name)s | "
            "%(levelname)s | " + "%(message)s"
        )

    def _colorize(self, level, message):
        color = self.COLORS.get(level, self.COLORS["RESET"])
        return f"{color}{message}{self.COLORS['RESET']}"

    def _get_traceback(self):
        """Returns a formatted traceback if an exception is active."""
        if self.include_stacktrace:
            if traceback.format_exc().strip() != "NoneType: None":
                return f"{self.COLORS['TRACEBACK']}Exception callstack :\n{traceback.format_exc()}{self.COLORS['RESET']}"
            else:
                raw_tb = traceback.format_stack()
                formatted_traceback = [f"{self.COLORS['TRACEBACK']}Error callstack :{self.COLORS['RESET']}"]
                # we need to skip the last 2 lines as they are not useful
                for line in raw_tb[-5:-2]:  
                    line = line.strip()
                    if 'site-packages' in line:
                        start = line.find('site-packages') + len('site-packages/')
                        line = line[start:]

                    # Format output nicely
                    line = line.replace('File "', f"\n{self.COLORS['TRACEBACK']}File ").replace('", line', '", Line')
                    formatted_traceback.append(f"{self.COLORS['TRACEBACK']}{line}{self.COLORS['RESET']}")

                return "\n".join(formatted_traceback) if len(formatted_traceback) > 1 else ""

        return ""

    def debug(self, message):
        self.logger.debug(self._colorize("DEBUG", message))

    def info(self, message):
        self.logger.info(self._colorize("INFO", message))

    def warning(self, message):
        self.logger.warning(self._colorize("WARNING", message))

    def error(self, message):
        tb_message = self._get_traceback()
        self.logger.error(self._colorize("ERROR", message) + ("\n" + tb_message if tb_message else ""))

    def critical(self, message):
        tb_message = self._get_traceback()
        self.logger.critical(self._colorize("CRITICAL", message) + ("\n" + tb_message if tb_message else ""))

    def log(self, level, message):
        self.logger.log(level, message)

logger = OpenfabricLogger("sdk")
logger_worker = OpenfabricLogger("worker")
