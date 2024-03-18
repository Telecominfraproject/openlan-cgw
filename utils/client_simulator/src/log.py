import logging


TRACE_LEVEL = logging.DEBUG - 5
TRACE_NAME = "TRACE"


class ColoredFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    white = "\x1b[37;20m"
    cyan = "\x1b[36;20m"
    reset = "\x1b[0m"
    format = "{asctime}|{levelname}|{threadName}|{funcName}:{lineno}\t{message}"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: cyan + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
        TRACE_LEVEL: white + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, style="{")
        return formatter.format(record)


def __trace(self, msg, *args, **kwargs):
    """
    Log 'msg % args' with severity 'TRACE'.

    logger.trace("periodic log")
    """
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, msg, args, **kwargs)


logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S")

logging.addLevelName(TRACE_LEVEL, TRACE_NAME)
setattr(logging, TRACE_NAME, TRACE_LEVEL)
setattr(logging.getLoggerClass(), TRACE_NAME.lower(), __trace)

console = logging.StreamHandler()
console.setLevel(logging.NOTSET)
console.setFormatter(ColoredFormatter())

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(console)
logging.getLogger('websockets.client').setLevel(logging.INFO)
