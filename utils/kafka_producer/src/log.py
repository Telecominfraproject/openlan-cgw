import logging


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
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, style="{")
        return formatter.format(record)


logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S")

console = logging.StreamHandler()
console.setLevel(logging.NOTSET)
console.setFormatter(ColoredFormatter())

logger = logging.getLogger(__name__)
logger.propagate = False
logger.addHandler(console)
