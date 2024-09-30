# src/utils/logging.py
import logging
from pathlib import Path
from typing import Optional


# Define VERBOSE Level
VERBOSE_LEVEL_NUM = 5
logging.addLevelName(VERBOSE_LEVEL_NUM, "VERBOSE")


def verbose(self, message, *args, **kws):
    if self.isEnabledFor(VERBOSE_LEVEL_NUM):
        self._log(VERBOSE_LEVEL_NUM, message, args, **kws)


logging.Logger.verbose = verbose


class Logger:
    _loggers = {}

    @staticmethod
    def get_logger(name: str, log_file: Optional[Path] = None) -> logging.Logger:
        if name not in Logger._loggers:
            logger = logging.getLogger(name)
            logger.setLevel(logging.DEBUG)

            # Formatter with contextual info
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            # Console Handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

            if log_file:
                # Ensure the log directory exists
                log_file.parent.mkdir(parents=True, exist_ok=True)

                # File Handler
                file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

            Logger._loggers[name] = logger

        return Logger._loggers[name]

    @staticmethod
    def set_log_level(level: str):
        """
        Set the log level for all loggers.
        """
        numeric_level = getattr(logging, level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {level}")

        for logger in Logger._loggers.values():
            logger.setLevel(numeric_level)
            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    handler.setLevel(logging.INFO)
                elif isinstance(handler, logging.FileHandler):
                    handler.setLevel(logging.DEBUG)
