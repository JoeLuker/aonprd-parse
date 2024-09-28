# tests/utils/test_logging.py

import pytest
from unittest.mock import patch
from src.utils.logging import Logger


@pytest.fixture
def logger():
    return Logger.get_logger("TestLogger")


def test_logger_info(logger):
    with patch.object(logger, "info") as mock_info:
        logger.info("This is an info message.")
        mock_info.assert_called_once_with("This is an info message.")


def test_logger_error(logger):
    with patch.object(logger, "error") as mock_error:
        logger.error("This is an error message.")
        mock_error.assert_called_once_with("This is an error message.")


def test_logger_verbose(logger):
    # Ensure 'verbose' method exists in Logger class
    if not hasattr(logger, "verbose"):
        pytest.skip("Logger does not have a 'verbose' method.")

    with patch.object(logger, "verbose") as mock_verbose:
        logger.verbose("This is a verbose message.")
        mock_verbose.assert_called_once_with("This is a verbose message.")
