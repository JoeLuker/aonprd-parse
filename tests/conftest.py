# tests/conftest.py

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

@pytest.fixture
def sample_structure():
    return {
        'nodes': [
            {'id': 'n1', 'type': 'document', 'filename': 'file1.html'},
            {'id': 'n2', 'type': 'tag', 'name': 'p', 'attributes_id': 'a1'},
            {'id': 'n3', 'type': 'textnode', 'data_id': 'txt1'}
        ],
        'edges': [
            {'source': 'n1', 'target': 'n2', 'relationship': 'CONTAINS_TAG', 'order': 1},
            {'source': 'n2', 'target': 'n3', 'relationship': 'CONTAINS_TEXT', 'order': 1}
        ]
    }

@pytest.fixture
def sample_data():
    return {
        'doctypes': {'d1': '<!DOCTYPE html>'},
        'comments': {'c1': 'This is a comment.'},
        'texts': {'txt1': 'Sample text.'},
        'attributes': {'a1': {'class': 'text'}}
    }

@pytest.fixture
def mock_logger():
    with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
        mock_logger = MagicMock()
        mock_logger_get.return_value = mock_logger
        yield mock_logger

@pytest.fixture
def mock_config():
    with patch("config.config") as mock_conf:
        mock_conf.paths.input_folder = Path("/mock/input")
        mock_conf.paths.output_folder = Path("/mock/output")
        mock_conf.database.crawler_db = Path("/mock/crawler_state.db")
        mock_conf.database.consolidated_html_db = Path("/mock/consolidated_html_data.db")
        mock_conf.processing.max_files = 1000
        mock_conf.memgraph.host = "localhost"
        mock_conf.memgraph.port = 7687
        yield mock_conf

@pytest.fixture
async def mock_file_operations():
    with patch("src.utils.file_operations.FileOperations") as mock_fo:
        mock_fo.read_file_async = MagicMock()
        mock_fo.write_file_async = MagicMock()
        mock_fo.copy_async = MagicMock()
        mock_fo.apply_replacements = MagicMock()
        mock_fo.ensure_directory = MagicMock()
        yield mock_fo

@pytest.fixture
async def mock_data_handler():
    with patch("src.utils.data_handling.DataHandler") as mock_dh:
        mock_dh.load_yaml = MagicMock()
        mock_dh.save_yaml = MagicMock()
        mock_dh.load_pickle = MagicMock()
        mock_dh.save_pickle = MagicMock()
        yield mock_dh

@pytest.fixture
def mock_database_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn