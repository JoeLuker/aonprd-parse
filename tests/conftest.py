# tests/conftest.py

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path


@pytest.fixture
def sample_structure():
    return {
        "nodes": [
            {"id": "n1", "type": "document", "filename": "file1.html"},
            {"id": "n2", "type": "tag", "name": "p", "attributes_id": "a1"},
            {"id": "n3", "type": "textnode", "data_id": "txt1"},
        ],
        "edges": [
            {
                "source": "n1",
                "target": "n2",
                "relationship": "CONTAINS_TAG",
                "order": 1,
            },
            {
                "source": "n2",
                "target": "n3",
                "relationship": "CONTAINS_TEXT",
                "order": 1,
            },
        ],
    }


@pytest.fixture
def sample_data():
    return {
        "doctypes": {"d1": "<!DOCTYPE html>"},
        "comments": {"c1": "This is a comment."},
        "texts": {"txt1": "Sample text."},
        "attributes": {"a1": {"class": "text"}},
    }


@pytest.fixture
def mock_logger():
    with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
        mock_logger = MagicMock()
        mock_logger_get.return_value = mock_logger
        yield mock_logger


@pytest.fixture
def mock_config():
    # Adjust the patch target based on how 'config' is imported in the code under test
    with patch("src.cleaning.manual_cleaning.config.config") as mock_conf:
        mock_conf.paths = MagicMock()
        mock_conf.paths.input_folder = Path("/mock/input")
        mock_conf.paths.manual_cleaned_html_data = Path("/mock/manual_cleaned_html")
        mock_conf.paths.consolidated_dir = Path("/mock/consolidated")
        mock_conf.paths.log_dir = Path("/mock/logs")
        mock_conf.paths.decomposed_output_dir = Path("/mock/decomposed")
        mock_conf.paths.condensed_output_dir = Path("/mock/condensed")
        mock_conf.paths.processing_output_dir = Path("/mock/processed")
        mock_conf.paths.import_files_dir = Path("/mock/import_files")
        
        mock_conf.processing = MagicMock()
        mock_conf.processing.max_files = 1000
        mock_conf.processing.similarity_threshold = 0.99
        mock_conf.processing.max_workers = 16
        
        mock_conf.memgraph = MagicMock()
        mock_conf.memgraph.host = "localhost"
        mock_conf.memgraph.port = 7687
        mock_conf.memgraph.batch_size = 1000
        
        mock_conf.files = MagicMock()
        mock_conf.files.data_pickle = "data.pickle"
        mock_conf.files.structure_pickle = "structure.pickle"
        mock_conf.files.filtered_data_pickle = "filtered_data.pickle"
        mock_conf.files.filtered_structure_pickle = "filtered_structure.pickle"
        mock_conf.files.data_yaml = "data.yaml"
        mock_conf.files.structure_yaml = "structure.yaml"
        mock_conf.files.filtered_data_yaml = "filtered_data.yaml"
        mock_conf.files.filtered_structure_yaml = "filtered_structure.yaml"
        
        mock_conf.database = MagicMock()
        mock_conf.database.consolidated_html_db = Path("/mock/consolidated_html_data.db")
        mock_conf.database.crawler_db = Path("/mock/crawler_state.db")
        
        mock_conf.logging = MagicMock()
        mock_conf.logging.processor_log = "html_processor.log"
        mock_conf.logging.csv_prep_log = "yaml_csv_prep.log"
        mock_conf.logging.memgraph_importer_log = "memgraph_importer.log"
        mock_conf.logging.unwrap_log = "unwrap_matching_nodes.log"
        mock_conf.logging.console_level = "INFO"
        mock_conf.logging.file_level = "DEBUG"
        
        yield mock_conf


@pytest.fixture
def mock_file_operations():
    with patch("src.utils.file_operations.FileOperations") as mock_fo:
        mock_fo.read_file_async = AsyncMock()
        mock_fo.write_file_async = AsyncMock()
        mock_fo.copy_async = AsyncMock()
        # 'apply_replacements' and 'ensure_directory' are synchronous methods
        # No need to mock them here; they can be mocked individually in tests if needed
        yield mock_fo


@pytest.fixture
def mock_data_handler():
    with patch("src.utils.data_handling.DataHandler") as mock_dh:
        mock_dh.load_yaml = AsyncMock()
        mock_dh.save_yaml = AsyncMock()
        mock_dh.load_pickle = AsyncMock()
        mock_dh.save_pickle = AsyncMock()
        yield mock_dh


@pytest.fixture
def mock_database_connection():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn
