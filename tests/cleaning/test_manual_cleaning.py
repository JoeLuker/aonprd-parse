# tests/cleaning/test_manual_cleaning.py

import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import sqlite3
import asyncio

from src.cleaning.manual_cleaning import (
    process_file_async,
    clean_and_copy_files_async,
    connect_to_db,
    get_html_file_mapping,
    prepare_canonical_mapping,
    insert_canonical_mapping,
    get_skip_files
)
from config.config import config

@pytest.fixture
async def mock_conn():
    conn = MagicMock(spec=sqlite3.Connection)
    return conn

@pytest.fixture
def mock_cursor():
    cursor = MagicMock(spec=sqlite3.Cursor)
    return cursor

@pytest.mark.asyncio
async def test_connect_to_db_success():
    with patch("asyncio.to_thread") as mock_to_thread:
        mock_conn = MagicMock()
        mock_to_thread.return_value = mock_conn
        conn = await connect_to_db(Path("crawler_state.db"))
        mock_to_thread.assert_called_once_with(sqlite3.connect, str(Path("crawler_state.db")))
        assert conn == mock_conn

@pytest.mark.asyncio
async def test_connect_to_db_failure():
    with patch("asyncio.to_thread", side_effect=Exception("Connection failed")):
        with pytest.raises(Exception) as excinfo:
            await connect_to_db(Path("crawler_state.db"))
        assert "Failed to connect to database" in str(excinfo.value)

@pytest.mark.asyncio
async def test_get_html_file_mapping(mock_conn, mock_cursor):
    mock_conn.execute.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        ("duplicate1.html", "original1.html", "url1&SchoolSort=true", "url1"),
        ("duplicate2.html", "original2.html", "url2&SchoolSort=true", "url2")
    ]
    mock_cursor.description = [
        ("file_with_sort",),
        ("file_without_sort",),
        ("url_with_sort",),
        ("url_without_sort",)
    ]
    mapping = await get_html_file_mapping(mock_conn)
    assert len(mapping) == 2
    assert mapping[0]['file_with_sort'] == "duplicate1.html"
    assert mapping[0]['file_without_sort'] == "original1.html"

@pytest.mark.asyncio
async def test_prepare_canonical_mapping():
    mapping_data = [
        {
            'file_with_sort': "duplicate1.html",
            'file_without_sort': "original1.html",
            'url_with_sort': "url1&SchoolSort=true",
            'url_without_sort': "url1"
        },
        {
            'file_with_sort': "duplicate2.html",
            'file_without_sort': "original2.html",
            'url_with_sort': "url2&SchoolSort=true",
            'url_without_sort': "url2"
        }
    ]
    canonical_mappings = await prepare_canonical_mapping(mapping_data)
    assert len(canonical_mappings) == 2
    assert canonical_mappings[0] == ("original1.html", "duplicate1.html", "url1", "url1&SchoolSort=true")

@pytest.mark.asyncio
async def test_insert_canonical_mapping(mock_conn):
    data_to_insert = [
        ("original1.html", "duplicate1.html", "url1", "url1&SchoolSort=true"),
        ("original2.html", "duplicate2.html", "url2", "url2&SchoolSort=true")
    ]
    await insert_canonical_mapping(mock_conn, data_to_insert)
    mock_conn.executemany.assert_called_once()
    mock_conn.commit.assert_called_once()

@pytest.mark.asyncio
async def test_get_skip_files(mock_conn, mock_cursor):
    mock_conn.execute.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        ("duplicate1.html", "original1.html", "url1&SchoolSort=true", "url1"),
        ("duplicate2.html", "original2.html", "url2&SchoolSort=true", "url2")
    ]
    skip_files = await get_skip_files(mock_conn)
    expected = {
        '63634bccb56c98559dab055327186a07.html',
        'f6d4aaa54adebd6a564120102e5af5a8.html',
        'ed23e664aba0fb30e732b358bf07df25.html',
        'ebcc0f2ca46897ee6e13d42cd9e28517.html',
        '76f8753eaeb05d007c35258e2712db49.html',
        'duplicate1.html',
        'duplicate2.html'
    }
    assert skip_files == expected

@pytest.mark.asyncio
async def test_process_file_async_with_replacements():
    source_path = Path("original.html")
    destination_path = Path("duplicate.html")
    content = "This is the original content."
    modified_content = "This is the duplicate content."
    
    with patch("src.utils.file_operations.FileOperations.read_file_async", return_value=content) as mock_read:
        with patch("src.utils.file_operations.FileOperations.apply_replacements", return_value=(modified_content, {"original"})) as mock_replace:
            with patch("src.utils.file_operations.FileOperations.write_file_async") as mock_write:
                modified, applied = await process_file_async(source_path, destination_path, skip_files=set())
                assert modified is True
                assert applied == {"original"}
                mock_read.assert_awaited_once_with(source_path)
                mock_replace.assert_awaited_once_with(content, config.cleaning.replacements)
                mock_write.assert_awaited_once_with(destination_path, modified_content)

@pytest.mark.asyncio
async def test_process_file_async_no_replacements():
    source_path = Path("original.html")
    destination_path = Path("duplicate.html")
    content = "This is the content without replacements."
    
    with patch("src.utils.file_operations.FileOperations.read_file_async", return_value=content) as mock_read:
        with patch("src.utils.file_operations.FileOperations.apply_replacements", return_value=(content, set())) as mock_replace:
            with patch("src.utils.file_operations.FileOperations.copy_async") as mock_copy:
                modified, applied = await process_file_async(source_path, destination_path, skip_files=set())
                assert modified is False
                assert applied == set()
                mock_read.assert_awaited_once_with(source_path)
                mock_replace.assert_awaited_once_with(content, config.cleaning.replacements)
                mock_copy.assert_awaited_once_with(source_path, destination_path)

@pytest.mark.asyncio
async def test_process_file_async_skip_file():
    source_path = Path("duplicate1.html")
    destination_path = Path("duplicate1.html")
    
    with patch("src.utils.file_operations.FileOperations.copy_async") as mock_copy:
        modified, applied = await process_file_async(source_path, destination_path, skip_files={"duplicate1.html"})
        assert modified is False
        assert applied == set()
        mock_copy.assert_awaited_once_with(source_path, destination_path)

@pytest.mark.asyncio
async def test_clean_and_copy_files_async():
    skip_files = {"duplicate1.html", "duplicate2.html"}
    files = [
        Path("original1.html"),
        Path("duplicate1.html"),
        Path("original2.html"),
        Path("duplicate2.html"),
        Path("original3.html")
    ]
    
    with patch("config.config.paths.input_folder.iterdir", return_value=files):
        with patch("src.utils.file_operations.FileOperations.read_file_async") as mock_read:
            with patch("src.utils.file_operations.FileOperations.apply_replacements") as mock_replace:
                with patch("src.utils.file_operations.FileOperations.write_file_async") as mock_write:
                    with patch("src.utils.file_operations.FileOperations.copy_async") as mock_copy:
                        def side_effect_apply_replacements(content, replacements):
                            if "original1" in content:
                                return ("This is the duplicate1 content.", {"original1"})
                            elif "original2" in content:
                                return (content, set())
                            elif "original3" in content:
                                return ("This is the duplicate3 content.", {"original3"})
                            return (content, set())
                        
                        mock_replace.side_effect = side_effect_apply_replacements
                        mock_read.side_effect = ["This is the original1 content.", "This is the original2 content.", "This is the original3 content."]
                        
                        modifications, replacement_counts = await clean_and_copy_files_async(skip_files)
                        
                        assert modifications == ["original1.html", "original3.html"]
                        assert replacement_counts == {"original1": 1, "original3": 1}
                        
                        mock_write.assert_any_await(config.paths.manual_cleaned_html_data / "original1.html", "This is the duplicate1 content.")
                        mock_write.assert_any_await(config.paths.manual_cleaned_html_data / "original3.html", "This is the duplicate3 content.")
                        
                        mock_copy.assert_any_await(config.paths.input_folder / "duplicate1.html", config.paths.manual_cleaned_html_data / "duplicate1.html")
                        mock_copy.assert_any_await(config.paths.input_folder / "duplicate2.html", config.paths.manual_cleaned_html_data / "duplicate2.html")
                        mock_copy.assert_any_await(config.paths.input_folder / "original2.html", config.paths.manual_cleaned_html_data / "original2.html")