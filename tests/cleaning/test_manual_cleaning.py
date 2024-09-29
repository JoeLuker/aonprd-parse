# tests/cleaning/test_manual_cleaning.py

import pytest
from unittest.mock import patch, AsyncMock, MagicMock, ANY
from pathlib import Path
import aiosqlite

from src.cleaning.manual_cleaning import (
    process_file_async,
    clean_and_copy_files_async,
    connect_to_db,
    get_html_file_mapping,
    prepare_canonical_mapping,
    insert_canonical_mapping,
    get_skip_files,
)
from config.config import config
from src.cleaning.manual_cleaning import REPLACEMENTS


@pytest.fixture
def mock_conn():
    """Fixture to create a mock aiosqlite.Connection."""
    conn = AsyncMock(spec=aiosqlite.Connection)
    cursor = AsyncMock(spec=aiosqlite.Cursor)
    # Mock the context manager for conn.execute
    conn.execute.return_value.__aenter__.return_value = cursor
    # Mock fetchall to return predefined data
    cursor.fetchall.return_value = [
        ("duplicate1.html", "original1.html", "url1&SchoolSort=true", "url1"),
        ("duplicate2.html", "original2.html", "url2&SchoolSort=true", "url2"),
    ]
    # Mock cursor.description
    cursor.description = [
        ("file_with_sort",),
        ("file_without_sort",),
        ("url_with_sort",),
        ("url_without_sort",),
    ]
    # Ensure executemany and commit are AsyncMock instances
    conn.executemany = AsyncMock()
    conn.commit = AsyncMock()
    return conn


@pytest.fixture
def mock_config_fixture():
    """Fixture to provide a mock configuration."""
    return config


@pytest.mark.asyncio
async def test_connect_to_db_success():
    with patch(
        "src.cleaning.manual_cleaning.aiosqlite.connect", new_callable=AsyncMock
    ) as mock_connect:
        mock_conn_instance = AsyncMock(spec=aiosqlite.Connection)
        mock_connect.return_value = mock_conn_instance

        conn = await connect_to_db(Path("crawler_state.db"))
        mock_connect.assert_awaited_once_with(str(Path("crawler_state.db")))
        assert conn == mock_conn_instance


@pytest.mark.asyncio
async def test_connect_to_db_failure():
    with patch(
        "src.cleaning.manual_cleaning.aiosqlite.connect",
        new_callable=AsyncMock,
        side_effect=aiosqlite.Error("Failed to connect to database"),
    ):
        with pytest.raises(aiosqlite.Error) as excinfo:
            await connect_to_db(Path("crawler_state.db"))
        assert "Failed to connect to database" in str(excinfo.value)


@pytest.mark.asyncio
async def test_get_html_file_mapping(mock_conn):
    mapping = await get_html_file_mapping(mock_conn)
    assert len(mapping) == 2
    assert mapping[0]["file_with_sort"] == "duplicate1.html"
    assert mapping[0]["file_without_sort"] == "original1.html"
    assert mapping[1]["file_with_sort"] == "duplicate2.html"
    assert mapping[1]["file_without_sort"] == "original2.html"


@pytest.mark.asyncio
async def test_prepare_canonical_mapping():
    mapping_data = [
        {
            "file_with_sort": "duplicate1.html",
            "file_without_sort": "original1.html",
            "url_with_sort": "url1&SchoolSort=true",
            "url_without_sort": "url1",
        },
        {
            "file_with_sort": "duplicate2.html",
            "file_without_sort": "original2.html",
            "url_with_sort": "url2&SchoolSort=true",
            "url_without_sort": "url2",
        },
    ]
    canonical_mappings = await prepare_canonical_mapping(mapping_data)
    assert len(canonical_mappings) == 2
    assert canonical_mappings[0] == (
        "original1.html",
        "duplicate1.html",
        "url1",
        "url1&SchoolSort=true",
    )
    assert canonical_mappings[1] == (
        "original2.html",
        "duplicate2.html",
        "url2",
        "url2&SchoolSort=true",
    )


@pytest.mark.asyncio
async def test_insert_canonical_mapping(mock_conn):
    data_to_insert = [
        ("original1.html", "duplicate1.html", "url1", "url1&SchoolSort=true"),
        ("original2.html", "duplicate2.html", "url2", "url2&SchoolSort=true"),
    ]
    await insert_canonical_mapping(mock_conn, data_to_insert)
    mock_conn.executemany.assert_awaited_once_with(ANY, data_to_insert)
    mock_conn.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_skip_files(mock_conn):
    skip_files = await get_skip_files(mock_conn)
    expected = {
        "63634bccb56c98559dab055327186a07.html",
        "f6d4aaa54adebd6a564120102e5af5a8.html",
        "ed23e664aba0fb30e732b358bf07df25.html",
        "ebcc0f2ca46897ee6e13d42cd9e28517.html",
        "76f8753eaeb05d007c35258e2712db49.html",
        "duplicate1.html",
        "duplicate2.html",
    }
    assert skip_files == expected


@pytest.mark.asyncio
async def test_process_file_async_with_replacements(mock_config_fixture):
    source_path = Path("original.html")
    destination_path = Path("duplicate.html")
    content = "This is the original content."
    modified_content = "This is the duplicate content."

    with patch(
        "src.cleaning.manual_cleaning.FileOperations.read_file_async",
        new_callable=AsyncMock,
        return_value=content,
    ) as mock_read, patch(
        "src.cleaning.manual_cleaning.FileOperations.apply_replacements",
        new_callable=AsyncMock,
        return_value=(modified_content, {"original"}),
    ) as mock_replace, patch(
        "src.cleaning.manual_cleaning.FileOperations.write_file_async",
        new_callable=AsyncMock,
    ) as mock_write, patch(
        "src.cleaning.manual_cleaning.FileOperations.copy_async",
        new_callable=AsyncMock,
    ) as mock_copy:

        modified, applied = await process_file_async(
            source_path, destination_path, skip_files=set()
        )
        assert modified is True
        assert applied == {"original"}
        mock_read.assert_awaited_once_with(source_path)
        mock_replace.assert_awaited_once_with(content, REPLACEMENTS)
        mock_write.assert_awaited_once_with(destination_path, modified_content)
        mock_copy.assert_not_called()


@pytest.mark.asyncio
async def test_process_file_async_no_replacements(mock_config_fixture):
    source_path = Path("original.html")
    destination_path = Path("duplicate.html")
    content = "This is the content without replacements."

    with patch(
        "src.cleaning.manual_cleaning.FileOperations.read_file_async",
        new_callable=AsyncMock,
        return_value=content,
    ) as mock_read, patch(
        "src.cleaning.manual_cleaning.FileOperations.apply_replacements",
        new_callable=AsyncMock,
        return_value=(content, set()),
    ) as mock_replace, patch(
        "src.cleaning.manual_cleaning.FileOperations.copy_async",
        new_callable=AsyncMock,
    ) as mock_copy, patch(
        "src.cleaning.manual_cleaning.FileOperations.write_file_async",
        new_callable=AsyncMock,
    ) as mock_write:

        modified, applied = await process_file_async(
            source_path, destination_path, skip_files=set()
        )
        assert modified is False
        assert applied == set()
        mock_read.assert_awaited_once_with(source_path)
        mock_replace.assert_awaited_once_with(content, REPLACEMENTS)
        mock_copy.assert_awaited_once_with(source_path, destination_path)
        mock_write.assert_not_called()


@pytest.mark.asyncio
async def test_process_file_async_skip_file():
    source_path = Path("duplicate1.html")
    destination_path = Path("duplicate1.html")

    with patch(
        "src.cleaning.manual_cleaning.FileOperations.copy_async",
        new_callable=AsyncMock,
    ) as mock_copy:
        modified, applied = await process_file_async(
            source_path, destination_path, skip_files={"duplicate1.html"}
        )
        assert modified is False
        assert applied == set()
        mock_copy.assert_awaited_once_with(source_path, destination_path)


@pytest.mark.asyncio
@patch("src.cleaning.manual_cleaning.process_file_async", new_callable=AsyncMock)
@patch("pathlib.Path.is_file", return_value=True)
async def test_clean_and_copy_files_async(
    mock_is_file, mock_process_file_async, mock_config_fixture
):
    skip_files = {"duplicate1.html", "duplicate2.html"}
    files = [
        Path("original1.html"),
        Path("duplicate1.html"),
        Path("original2.html"),
        Path("duplicate2.html"),
        Path("original3.html"),
    ]

    async def mock_list_files(_):
        for file in files:
            yield file

    # Mock the results of process_file_with_name
    mock_process_file_async.side_effect = [
        (True, {"original1"}),
        (False, set()),
        (False, set()),
        (False, set()),
        (True, {"original3"}),
    ]

    with patch(
        "src.cleaning.manual_cleaning.FileOperations.list_files", new=mock_list_files
    ):
        modifications, replacement_counts = await clean_and_copy_files_async(skip_files)

        assert modifications == ["original1.html", "original3.html"]
        assert replacement_counts == {"original1": 1, "original3": 1}

    # Verify that process_file_async was called with correct arguments
    mock_process_file_async.assert_any_await(
        files[0],  # original1.html
        config.paths.manual_cleaned_html_data / "original1.html",
        skip_files,
    )
    mock_process_file_async.assert_any_await(
        files[4],  # original3.html
        config.paths.manual_cleaned_html_data / "original3.html",
        skip_files,
    )
