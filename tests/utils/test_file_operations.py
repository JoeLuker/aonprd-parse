# tests/utils/test_file_operations.py

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from pathlib import Path

from src.utils.file_operations import FileOperations


@pytest.mark.asyncio
async def test_read_file_async_success():
    mock_content = "Sample file content"
    mock_aiofiles = AsyncMock()
    mock_aiofiles.__aenter__.return_value.read = AsyncMock(return_value=mock_content)

    with patch("src.utils.file_operations.aiofiles.open", return_value=mock_aiofiles):
        content = await FileOperations.read_file_async(Path("dummy.txt"))
        assert content == mock_content
        mock_aiofiles.__aenter__.return_value.read.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_file_async_failure():
    mock_aiofiles = AsyncMock()
    mock_aiofiles.__aenter__.side_effect = Exception("Read error")

    with patch("src.utils.file_operations.aiofiles.open", return_value=mock_aiofiles):
        with pytest.raises(Exception) as excinfo:
            await FileOperations.read_file_async(Path("dummy.txt"))
        assert "Failed to read file asynchronously" in str(excinfo.value)


@pytest.mark.asyncio
async def test_write_file_async_success():
    mock_content = "Sample file content"
    mock_aiofiles = AsyncMock()
    mock_aiofiles.__aenter__.return_value.write = AsyncMock(return_value=None)

    with patch("src.utils.file_operations.aiofiles.open", return_value=mock_aiofiles):
        await FileOperations.write_file_async(Path("dummy.txt"), mock_content)
        mock_aiofiles.__aenter__.return_value.write.assert_awaited_once_with(
            mock_content
        )


@pytest.mark.asyncio
async def test_write_file_async_failure():
    mock_aiofiles = AsyncMock()
    mock_aiofiles.__aenter__.return_value.write.side_effect = Exception("Write error")

    with patch("src.utils.file_operations.aiofiles.open", return_value=mock_aiofiles):
        with pytest.raises(Exception) as excinfo:
            await FileOperations.write_file_async(Path("dummy.txt"), "Content")
        assert "Failed to write file asynchronously" in str(excinfo.value)
        mock_aiofiles.__aenter__.return_value.write.assert_awaited_once_with("Content")


@pytest.mark.asyncio
async def test_copy_async_success():
    with patch("src.utils.file_operations.shutil.copy2", return_value=None) as mock_copy2:
        await FileOperations.copy_async(Path("source.txt"), Path("destination.txt"))
        mock_copy2.assert_called_once_with(Path("source.txt"), Path("destination.txt"))


@pytest.mark.asyncio
async def test_copy_async_failure():
    with patch("src.utils.file_operations.shutil.copy2", side_effect=Exception("Copy error")) as mock_copy2:
        with pytest.raises(Exception) as excinfo:
            await FileOperations.copy_async(Path("source.txt"), Path("destination.txt"))
        assert "Failed to copy file asynchronously" in str(excinfo.value)
        mock_copy2.assert_called_once_with(Path("source.txt"), Path("destination.txt"))


def test_apply_replacements():
    content = "Hello <<World>>!"
    replacements = {"<<": "<", ">>": ">"}
    expected_content = "Hello <World>!"

    # Assuming apply_replacements is a synchronous method
    modified_content, applied_replacements = FileOperations.apply_replacements(
        content, replacements
    )
    assert modified_content == expected_content
    assert applied_replacements == {"<<", ">>"}


def test_apply_replacements_no_changes():
    content = "Hello World!"
    replacements = {"<<": "<", ">>": ">"}
    expected_content = "Hello World!"

    modified_content, applied_replacements = FileOperations.apply_replacements(
        content, replacements
    )
    assert modified_content == expected_content
    assert applied_replacements == set()


def test_ensure_directory_exists():
    with patch("pathlib.Path.mkdir") as mock_mkdir:
        directory = Path("/fake/directory")
        FileOperations.ensure_directory(directory)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


def test_ensure_directory_failure():
    with patch("pathlib.Path.mkdir", side_effect=Exception("mkdir error")):
        with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
            mock_logger = MagicMock()
            mock_logger_get.return_value = mock_logger
            with pytest.raises(Exception) as excinfo:
                FileOperations.ensure_directory(Path("/fake/directory"))
            assert "Failed to create directory" in str(excinfo.value)
            mock_logger.error.assert_called_once_with(
                "Failed to create directory: mkdir error"
            )
