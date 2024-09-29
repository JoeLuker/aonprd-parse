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
        assert "Read error" in str(excinfo.value)


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
        assert "Write error" in str(excinfo.value)


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
        assert "Copy error" in str(excinfo.value)


@pytest.mark.asyncio
async def test_apply_replacements():
    content = "Hello <<World>>!"
    replacements = {"<<": "<", ">>": ">"}
    expected_content = "Hello <World>!"

    modified_content, applied_replacements = await FileOperations.apply_replacements(
        content, replacements
    )
    assert modified_content == expected_content
    assert applied_replacements == {"<<", ">>"}


@pytest.mark.asyncio
async def test_apply_replacements_no_changes():
    content = "Hello World!"
    replacements = {"<<": "<", ">>": ">"}
    expected_content = "Hello World!"

    modified_content, applied_replacements = await FileOperations.apply_replacements(
        content, replacements
    )
    assert modified_content == expected_content
    assert applied_replacements == set()


@pytest.mark.asyncio
async def test_ensure_directory_exists():
    with patch("pathlib.Path.mkdir") as mock_mkdir:
        directory = Path("/fake/directory")
        await FileOperations.ensure_directory(directory)
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


@pytest.mark.asyncio
async def test_ensure_directory_failure():
    with patch("pathlib.Path.mkdir", side_effect=Exception("mkdir error")):
        with pytest.raises(Exception) as excinfo:
            await FileOperations.ensure_directory(Path("/fake/directory"))
        assert "mkdir error" in str(excinfo.value)