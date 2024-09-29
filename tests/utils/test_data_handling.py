import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from pathlib import Path
import pickle
import yaml
import aiofiles

from src.utils.data_handling import DataHandler

class AsyncContextManagerMock:
    def __init__(self, mock_file):
        self.mock_file = mock_file

    async def __aenter__(self):
        return self.mock_file

    async def __aexit__(self, exc_type, exc, tb):
        pass

@pytest.fixture
def mock_aiofiles_open():
    mock = MagicMock(spec=aiofiles.open)
    mock.return_value = AsyncContextManagerMock(AsyncMock())
    return mock

@pytest.mark.asyncio
async def test_load_yaml_success(mock_aiofiles_open):
    mock_data = {'key': 'value'}
    yaml_content = yaml.dump(mock_data)
    
    mock_file = mock_aiofiles_open.return_value.mock_file
    mock_file.read.return_value = yaml_content

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("yaml.safe_load", return_value=mock_data) as mock_yaml_load:
            result = await DataHandler.load_yaml(Path("dummy.yaml"))
            mock_yaml_load.assert_called_once_with(yaml_content)
            assert result == mock_data

@pytest.mark.asyncio
async def test_load_yaml_failure(mock_aiofiles_open):
    mock_aiofiles_open.side_effect = Exception("File not found")

    with patch("aiofiles.open", mock_aiofiles_open):
        with pytest.raises(Exception) as excinfo:
            await DataHandler.load_yaml(Path("dummy.yaml"))
        assert "Failed to load YAML from dummy.yaml: File not found" in str(excinfo.value)

@pytest.mark.asyncio
async def test_save_yaml_success(mock_aiofiles_open):
    data = {'key': 'value'}
    yaml_content = yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    mock_file = mock_aiofiles_open.return_value.mock_file

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("yaml.dump", return_value=yaml_content) as mock_yaml_dump:
            await DataHandler.save_yaml(data, Path("dummy.yaml"))
            mock_yaml_dump.assert_called_once_with(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
            mock_file.write.assert_awaited_once_with(yaml_content)

@pytest.mark.asyncio
async def test_save_yaml_failure(mock_aiofiles_open):
    data = {'key': 'value'}
    mock_aiofiles_open.side_effect = Exception("Write error")

    with patch("aiofiles.open", mock_aiofiles_open):
        with pytest.raises(Exception) as excinfo:
            await DataHandler.save_yaml(data, Path("dummy.yaml"))
        assert "Failed to save YAML to dummy.yaml: Write error" in str(excinfo.value)

@pytest.mark.asyncio
async def test_load_pickle_success(mock_aiofiles_open):
    mock_data = {'key': 'value'}
    pickled_data = pickle.dumps(mock_data)
    
    mock_file = mock_aiofiles_open.return_value.mock_file
    mock_file.read.return_value = pickled_data

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("asyncio.to_thread", return_value=mock_data) as mock_to_thread:
            result = await DataHandler.load_pickle(Path("dummy.pickle"))
            mock_to_thread.assert_called_once_with(pickle.loads, pickled_data)
            assert result == mock_data

@pytest.mark.asyncio
async def test_load_pickle_failure(mock_aiofiles_open):
    mock_aiofiles_open.side_effect = Exception("Pickle load error")

    with patch("aiofiles.open", mock_aiofiles_open):
        with pytest.raises(Exception) as excinfo:
            await DataHandler.load_pickle(Path("dummy.pickle"))
        assert "Failed to load pickle from dummy.pickle: Pickle load error" in str(excinfo.value)

@pytest.mark.asyncio
async def test_save_pickle_success(mock_aiofiles_open):
    data = {'key': 'value'}
    pickled_data = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
    
    mock_file = mock_aiofiles_open.return_value.mock_file

    with patch("aiofiles.open", mock_aiofiles_open):
        with patch("asyncio.to_thread", return_value=pickled_data) as mock_to_thread:
            await DataHandler.save_pickle(data, Path("dummy.pickle"))
            mock_to_thread.assert_called_once_with(pickle.dumps, data, protocol=pickle.HIGHEST_PROTOCOL)
            mock_file.write.assert_awaited_once_with(pickled_data)

@pytest.mark.asyncio
async def test_save_pickle_failure(mock_aiofiles_open):
    data = {'key': 'value'}
    mock_aiofiles_open.side_effect = Exception("Pickle write error")

    with patch("aiofiles.open", mock_aiofiles_open):
        with pytest.raises(Exception) as excinfo:
            await DataHandler.save_pickle(data, Path("dummy.pickle"))
        assert "Failed to save pickle to dummy.pickle: Pickle write error" in str(excinfo.value)