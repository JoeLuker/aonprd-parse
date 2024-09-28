# tests/utils/test_data_handling.py

import pytest
from unittest.mock import mock_open, patch, MagicMock
from pathlib import Path
import pickle
import yaml

from src.utils.data_handling import DataHandler


@pytest.mark.asyncio
async def test_load_yaml_success():
    mock_data = {'key': 'value'}
    yaml_content = yaml.dump(mock_data)
    
    with patch("src.utils.data_handling.open", mock_open(read_data=yaml_content)):
        with patch("yaml.load", return_value=mock_data) as mock_yaml_load:
            result = await DataHandler.load_yaml(Path("dummy.yaml"))
            mock_yaml_load.assert_called_once()
            assert result == mock_data


@pytest.mark.asyncio
async def test_load_yaml_failure():
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("File not found")
        with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
            mock_logger = MagicMock()
            mock_logger_get.return_value = mock_logger
            with pytest.raises(Exception) as excinfo:
                await DataHandler.load_yaml(Path("dummy.yaml"))
            assert "Failed to load YAML" in str(excinfo.value)
            mock_logger.error.assert_called_once_with("Failed to load YAML: File not found")


@pytest.mark.asyncio
async def test_save_yaml_success():
    data = {'key': 'value'}
    yaml_dump_content = yaml.dump(data, Dumper=yaml.CDumper, default_flow_style=False, allow_unicode=True, sort_keys=False)
    
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        with patch("yaml.dump") as mock_yaml_dump:
            await DataHandler.save_yaml(data, Path("dummy.yaml"))
            mock_yaml_dump.assert_called_once_with(data, mock_file(), Dumper=yaml.CDumper, default_flow_style=False, allow_unicode=True, sort_keys=False)
            mock_file.assert_called_once_with(Path("dummy.yaml"), 'w')


@pytest.mark.asyncio
async def test_save_yaml_failure():
    data = {'key': 'value'}
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Write error")
        with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
            mock_logger = MagicMock()
            mock_logger_get.return_value = mock_logger
            with pytest.raises(Exception) as excinfo:
                await DataHandler.save_yaml(data, Path("dummy.yaml"))
            assert "Failed to save YAML" in str(excinfo.value)
            mock_logger.error.assert_called_once_with("Failed to save YAML: Write error")


@pytest.mark.asyncio
async def test_load_pickle_success():
    mock_data = {'key': 'value'}
    pickled_data = pickle.dumps(mock_data)
    
    with patch("src.utils.data_handling.open", mock_open(read_data=pickled_data)):
        with patch("pickle.load", return_value=mock_data) as mock_pickle_load:
            result = await DataHandler.load_pickle(Path("dummy.pickle"))
            mock_pickle_load.assert_called_once()
            assert result == mock_data


@pytest.mark.asyncio
async def test_load_pickle_failure():
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Pickle load error")
        with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
            mock_logger = MagicMock()
            mock_logger_get.return_value = mock_logger
            with pytest.raises(Exception) as excinfo:
                await DataHandler.load_pickle(Path("dummy.pickle"))
            assert "Failed to load pickle" in str(excinfo.value)
            mock_logger.error.assert_called_once_with("Failed to load pickle: Pickle load error")


@pytest.mark.asyncio
async def test_save_pickle_success():
    data = {'key': 'value'}
    
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        with patch("pickle.dump") as mock_pickle_dump:
            await DataHandler.save_pickle(data, Path("dummy.pickle"))
            mock_pickle_dump.assert_called_once_with(data, mock_file(), protocol=pickle.HIGHEST_PROTOCOL)
            mock_file.assert_called_once_with(Path("dummy.pickle"), 'wb')


@pytest.mark.asyncio
async def test_save_pickle_failure():
    data = {'key': 'value'}
    with patch("src.utils.data_handling.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Pickle write error")
        with patch("src.utils.logging.Logger.get_logger") as mock_logger_get:
            mock_logger = MagicMock()
            mock_logger_get.return_value = mock_logger
            with pytest.raises(Exception) as excinfo:
                await DataHandler.save_pickle(data, Path("dummy.pickle"))
            assert "Failed to save pickle" in str(excinfo.value)
            mock_logger.error.assert_called_once_with("Failed to save pickle: Pickle write error")
