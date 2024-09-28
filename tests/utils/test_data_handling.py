# tests/utils/test_data_handling.py

import pytest
from unittest.mock import mock_open, patch
from pathlib import Path
import pickle
import yaml

from src.utils.data_handling import DataHandler

def test_load_yaml_success():
    mock_data = {'key': 'value'}
    with patch("builtins.open", mock_open(read_data=yaml.dump(mock_data))):
        with patch("yaml.load", return_value=mock_data) as mock_yaml_load:
            result = DataHandler.load_yaml(Path("dummy.yaml"))
            mock_yaml_load.assert_called_once()
            assert result == mock_data

def test_load_yaml_failure():
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("File not found")
        with pytest.raises(Exception) as excinfo:
            DataHandler.load_yaml(Path("dummy.yaml"))
        assert "Failed to load YAML" in str(excinfo.value)

def test_save_yaml_success():
    data = {'key': 'value'}
    with patch("builtins.open", mock_open()) as mock_file:
        with patch("yaml.dump") as mock_yaml_dump:
            DataHandler.save_yaml(data, Path("dummy.yaml"))
            mock_yaml_dump.assert_called_once_with(data, mock_file(), Dumper=yaml.CDumper, default_flow_style=False, allow_unicode=True, sort_keys=False)

def test_save_yaml_failure():
    data = {'key': 'value'}
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Write error")
        with pytest.raises(Exception) as excinfo:
            DataHandler.save_yaml(data, Path("dummy.yaml"))
        assert "Failed to save YAML" in str(excinfo.value)

def test_load_pickle_success():
    mock_data = {'key': 'value'}
    pickled_data = pickle.dumps(mock_data)
    with patch("builtins.open", mock_open(read_data=pickled_data)):
        with patch("pickle.load", return_value=mock_data) as mock_pickle_load:
            result = DataHandler.load_pickle(Path("dummy.pickle"))
            mock_pickle_load.assert_called_once()
            assert result == mock_data

def test_load_pickle_failure():
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Pickle load error")
        with pytest.raises(Exception) as excinfo:
            DataHandler.load_pickle(Path("dummy.pickle"))
        assert "Failed to load pickle" in str(excinfo.value)

def test_save_pickle_success():
    data = {'key': 'value'}
    with patch("builtins.open", mock_open()) as mock_file:
        with patch("pickle.dump") as mock_pickle_dump:
            DataHandler.save_pickle(data, Path("dummy.pickle"))
            mock_pickle_dump.assert_called_once_with(data, mock_file(), protocol=pickle.HIGHEST_PROTOCOL)

def test_save_pickle_failure():
    data = {'key': 'value'}
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = Exception("Pickle write error")
        with pytest.raises(Exception) as excinfo:
            DataHandler.save_pickle(data, Path("dummy.pickle"))
        assert "Failed to save pickle" in str(excinfo.value)
