# tests/decomposing/test_decomposer.py

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import asyncio
from bs4 import BeautifulSoup

from src.decomposing.decomposer import Decomposer
from config.config import config

@pytest.fixture
def decomposer():
    return Decomposer()

@pytest.mark.asyncio
async def test_process_file_success(decomposer):
    file_path = Path("test.html")
    content = "<html><body><p>Test</p></body></html>"
    
    with patch("src.utils.file_operations.FileOperations.read_file_async", return_value=content) as mock_read:
        with patch.object(decomposer, '_process_element') as mock_process_element:
            await decomposer.process_file(file_path)
            mock_read.assert_awaited_once_with(file_path)
            mock_process_element.assert_awaited_once()
            assert len(decomposer.structure['nodes']) == 1
            assert decomposer.structure['nodes'][0]['type'] == 'document'
            assert decomposer.structure['nodes'][0]['filename'] == 'test.html'

@pytest.mark.asyncio
async def test_process_element(decomposer):
    soup = BeautifulSoup("<p>Test</p>", 'html.parser')
    parent_id = "n1"
    
    await decomposer._process_element(soup.p, parent_id)
    
    assert len(decomposer.structure['nodes']) == 2  # parent_id node and new 'p' node
    assert len(decomposer.structure['edges']) == 1
    assert decomposer.structure['edges'][0]['relationship'] == 'CONTAINS_TAG'
    assert len(decomposer.data['texts']) == 1

@pytest.mark.asyncio
async def test_run_no_files(decomposer):
    input_dir = Path("empty_dir")
    
    with patch("pathlib.Path.iterdir", return_value=[]):
        await decomposer.run(input_dir)
        assert len(decomposer.structure['nodes']) == 0
        assert len(decomposer.structure['edges']) == 0

@pytest.mark.asyncio
async def test_run_with_files(decomposer):
    input_dir = Path("html_files")
    files = [Path("file1.html"), Path("file2.html")]
    
    with patch("pathlib.Path.iterdir", return_value=files):
        with patch("src.utils.file_operations.FileOperations.read_file_async", side_effect=["<html><p>Test1</p></html>", "<html><p>Test2</p></html>"]) as mock_read:
            with patch.object(decomposer, 'process_file', side_effect=asyncio.coroutine(lambda x: None)) as mock_process:
                await decomposer.run(input_dir)
                assert mock_process.call_count == 2
                mock_read.assert_has_awaits([patch.call(file) for file in files], any_order=True)

@pytest.mark.asyncio
async def test_save_results(decomposer, tmp_path):
    decomposer.data = {'texts': {'t1': 'Test'}}
    decomposer.structure = {'nodes': [{'id': 'n1', 'type': 'document'}], 'edges': []}
    
    with patch("src.utils.data_handling.DataHandler.save_yaml") as mock_save_yaml:
        with patch("src.utils.data_handling.DataHandler.save_pickle") as mock_save_pickle:
            decomposer.save_results(tmp_path)
            
            assert mock_save_yaml.call_count == 2
            assert mock_save_pickle.call_count == 2

@pytest.mark.asyncio
async def test_create_node(decomposer):
    node_id = decomposer._create_node('tag', name='p', attributes_id='a1')
    assert len(decomposer.structure['nodes']) == 1
    assert decomposer.structure['nodes'][0]['type'] == 'tag'
    assert decomposer.structure['nodes'][0]['name'] == 'p'
    assert decomposer.structure['nodes'][0]['attributes_id'] == 'a1'

@pytest.mark.asyncio
async def test_create_edge(decomposer):
    decomposer._create_edge('n1', 'n2', 'CONTAINS_TAG', 1)
    assert len(decomposer.structure['edges']) == 1
    assert decomposer.structure['edges'][0]['source'] == 'n1'
    assert decomposer.structure['edges'][0]['target'] == 'n2'
    assert decomposer.structure['edges'][0]['relationship'] == 'CONTAINS_TAG'
    assert decomposer.structure['edges'][0]['order'] == 1