# tests/decomposing/test_decomposer.py

import pytest
from unittest.mock import patch, AsyncMock, MagicMock, call
from pathlib import Path

from src.decomposing.decomposer import Decomposer


@pytest.fixture
def decomposer():
    return Decomposer()


@pytest.mark.asyncio
async def test_process_file_success(decomposer):
    file_path = Path("test.html")
    content = "<html><body><p>Test</p></body></html>"
    
    with patch("src.utils.file_operations.FileOperations.read_file_async", return_value=content) as mock_read:
        with patch.object(decomposer, '_process_element', new_callable=AsyncMock) as mock_process_element:
            await decomposer.process_file(file_path)
            mock_read.assert_awaited_once_with(file_path)
            mock_process_element.assert_awaited_once()
            assert len(decomposer.structure['nodes']) == 1
            assert decomposer.structure['nodes'][0]['type'] == 'document'
            assert decomposer.structure['nodes'][0]['filename'] == 'test.html'


@pytest.mark.asyncio
async def test_process_element(decomposer):
    from bs4 import BeautifulSoup  # Ensure BeautifulSoup is imported here
    
    soup = BeautifulSoup("<p>Test</p>", 'html.parser')
    parent_id = "n1"
    
    await decomposer._process_element(soup.p, parent_id)
    
    assert len(decomposer.structure['nodes']) == 2  # parent_id node and new 'p' node
    assert len(decomposer.structure['edges']) == 2
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
    
    # Create mocked Path objects with is_file() returning True
    mock_file1 = MagicMock(spec=Path)
    mock_file1.is_file.return_value = True
    mock_file1.suffix = ".html"
    mock_file1.name = "file1.html"
    
    mock_file2 = MagicMock(spec=Path)
    mock_file2.is_file.return_value = True
    mock_file2.suffix = ".html"
    mock_file2.name = "file2.html"
    
    files = [mock_file1, mock_file2]

    async def mock_read_file(file_path):
        return f"<html><body>Content of {file_path.name}</body></html>"

    with patch.object(Path, "iterdir", return_value=files):
        with patch("src.utils.file_operations.FileOperations.read_file_async", new_callable=AsyncMock, side_effect=mock_read_file) as mock_read:
            await decomposer.run(input_dir)
            
            # Verify that read_file_async was called twice
            assert mock_read.call_count == 2
            mock_read.assert_has_awaits([call(mock_file1), call(mock_file2)], any_order=True)
            
            # Verify that two document nodes were created
            document_nodes = [node for node in decomposer.structure['nodes'] if node['type'] == 'document']
            assert len(document_nodes) == 2, f"Expected 2 document nodes, found {len(document_nodes)}"
            
            # Optionally, verify other aspects of the nodes
            # For example, ensure that each document node has the correct filename
            filenames = {node['filename'] for node in document_nodes}
            assert filenames == {'file1.html', 'file2.html'}, f"Unexpected filenames: {filenames}"


@pytest.mark.asyncio
async def test_save_results(decomposer, tmp_path):
    decomposer.data = {'texts': {'t1': 'Test'}}
    decomposer.structure = {'nodes': [{'id': 'n1', 'type': 'document'}], 'edges': []}
    
    with patch("src.utils.data_handling.DataHandler.save_yaml", new_callable=AsyncMock) as mock_save_yaml, \
         patch("src.utils.data_handling.DataHandler.save_pickle", new_callable=AsyncMock) as mock_save_pickle:
        await decomposer.save_results(tmp_path)
        
        assert mock_save_yaml.call_count == 2
        assert mock_save_pickle.call_count == 2
        mock_save_yaml.assert_any_await(decomposer.data, tmp_path / 'data.yaml')
        mock_save_yaml.assert_any_await(decomposer.structure, tmp_path / 'structure.yaml')
        mock_save_pickle.assert_any_await(decomposer.data, tmp_path / 'data.pickle')
        mock_save_pickle.assert_any_await(decomposer.structure, tmp_path / 'structure.pickle')


@pytest.mark.asyncio
async def test_create_node(decomposer):
    node_id = decomposer._create_node("tag", name="p", attributes_id="a1")
    assert len(decomposer.structure['nodes']) == 1
    assert decomposer.structure['nodes'][0]['type'] == 'tag'
    assert decomposer.structure['nodes'][0]['name'] == 'p'
    assert decomposer.structure['nodes'][0]['attributes_id'] == 'a1'
    assert decomposer.structure['nodes'][0]['id'] == node_id


@pytest.mark.asyncio
async def test_create_edge(decomposer):
    decomposer._create_edge('n1', 'n2', 'CONTAINS_TAG', 1)
    assert len(decomposer.structure['edges']) == 1
    assert decomposer.structure['edges'][0]['source'] == 'n1'
    assert decomposer.structure['edges'][0]['target'] == 'n2'
    assert decomposer.structure['edges'][0]['relationship'] == 'CONTAINS_TAG'
    assert decomposer.structure['edges'][0]['order'] == 1
