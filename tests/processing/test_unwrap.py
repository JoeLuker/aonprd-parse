# tests/processing/test_unwrap.py

import pytest
from unittest.mock import patch, AsyncMock
from pathlib import Path
import networkx as nx

from src.processing.unwrap import Unwrapper
from config.config import config


@pytest.fixture
def mock_graph():
    G = nx.DiGraph()
    G.add_node('n1', type='document')
    G.add_node('n2', type='tag')
    G.add_node('n3', type='textnode')
    G.add_edge('n1', 'n2')
    G.add_edge('n2', 'n3')
    return G


@pytest.fixture
def mock_data():
    return {
        'doctypes': {},
        'comments': {},
        'texts': {'n3': 'Sample text.'},
        'attributes': {}
    }


@pytest.fixture
def mock_structure():
    return {
        'nodes': [
            {'id': 'n1', 'type': 'document', 'filename': 'file1.html'},
            {'id': 'n2', 'type': 'tag', 'name': 'p', 'attributes_id': 'a1'},
            {'id': 'n3', 'type': 'textnode', 'data_id': 'txt1'}
        ],
        'edges': [
            {'source': 'n1', 'target': 'n2', 'relationship': 'CONTAINS_TAG', 'order': 1},
            {'source': 'n2', 'target': 'n3', 'relationship': 'CONTAINS_TEXT', 'order': 1}
        ]
    }


@pytest.fixture
def unwrapper(mock_graph, mock_data, mock_structure):
    return Unwrapper(mock_graph, mock_data, mock_structure)


def test_validate_graph(unwrapper):
    with patch("networkx.is_directed_acyclic_graph", return_value=True) as mock_validate:
        unwrapper.validate_graph()
        mock_validate.assert_called_once_with(unwrapper.graph)


def test_validate_graph_with_cycles(unwrapper):
    unwrapper.graph.add_edge('n3', 'n1')  # Introduce a cycle
    with patch("networkx.is_directed_acyclic_graph", return_value=False) as mock_validate:
        unwrapper.validate_graph()
        mock_validate.assert_called_once_with(unwrapper.graph)


@pytest.mark.asyncio
async def test_unwrap_matching_nodes(unwrapper):
    target_attributes = [{'type': 'tag', 'name': 'p'}]
    with patch.object(unwrapper, 'find_nodes_with_attributes', return_value={'key': ['n2']}), \
         patch.object(unwrapper, 'rewire_graph'), \
         patch.object(unwrapper, 'validate_graph'):
        await unwrapper.unwrap_matching_nodes(target_attributes)
        unwrapper.find_nodes_with_attributes.assert_called_once_with(target_attributes)
        unwrapper.rewire_graph.assert_called_once()
        unwrapper.validate_graph.assert_called_once()


@pytest.mark.asyncio
async def test_save_results(unwrapper, tmp_path):
    with patch("src.utils.data_handling.DataHandler.save_yaml", new_callable=AsyncMock) as mock_save_yaml, \
         patch("src.utils.data_handling.DataHandler.save_pickle", new_callable=AsyncMock) as mock_save_pickle, \
         patch("src.processing.unwrap.logger.info") as mock_logger_info:

        output_dir = Path("output_dir")
        unwrapper.unwrapped_data = {'key': 'value'}
        unwrapper.unwrapped_structure = {'nodes': [], 'edges': []}

        await unwrapper.save_results(output_dir)

        assert mock_save_yaml.await_count == 2
        assert mock_save_pickle.await_count == 2
        
        mock_save_yaml.assert_any_await(unwrapper.unwrapped_data, output_dir / config.files.filtered_data_yaml)
        mock_save_yaml.assert_any_await(unwrapper.unwrapped_structure, output_dir / config.files.filtered_structure_yaml)
        mock_save_pickle.assert_any_await(unwrapper.unwrapped_data, output_dir / config.files.filtered_data_pickle)
        mock_save_pickle.assert_any_await(unwrapper.unwrapped_structure, output_dir / config.files.filtered_structure_pickle)

        mock_logger_info.assert_called_once_with("Unwrapped data and structure saved successfully.")