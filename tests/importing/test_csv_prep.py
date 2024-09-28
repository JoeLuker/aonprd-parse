# tests/importing/test_csv_prep.py

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from src.importing.csv_prep import CSVExporter, CSVPrep


@pytest.fixture
def mock_csv_exporter():
    with patch("src.importing.csv_prep.csv.writer") as mock_csv_writer:
        exporter = CSVExporter(Path("dummy_dir"))
        # Mock the writers for different CSV types
        mock_writer_document = MagicMock()
        mock_writer_attribute = MagicMock()
        mock_writer_relationship = MagicMock()
        
        # Assuming CSVExporter initializes writers for 'Document', 'Attribute', and relationships
        mock_csv_writer.side_effect = [mock_writer_document, mock_writer_attribute, mock_writer_relationship]
        exporter.files = {
            'Document': {'writer': mock_writer_document},
            'Attribute': {'writer': mock_writer_attribute},
            'HAS_DOCTYPE': {'writer': mock_writer_relationship},
            'HAS_COMMENT': {'writer': mock_writer_relationship},
            'CONTAINS_TEXT': {'writer': mock_writer_relationship},
            'CONTAINS_TAG': {'writer': mock_writer_relationship},
            'HAS_ROOT': {'writer': mock_writer_relationship},
            'HAS_ATTRIBUTE': {'writer': mock_writer_relationship}
        }
        yield exporter
        exporter.close()


def test_export_node(mock_csv_exporter):
    node_type = 'Document'
    node_id = 'n1'
    properties = {'filename': 'file1.html'}
    
    mock_csv_exporter.files['Document']['writer'].writerow = MagicMock()
    
    CSVExporter.export_node(node_type, node_id, properties)
    mock_csv_exporter.files['Document']['writer'].writerow.assert_called_once_with(['n1', 'file1.html'])


def test_export_attribute(mock_csv_exporter):
    attribute_id = 'a1'
    attributes = {'class': 'test', 'id': 'test-id'}
    
    mock_csv_exporter.files['Attribute']['writer'].writerow = MagicMock()
    
    CSVExporter.export_attribute(attribute_id, attributes)
    mock_csv_exporter.files['Attribute']['writer'].writerow.assert_called_once_with(['a1', '{"class": "test", "id": "test-id"}'])


def test_export_relationship_with_order(mock_csv_exporter):
    rel_type = 'HAS_DOCTYPE'
    source = 'n1'
    target = 'd1'
    source_type = 'Document'
    target_type = 'Doctype'
    order = 1
    
    mock_csv_exporter.files['HAS_DOCTYPE']['writer'].writerow = MagicMock()
    
    CSVExporter.export_relationship(rel_type, source, target, source_type, target_type, order)
    mock_csv_exporter.files['HAS_DOCTYPE']['writer'].writerow.assert_called_once_with(['n1', 'd1', 'Document', 'Doctype', 1])


def test_export_relationship_without_order(mock_csv_exporter):
    rel_type = 'HAS_ATTRIBUTE'
    source = 'n1'
    target = 'a1'
    source_type = 'Tag'
    target_type = 'Attribute'
    
    mock_csv_exporter.files['HAS_ATTRIBUTE']['writer'].writerow = MagicMock()
    
    CSVExporter.export_relationship(rel_type, source, target, source_type, target_type)
    mock_csv_exporter.files['HAS_ATTRIBUTE']['writer'].writerow.assert_called_once_with(['n1', 'a1', 'Tag', 'Attribute'])


def test_csv_prep_run(mock_csv_exporter):
    structure = {
        'nodes': [
            {'id': 'n1', 'type': 'document', 'filename': 'file1.html'},
            {'id': 'd1', 'type': 'doctype', 'data_id': 'do1'},
            {'id': 'c1', 'type': 'comment', 'data_id': 'cmt1'},
            {'id': 't1', 'type': 'textnode', 'data_id': 'txt1'},
            {'id': 'tag1', 'type': 'tag', 'name': 'p', 'attributes_id': 'a1'}
        ],
        'edges': [
            {'source': 'n1', 'target': 'd1', 'relationship': 'HAS_DOCTYPE', 'order': 1},
            {'source': 'n1', 'target': 'c1', 'relationship': 'HAS_COMMENT', 'order': 2},
            {'source': 'tag1', 'target': 't1', 'relationship': 'CONTAINS_TEXT', 'order': 1},
            {'source': 'n1', 'target': 'tag1', 'relationship': 'CONTAINS_TAG', 'order': 3},
            {'source': 'n1', 'target': 'tag1', 'relationship': 'HAS_ROOT', 'order': 4},
            {'source': 'tag1', 'target': 'a1', 'relationship': 'HAS_ATTRIBUTE'}
        ]
    }
    data = {
        'doctypes': {'do1': '<!DOCTYPE html>'},
        'comments': {'cmt1': 'This is a comment.'},
        'texts': {'txt1': 'This is some text.'},
        'attributes': {'a1': {'class': 'text'}}
    }
    
    exporter = CSVPrep(structure, data, mock_csv_exporter)
    
    with patch.object(CSVPrep, 'prepare_nodes') as mock_prepare_nodes, \
         patch.object(CSVPrep, 'prepare_attributes') as mock_prepare_attributes, \
         patch.object(CSVPrep, 'prepare_relationships') as mock_prepare_relationships:
        exporter.run()
        mock_prepare_nodes.assert_called_once()
        mock_prepare_attributes.assert_called_once()
        mock_prepare_relationships.assert_called_once()
