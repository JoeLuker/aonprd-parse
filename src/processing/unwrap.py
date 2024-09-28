# src/processing/unwrap.py

import asyncio
import copy
from pathlib import Path
from typing import Dict, Any, Set, List, Tuple
from collections import defaultdict

import networkx as nx
from tqdm.asyncio import tqdm

from config.config import config
from src.utils.logging import Logger
from src.utils.data_handling import DataHandler
from src.utils.file_operations import FileOperations

# Initialize Logger
logger = Logger.get_logger('UnwrapLogger', config.paths.log_dir / config.logging.unwrap_log)

class Unwrapper:
    """
    Handles unwrapping and validating graph data.
    """
    def __init__(self, graph: nx.DiGraph, data: Dict[str, Any], structure: Dict[str, Any]):
        self.graph = graph
        self.data = data
        self.structure = structure
        self.unwrapped_graph = None
        self.unwrapped_data = {}
        self.unwrapped_structure = {}

    def find_nodes_with_attributes(self, target_attributes: List[Dict[str, Any]]) -> Dict[Tuple, List[str]]:
        """Finds nodes in the graph that match the target attributes."""
        matching_nodes = defaultdict(list)
        for node, node_data in self.graph.nodes(data=True):
            if node_data.get('type') == 'tag' and node_data.get('name') == 'div':
                attributes_id = node_data.get('attributes_id')
                if attributes_id:
                    node_attributes = self.data['attributes'].get(attributes_id, {})
                    for target in target_attributes:
                        if all(
                            node_attributes.get(k) == v if not isinstance(v, list) else set(node_attributes.get(k, [])) == set(v)
                            for k, v in target.items()
                        ):
                            key = tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in target.items()))
                            matching_nodes[key].append(node)
                            logger.debug(f"Node {node} matched target attributes: {dict(target)}")
                            break

        logger.info(f"Identified {sum(len(nodes) for nodes in matching_nodes.values())} nodes matching target attributes.")
        return dict(matching_nodes)

    def rewire_graph(self, nodes_to_remove: Set[str]):
        """Rewires the graph by removing specified nodes and connecting their predecessors to successors."""
        for node in nodes_to_remove:
            if node not in self.unwrapped_graph:
                logger.warning(f"Node {node} not found in graph during rewiring. Skipping.")
                continue
            predecessors = list(self.unwrapped_graph.predecessors(node))
            successors = list(self.unwrapped_graph.successors(node))
            
            for pred in predecessors:
                for succ in successors:
                    edge_data = {**self.unwrapped_graph.edges.get((pred, node), {}), 
                                 **self.unwrapped_graph.edges.get((node, succ), {}), 
                                 'rewired': True}
                    self.unwrapped_graph.add_edge(pred, succ, **edge_data)
            
            self.unwrapped_graph.remove_node(node)
            logger.debug(f"Removed node {node} and rewired its connections.")

    async def unwrap_matching_nodes(self, target_attributes: List[Dict[str, Any]]):
        """Unwrap nodes matching specific criteria."""
        logger.info("Starting the unwrapping process.")
        
        matching_nodes = self.find_nodes_with_attributes(target_attributes)
        nodes_to_remove = set(node for nodes in matching_nodes.values() for node in nodes)

        logger.info(f"Total nodes to unwrap: {len(nodes_to_remove)}")

        # Create a copy of the graph to modify
        self.unwrapped_graph = self.graph.copy()

        # Rewire the graph
        self.rewire_graph(nodes_to_remove)

        # Process data
        self.unwrapped_data = copy.deepcopy(self.data)
        attributes_to_remove = set()
        for node in nodes_to_remove:
            if node in self.graph.nodes:
                node_data = self.graph.nodes[node]
                if 'attributes_id' in node_data:
                    attributes_to_remove.add(node_data['attributes_id'])
            else:
                logger.warning(f"Node {node} was not found in the original graph. It may have been removed during rewiring.")

        self.unwrapped_data['attributes'] = {k: v for k, v in self.unwrapped_data['attributes'].items() if k not in attributes_to_remove}
        removed_attributes = len(self.data['attributes']) - len(self.unwrapped_data['attributes'])
        logger.info(f"Removed {removed_attributes} attributes.")

        # Create unwrapped_structure
        self.unwrapped_structure = {
            'nodes': [{**data, 'id': node} for node, data in self.unwrapped_graph.nodes(data=True)],
            'edges': [{**data, 'source': u, 'target': v} for u, v, data in self.unwrapped_graph.edges(data=True)]
        }

        logger.info("Unwrapping process completed.")

    def validate_graph(self):
        """Validate the integrity of the unwrapped graph."""
        try:
            if nx.is_directed_acyclic_graph(self.unwrapped_graph):
                logger.info("Unwrapped graph is a Directed Acyclic Graph (DAG).")
            else:
                logger.warning("Unwrapped graph contains cycles.")

            if not nx.is_weakly_connected(self.unwrapped_graph):
                num_components = nx.number_weakly_connected_components(self.unwrapped_graph)
                logger.warning(f"Unwrapped graph has {num_components} weakly connected components.")
            
            orphaned_nodes = [node for node in self.unwrapped_graph.nodes() if self.unwrapped_graph.degree(node) == 0]
            if orphaned_nodes:
                logger.warning(f"Found {len(orphaned_nodes)} orphaned nodes in the unwrapped graph.")

        except Exception as e:
            logger.error(f"Error validating graph: {e}", exc_info=True)

    def save_results(self, output_dir: Path):
        """Save the unwrapped data and structure."""
        DataHandler.save_yaml(self.unwrapped_data, output_dir / config.files.filtered_data_yaml)
        DataHandler.save_yaml(self.unwrapped_structure, output_dir / config.files.filtered_structure_yaml)
        DataHandler.save_pickle(self.unwrapped_data, output_dir / config.files.filtered_data_pickle)
        DataHandler.save_pickle(self.unwrapped_structure, output_dir / config.files.filtered_structure_pickle)
        logger.info("Unwrapped data and structure saved successfully.")

async def main():
    # Define input and output directories
    input_dir = config.paths.condensed_output_dir
    output_dir = config.paths.processing_output_dir

    # Ensure output directory exists
    FileOperations.ensure_directory(output_dir)

    # Load condensed data and structure
    try:
        data = DataHandler.load_pickle(input_dir / config.files.filtered_data_pickle)
        structure = DataHandler.load_pickle(input_dir / config.files.filtered_structure_pickle)
        logger.info("Loaded condensed data and structure.")
    except FileNotFoundError:
        logger.error("Condensed data or structure not found.")
        return

    # Build graph
    graph = nx.DiGraph()
    for node in structure.get('nodes', []):
        graph.add_node(node['id'], **node)
    for edge in structure.get('edges', []):
        graph.add_edge(edge['source'], edge['target'], **edge)
    logger.info(f"Built graph with {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges.")

    # Initialize unwrapper
    unwrapper = Unwrapper(graph, data, structure)

    # Define target attributes for unwrapping
    target_attributes = [
        {'class': ['page', 'clearfix'], 'id': 'page'},
        {'class': ['main-wrapper'], 'id': 'main-wrapper'},
        {'class': ['main'], 'id': 'main'},
        {'class': ['clearfix'], 'id': 'wrapper'}
    ]

    # Perform unwrapping
    await unwrapper.unwrap_matching_nodes(target_attributes)

    # Validate graph
    unwrapper.validate_graph()

    # Save results
    unwrapper.save_results(output_dir)

    logger.info("Unwrap Process Completed Successfully.")

if __name__ == "__main__":
    asyncio.run(main())