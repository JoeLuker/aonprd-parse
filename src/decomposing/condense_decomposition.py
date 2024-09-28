# src/decomposing/condense_decomposition.py

import asyncio
from pathlib import Path
from typing import Dict, Any, List, Set
from collections import Counter, defaultdict


from config.config import config
from src.utils.logging import Logger
from src.utils.data_handling import DataHandler
from src.utils.file_operations import FileOperations

# Initialize Logger
logger = Logger.get_logger(
    "CondenseDecompositionLogger", config.paths.log_dir / config.logging.processor_log
)


class Condenser:
    """
    Condenses and filters decomposed data based on specific criteria.
    """

    def __init__(self, data: Dict[str, Any], structure: Dict[str, Any]):
        self.data = data
        self.structure = structure
        self.filtered_data = {}
        self.filtered_structure = {}

    def analyze_edges(self) -> Set[str]:
        """Identify bad targets based on edge analysis."""
        edge_counter = {"source": Counter(), "target": Counter()}
        for edge in self.structure.get("edges", []):
            edge_counter["source"][edge["source"]] += 1
            edge_counter["target"][edge["target"]] += 1

        bad_targets = {
            target for target, count in edge_counter["target"].items() if count > 1000
        }

        good_nodes = {
            node["id"]
            for node in self.structure.get("nodes", [])
            if node.get("name")
            in ["title", "head", "body", "html", "text", "b", "i", "form"]
            or node.get("type") in ["document"]
        }
        bad_nodes = {
            node["id"]
            for node in self.structure.get("nodes", [])
            if node.get("name") in ["meta", "input", "script", "select", "option"]
        }

        bad_targets = (bad_targets | bad_nodes) - good_nodes

        logger.debug(f"Identified {len(bad_targets)} bad targets.")
        return bad_targets

    def find_connected_nodes(self, filtered_edges: List[Dict[str, Any]]) -> Set[str]:
        """Find nodes connected to documents."""
        node_dict = {node["id"]: node for node in self.structure.get("nodes", [])}
        edge_dict = defaultdict(list)
        for edge in filtered_edges:
            edge_dict[edge["target"]].append(edge["source"])

        def recursive_lookback(target_id: str, visited: Set[str] = None) -> bool:
            if visited is None:
                visited = set()
            if target_id in visited:
                return False
            visited.add(target_id)

            node = node_dict.get(target_id)
            if node and node.get("type") == "document":
                return True

            for source_id in edge_dict.get(target_id, []):
                if recursive_lookback(source_id, visited):
                    return True

            return False

        connected_nodes = set()
        for node_id in node_dict:
            if recursive_lookback(node_id):
                connected_nodes.add(node_id)

        logger.debug(f"Found {len(connected_nodes)} connected nodes.")
        return connected_nodes

    def filter_structure(self, bad_targets: Set[str]) -> None:
        """Filter the structure based on bad targets."""
        filtered_edges = [
            edge
            for edge in self.structure.get("edges", [])
            if edge["target"] not in bad_targets
        ]
        connected_nodes = self.find_connected_nodes(filtered_edges)

        self.filtered_structure = {
            "nodes": [
                node
                for node in self.structure.get("nodes", [])
                if node["id"] in connected_nodes
            ],
            "edges": [
                edge
                for edge in filtered_edges
                if edge["source"] in connected_nodes
                and edge["target"] in connected_nodes
            ],
        }
        logger.debug(
            f"Filtered structure has {len(self.filtered_structure['nodes'])} nodes and {len(self.filtered_structure['edges'])} edges."
        )

    def filter_data(self) -> None:
        """Filter the data based on filtered structure."""
        good_attributes = {
            node["attributes_id"]
            for node in self.filtered_structure.get("nodes", [])
            if "attributes_id" in node
        }
        good_texts = {
            node["data_id"]
            for node in self.filtered_structure.get("nodes", [])
            if "data_id" in node
        }

        self.filtered_data = {
            "attributes": {
                k: v
                for k, v in self.data.get("attributes", {}).items()
                if k in good_attributes
            },
            "texts": {
                k: v for k, v in self.data.get("texts", {}).items() if k in good_texts
            },
        }
        logger.debug(
            f"Filtered data has {len(self.filtered_data['attributes'])} attributes and {len(self.filtered_data['texts'])} texts."
        )

    async def run(self):
        """Execute the condensation process."""
        bad_targets = self.analyze_edges()
        self.filter_structure(bad_targets)
        self.filter_data()

    def save_results(self, output_dir: Path):
        """Save the condensed data and structure."""
        DataHandler.save_yaml(
            self.filtered_data, output_dir / config.files.filtered_data_yaml
        )
        DataHandler.save_yaml(
            self.filtered_structure, output_dir / config.files.filtered_structure_yaml
        )
        DataHandler.save_pickle(
            self.filtered_data, output_dir / config.files.filtered_data_pickle
        )
        DataHandler.save_pickle(
            self.filtered_structure, output_dir / config.files.filtered_structure_pickle
        )
        logger.info("Condensed data and structure saved successfully.")


async def main():
    # Define input and output directories
    input_dir = config.paths.decomposed_output_dir
    output_dir = config.paths.condensed_output_dir

    # Ensure output directory exists
    FileOperations.ensure_directory(output_dir)

    # Load decomposed data and structure
    try:
        data = DataHandler.load_pickle(input_dir / config.files.data_pickle)
        structure = DataHandler.load_pickle(input_dir / config.files.structure_pickle)
        logger.info("Loaded decomposed data and structure.")
    except FileNotFoundError:
        logger.error("Decomposed data or structure not found.")
        return

    condenser = Condenser(data, structure)
    await condenser.run()
    condenser.save_results(output_dir)

    # Print file size information
    original_structure_size = (input_dir / config.files.structure_yaml).stat().st_size
    original_data_size = (input_dir / config.files.data_yaml).stat().st_size
    filtered_structure_size = (
        (output_dir / config.files.filtered_structure_yaml).stat().st_size
    )
    filtered_data_size = (output_dir / config.files.filtered_data_yaml).stat().st_size

    logger.info("\nFile sizes:")
    logger.info(f"Original structure: {original_structure_size:,} bytes")
    logger.info(f"Original data: {original_data_size:,} bytes")
    logger.info(f"Filtered structure: {filtered_structure_size:,} bytes")
    logger.info(f"Filtered data: {filtered_data_size:,} bytes")

    total_original = original_structure_size + original_data_size
    total_filtered = filtered_structure_size + filtered_data_size
    reduction_percentage = (
        ((total_original - total_filtered) / total_original) * 100
        if total_original > 0
        else 0
    )

    logger.info(f"\nTotal size reduction: {reduction_percentage:.2f}%")


if __name__ == "__main__":
    asyncio.run(main())
