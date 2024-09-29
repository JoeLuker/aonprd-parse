# src/decomposing/decomposer.py

import asyncio
import xxhash
from pathlib import Path
from typing import Dict, Any
from bs4 import BeautifulSoup, Tag, NavigableString, Comment, Doctype
from tqdm.asyncio import tqdm

# Import shared modules
from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations
from src.utils.data_handling import DataHandler

# Initialize Logger
logger = Logger.get_logger(
    "DecomposerLogger", config.paths.log_dir / "decomposer.log"
)


class Decomposer:
    """
    Handles the decomposition of HTML files into graph structures.
    """

    def __init__(self):
        self.node_id_counter = 1
        self.attribute_id_counter = 1
        self.processed_files = set()

    async def process(self):
        self.data = {
            'texts': {},
            'doctypes': {},
            'comments': {},
            'attributes': {}  # Initialize attributes
        }
        self.structure = {'nodes': [], 'edges': []}
        # Continue with decomposition
        input_dir = config.paths.manual_cleaned_html_data
        html_files = [
            f for f in input_dir.iterdir() if f.is_file() and f.suffix == ".html"
        ]
        logger.info(f"Found {len(html_files)} HTML files to process.")

        max_files = getattr(config.processing, 'max_files', float('inf'))
        files_to_process = html_files[:max_files]
        
        if files_to_process:
            tasks = [self.process_file(file) for file in files_to_process]
            await tqdm.gather(*tasks, desc="Processing HTML files", unit="file")
        else:
            logger.warning("No HTML files found to process.")

    async def process_file(self, file_path: Path):
        """Asynchronously process a single HTML file."""
        file_hash = self._hash_file(file_path)
        if file_hash in self.processed_files:
            logger.info(f"File {file_path.name} already processed. Skipping.")
            return

        try:
            content = await FileOperations.read_file_async(file_path)
            soup = BeautifulSoup(content, "html.parser")
            document_node_id = self._create_node("document", filename=file_path.name)
            await self._process_element(soup, document_node_id)
            self.processed_files.add(file_hash)
            logger.verbose(f"Processed file: {file_path.name}")
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {e}", exc_info=True)

    async def _process_element(self, element: Any, parent_id: str, order: int = 0):
        if isinstance(element, Doctype):
            doctype_id = self._create_doctype(str(element))
            node_id = self._create_node("doctype", data_id=doctype_id)
            self._create_edge(parent_id, node_id, "HAS_DOCTYPE", order)
        elif isinstance(element, Comment):
            comment_id = self._create_comment(str(element))
            node_id = self._create_node("comment", data_id=comment_id)
            self._create_edge(parent_id, node_id, "HAS_COMMENT", order)
        elif isinstance(element, NavigableString):
            if element.strip():
                text_id = self._create_text(str(element))
                node_id = self._create_node("textnode", data_id=text_id)
                self._create_edge(parent_id, node_id, "CONTAINS_TEXT", order)
        elif isinstance(element, Tag):
            attributes_id = (
                self._create_attribute(element.attrs) if element.attrs else None
            )
            node_id = self._create_node(
                "tag", name=element.name, attributes_id=attributes_id
            )
            self._create_edge(parent_id, node_id, "CONTAINS_TAG", order)

            for child_order, child in enumerate(element.children, start=1):
                await self._process_element(child, node_id, child_order)

    def _create_node(self, node_type: str, **kwargs) -> str:
        # Check if node already exists
        for node in self.structure["nodes"]:
            if node["type"] == node_type and all(node.get(k) == v for k, v in kwargs.items()):
                return node["id"]

        node_id = f"n{self.node_id_counter}"
        self.node_id_counter += 1
        node = {"id": node_id, "type": node_type, **kwargs}
        self.structure["nodes"].append(node)
        return node_id

    def _create_edge(
        self, source_id: str, target_id: str, relationship: str, order: int
    ):
        # Check if edge already exists
        for edge in self.structure["edges"]:
            if (
                edge["source"] == source_id
                and edge["target"] == target_id
                and edge["relationship"] == relationship
                and edge["order"] == order
            ):
                return

        edge = {
            "source": source_id,
            "target": target_id,
            "relationship": relationship,
            "order": order,
        }
        self.structure["edges"].append(edge)

    def _create_text(self, content: str) -> str:
        # Check if text already exists
        text_hash = self._hash_content(content)
        if text_hash in self.data["texts"]:
            return self.data["texts"][text_hash]

        text_id = f"t{len(self.data['texts']) + 1}"
        self.data["texts"][text_hash] = text_id
        return text_id

    def _create_doctype(self, content: str) -> str:
        # Check if doctype already exists
        doctype_hash = self._hash_content(content)
        if doctype_hash in self.data["doctypes"]:
            return self.data["doctypes"][doctype_hash]

        doctype_id = f"d{len(self.data['doctypes']) + 1}"
        self.data["doctypes"][doctype_hash] = doctype_id
        return doctype_id

    def _create_comment(self, content: str) -> str:
        # Check if comment already exists
        comment_hash = self._hash_content(content)
        if comment_hash in self.data["comments"]:
            return self.data["comments"][comment_hash]

        comment_id = f"c{len(self.data['comments']) + 1}"
        self.data["comments"][comment_hash] = comment_id
        return comment_id

    def _create_attribute(self, attributes: Dict[str, Any]) -> str:
        # Check if attributes already exist
        attributes_hash = self._hash_content(str(attributes))
        if attributes_hash in self.data["attributes"]:
            return self.data["attributes"][attributes_hash]

        attribute_id = f"a{self.attribute_id_counter}"
        self.attribute_id_counter += 1
        self.data["attributes"][attributes_hash] = attribute_id
        return attribute_id

    def _hash_content(self, content: str) -> str:
        """Generate a hash for the given content."""
        return xxhash.xxh64(content.encode('utf-8')).hexdigest()

    def _hash_file(self, file_path: Path) -> str:
        """Generate a hash for the given file."""
        hasher = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            buf = f.read()
            hasher.update(buf)
        return hasher.hexdigest()

    async def save_results(self, output_dir: Path):
        """Save the decomposed data and structure to YAML and Pickle files."""
        data_yaml_path = output_dir / config.files.data_yaml
        structure_yaml_path = output_dir / config.files.structure_yaml
        data_pickle_path = output_dir / config.files.data_pickle
        structure_pickle_path = output_dir / config.files.structure_pickle

        save_tasks = [
            DataHandler.save_yaml(self.data, data_yaml_path),
            DataHandler.save_yaml(self.structure, structure_yaml_path),
            DataHandler.save_pickle(self.data, data_pickle_path),
            DataHandler.save_pickle(self.structure, structure_pickle_path)
        ]

        await tqdm.gather(*save_tasks, desc="Saving results", unit="file")

        logger.info("Decomposed data and structure saved successfully.")


async def main():
    # Define input and output directories
    output_dir = config.paths.decomposed_output_dir

    # Ensure output directory exists
    FileOperations.ensure_directory(output_dir)

    decomposer = Decomposer()
    await decomposer.process()
    await decomposer.save_results(output_dir)

    logger.info("Decomposition process completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())