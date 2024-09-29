# src/importing/csv_prep.py

import asyncio
import csv
import json
from pathlib import Path
from typing import Dict, Any

from tqdm import tqdm

from config.config import config
from src.utils.logging import Logger
from src.utils.data_handling import DataHandler

# Initialize Logger
logger = Logger.get_logger(
    "CSVPrepLogger", config.paths.log_dir / config.logging.csv_prep_log
)


class CSVExporter:
    """
    Handles exporting nodes, attributes, and relationships to CSV files.
    """

    def __init__(self, csv_dir: Path):
        self.csv_dir = csv_dir
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.files = self._initialize_csv_files()

    def _initialize_csv_files(self) -> Dict[str, Dict[str, Any]]:
        """
        Initializes CSV writers for different node and relationship types.
        """
        file_specs = {
            "Document": ("documents.csv", ["id", "filename"]),
            "Doctype": ("doctypes.csv", ["id", "doctype"]),
            "Comment": ("comments.csv", ["id", "comment"]),
            "TextNode": ("texts.csv", ["id", "content"]),
            "Tag": ("tags.csv", ["id", "name", "attributes_id"]),
            "Attribute": ("attributes.csv", ["id", "attributes"]),
            "HAS_DOCTYPE": (
                "has_doctype.csv",
                ["source", "target", "source_type", "target_type", "order"],
            ),
            "HAS_COMMENT": (
                "has_comment.csv",
                ["source", "target", "source_type", "target_type", "order"],
            ),
            "CONTAINS_TEXT": (
                "contains_text.csv",
                ["source", "target", "source_type", "target_type", "order"],
            ),
            "CONTAINS_TAG": (
                "contains_tag.csv",
                ["source", "target", "source_type", "target_type", "order"],
            ),
            "HAS_ROOT": (
                "has_root.csv",
                ["source", "target", "source_type", "target_type", "order"],
            ),
            "HAS_ATTRIBUTE": (
                "has_attribute.csv",
                ["source", "target", "source_type", "target_type"],
            ),
        }

        files = {}
        for key, (filename, headers) in file_specs.items():
            filepath = self.csv_dir / filename
            try:
                file = open(filepath, "w", newline="", encoding="utf-8")
                writer = csv.writer(file)
                writer.writerow(headers)
                logger.debug(f"Created CSV file: {filepath}")
                files[key] = {"file": file, "writer": writer}
            except Exception as e:
                logger.error(f"Failed to create CSV file {filepath}: {e}")
                files[key] = {"file": None, "writer": None}
        return files

    async def export_node(self, node_type: str, node_id: str, properties: Dict[str, Any]):
        """
        Exports a node to the corresponding CSV file.
        """
        writer = self.files.get(node_type, {}).get("writer")
        if not writer:
            logger.warning(f"No CSV writer found for node type: {node_type}")
            return
        if node_type == "Tag":
            await asyncio.to_thread(writer.writerow, [
                node_id,
                properties.get("name", ""),
                properties.get("attributes_id", ""),
            ])
        else:
            await asyncio.to_thread(writer.writerow, [node_id] + list(properties.values()))
        logger.debug(f"Exported node: {node_type} with ID {node_id}")

    async def export_attribute(self, attribute_id: str, attributes: Dict[str, Any]):
        """
        Exports an attribute to the attributes CSV file.
        """
        writer = self.files.get("Attribute", {}).get("writer")
        if not writer:
            logger.warning("No CSV writer found for attributes")
            return
        await asyncio.to_thread(writer.writerow, [attribute_id, json.dumps(attributes)])
        logger.debug(f"Exported attribute: {attribute_id}")

    async def export_relationship(
        self,
        rel_type: str,
        source: str,
        target: str,
        source_type: str,
        target_type: str,
        order: int = None,
    ):
        """
        Exports a relationship to the corresponding CSV file.
        """
        writer = self.files.get(rel_type, {}).get("writer")
        if not writer:
            logger.warning(f"No CSV writer found for relationship type: {rel_type}")
            return
        if order is not None:
            await asyncio.to_thread(writer.writerow, [source, target, source_type, target_type, order])
        else:
            await asyncio.to_thread(writer.writerow, [source, target, source_type, target_type])
        logger.debug(f"Exported relationship: {rel_type} from {source} to {target}")

    async def close(self):
        """
        Closes all open CSV files.
        """
        for rel_type, file_dict in self.files.items():
            file = file_dict.get("file")
            if file:
                try:
                    await asyncio.to_thread(file.close)
                    logger.debug(f"Closed CSV file for {rel_type}")
                except Exception as e:
                    logger.error(f"Failed to close CSV file for {rel_type}: {e}")
        logger.debug("Closed all CSV files")


class CSVPreparation:
    """
    Prepares CSV files from the structured data for Memgraph import.
    """

    def __init__(self):
        self.structure = None
        self.data = None
        self.exporter = None
        self.node_id_map: Dict[str, str] = {}
        self.logger = logger

    async def run(self):
        self.logger.info("Starting CSV Preparation Process...")
        await self.load_data()
        await self.prepare_nodes()
        await self.prepare_attributes()
        await self.prepare_relationships()
        await self.exporter.close()
        self.logger.info("CSV Preparation Process Completed.")

    async def load_data(self):
        self.logger.info("Loading data from Pickle files...")
        structure_pickle_path = (
            config.paths.condensed_output_dir / f"unwrapped_{config.files.structure_pickle}"
        )
        data_pickle_path = (
            config.paths.condensed_output_dir / f"unwrapped_{config.files.data_pickle}"
        )
        try:
            self.structure = await DataHandler.load_pickle(structure_pickle_path)
            self.data = await DataHandler.load_pickle(data_pickle_path)
        except Exception as e:
            self.logger.error(f"Failed to load pickles: {e}")
            return

        if not self.structure or not self.data:
            self.logger.error("Loaded data is empty. Exiting CSV preparation.")
            return

        self.exporter = CSVExporter(config.paths.import_files_dir)

    async def prepare_nodes(self):
        self.logger.info("Preparing node CSV files...")
        for node in tqdm(self.structure.get("nodes", []), desc="Exporting Nodes"):
            node_id = node.get("id", "")
            node_type_raw = node.get("type", "").lower()
            node_type_map = {
                "document": "Document",
                "doctype": "Doctype",
                "comment": "Comment",
                "textnode": "TextNode",
                "tag": "Tag",
            }
            node_type = node_type_map.get(node_type_raw, "Unknown")
            self.node_id_map[node_id] = node_type
            if node_type in ["Document", "Doctype", "Comment", "TextNode", "Tag"]:
                try:
                    if node_type == "Document":
                        filename = node.get("filename", "")
                        await self.exporter.export_node(
                            "Document", node_id, {"filename": filename}
                        )
                    elif node_type == "Doctype":
                        doctype_content = self.data["doctypes"].get(
                            node.get("data_id", ""), ""
                        )
                        await self.exporter.export_node(
                            "Doctype", node_id, {"doctype": doctype_content}
                        )
                    elif node_type == "Comment":
                        comment_content = self.data["comments"].get(
                            node.get("data_id", ""), ""
                        )
                        await self.exporter.export_node(
                            "Comment", node_id, {"comment": comment_content}
                        )
                    elif node_type == "TextNode":
                        text_content = self.data["texts"].get(
                            node.get("data_id", ""), ""
                        )
                        await self.exporter.export_node(
                            "TextNode", node_id, {"content": text_content}
                        )
                    elif node_type == "Tag":
                        tag_name = node.get("name", "")
                        attributes_id = node.get("attributes_id", "")
                        await self.exporter.export_node(
                            "Tag",
                            node_id,
                            {"name": tag_name, "attributes_id": attributes_id},
                        )
                        if attributes_id:
                            await self.exporter.export_relationship(
                                "HAS_ATTRIBUTE",
                                node_id,
                                attributes_id,
                                "Tag",
                                "Attribute",
                            )
                except Exception as e:
                    self.logger.error(f"Error exporting node {node_id}: {e}")
            else:
                self.logger.warning(f"Unknown node type: {node_type_raw} for node {node}")

    async def prepare_attributes(self):
        self.logger.info("Preparing attributes CSV file...")
        for attribute_id, attributes in tqdm(
            self.data.get("attributes", {}).items(), desc="Exporting Attributes"
        ):
            try:
                await self.exporter.export_attribute(attribute_id, attributes)
            except Exception as e:
                self.logger.error(f"Error exporting attribute {attribute_id}: {e}")

    async def prepare_relationships(self):
        self.logger.info("Preparing relationship CSV files...")
        for edge in tqdm(
            self.structure.get("edges", []), desc="Exporting Relationships"
        ):
            try:
                source = edge.get("source", "")
                target = edge.get("target", "")
                relationship = edge.get("relationship", "").upper().strip()
                order = edge.get("order", None)

                source_type = self.node_id_map.get(source, "Unknown")
                target_type = self.node_id_map.get(target, "Unknown")

                rel_map = {
                    "HAS_DOCTYPE": "HAS_DOCTYPE",
                    "HAS_COMMENT": "HAS_COMMENT",
                    "CONTAINS_TEXT": "CONTAINS_TEXT",
                    "CONTAINS_TAG": "CONTAINS_TAG",
                    "HAS_ROOT": "HAS_ROOT",
                    "HAS_ATTRIBUTE": "HAS_ATTRIBUTE",
                }

                csv_rel_type = rel_map.get(relationship, None)
                if csv_rel_type:
                    if csv_rel_type == "HAS_ATTRIBUTE":
                        await self.exporter.export_relationship(
                            csv_rel_type, source, target, source_type, "Attribute"
                        )
                    else:
                        await self.exporter.export_relationship(
                            csv_rel_type,
                            source,
                            target,
                            source_type,
                            target_type,
                            order,
                        )
                else:
                    self.logger.warning(f"Unknown relationship type: {relationship}")
            except Exception as e:
                self.logger.error(f"Error exporting relationship {relationship}: {e}")


async def main():
    prep = CSVPreparation()
    await prep.run()


if __name__ == "__main__":
    asyncio.run(main())
