# src/importing/memgraph.py

import asyncio
import csv
import multiprocessing
import random
from pathlib import Path
from typing import List, Tuple, Dict, Any

from gqlalchemy import Memgraph
from tqdm.asyncio import tqdm as atqdm

from config.config import config
from src.utils.logging import Logger

# Initialize Logger
logger = Logger.get_logger(
    "MemgraphImporterLogger",
    config.paths.log_dir / "memgraph_importer.log",
)


class MemgraphConnectionPool:
    def __init__(self, host: str, port: int, max_connections: int):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.connections = []
        self.lock = multiprocessing.Lock()

    def get_connection(self):
        with self.lock:
            if not self.connections:
                return Memgraph(host=self.host, port=self.port)
            return self.connections.pop()

    def return_connection(self, connection):
        with self.lock:
            if len(self.connections) < self.max_connections:
                self.connections.append(connection)
            else:
                connection._client.close()


class MemgraphImporter:
    def __init__(self, csv_dir: Path, batch_size: int = 1000, max_workers: int = 16):
        self.csv_dir = csv_dir
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.connection_pool = MemgraphConnectionPool(
            config.memgraph.host, config.memgraph.port, max_workers
        )

    async def execute_query(self, query: str, params: Dict[str, Any] = None):
        """Asynchronously execute a single Memgraph query."""
        db = self.connection_pool.get_connection()
        try:
            await asyncio.to_thread(db.execute, query, params)
            logger.debug(f"Executed query: {query[:60]}...")
        except Exception as e:
            logger.error(f"Failed to execute query: {e}", exc_info=True)
        finally:
            self.connection_pool.return_connection(db)

    async def create_indexes(self):
        """Create necessary indexes in Memgraph."""
        index_queries = [
            "CREATE INDEX ON :Document(id)",
            "CREATE INDEX ON :Doctype(id)",
            "CREATE INDEX ON :Comment(id)",
            "CREATE INDEX ON :TextNode(id)",
            "CREATE INDEX ON :Tag(id)",
            "CREATE INDEX ON :Attribute(id)",
        ]
        tasks = [self.execute_query(query) for query in index_queries]
        await asyncio.gather(*tasks)
        logger.info("Created all necessary indexes in Memgraph.")

    async def load_nodes(self):
        """Load node data from CSV files into Memgraph."""
        node_files = {
            "Document": "documents.csv",
            "Doctype": "doctypes.csv",
            "Comment": "comments.csv",
            "TextNode": "texts.csv",
            "Tag": "tags.csv",
            "Attribute": "attributes.csv",
        }
        tasks = []
        for node_type, filename in node_files.items():
            filepath = self.csv_dir / filename
            if not filepath.exists():
                logger.warning(f"CSV file for {node_type} not found: {filepath}")
                continue
            query = f"""
            LOAD CSV FROM "{filepath}" WITH HEADER AS row
            CREATE (:{node_type} {{id: row.id, filename: row.filename, doctype: row.doctype, comment: row.comment, content: row.content, name: row.name, attributes_id: row.attributes_id}})
            """
            tasks.append(self.execute_query(query))
        await asyncio.gather(*tasks)
        logger.info("Loaded all node data into Memgraph.")

    def get_node_types(self, rel_type: str) -> Tuple[str, str]:
        """Determine source and target node types based on relationship type."""
        rel_map = {
            "HAS_DOCTYPE": ("Document", "Doctype"),
            "HAS_COMMENT": ("Document", "Comment"),
            "CONTAINS_TEXT": ("Tag", "TextNode"),
            "CONTAINS_TAG": ("Document", "Tag"),
            "HAS_ROOT": ("Document", "Tag"),
            "HAS_ATTRIBUTE": ("Tag", "Attribute"),
        }
        return rel_map.get(rel_type, ("Unknown", "Unknown"))

    async def process_relationship_batch(
        self,
        batch: List[Dict[str, Any]],
        relationship_type: str,
        source_type: str,
        target_type: str,
    ):
        query = f"""
        UNWIND $batch AS rel
        MATCH (a:{source_type} {{id: rel.source}}), (b:{target_type} {{id: rel.target}})
        CREATE (a)-[:{relationship_type} {{order: rel.order}}]->(b)
        """

        max_retries = 5
        backoff_factor = 1.5
        initial_wait = 0.5

        for attempt in range(max_retries):
            try:
                await self.execute_query(query, {"batch": batch})
                break
            except Exception as e:
                wait_time = initial_wait * (backoff_factor**attempt) + random.uniform(
                    0, 0.1
                )
                logger.error(
                    f"Failed to execute batch, attempt {attempt + 1}: {e}. Retrying in {wait_time:.2f} seconds..."
                )
                await asyncio.sleep(wait_time)
                if attempt == max_retries - 1:
                    logger.error("Max retries reached, failing the batch.")
                    raise

    async def import_relationships(self, csv_file_path: Path, relationship_type: str):
        logger.info(
            f"Importing relationships from {csv_file_path} with type {relationship_type}"
        )
        relationship_data = []
        with open(csv_file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            relationship_data = list(reader)

        total_relationships = len(relationship_data)
        logger.info(f"Total number of relationships: {total_relationships}")

        source_type, target_type = self.get_node_types(relationship_type)

        tasks = []
        for i in range(0, total_relationships, self.batch_size):
            batch = relationship_data[i : i + self.batch_size]
            tasks.append(
                self.process_relationship_batch(
                    batch, relationship_type, source_type, target_type
                )
            )

        for f in atqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc=f"Importing {relationship_type}",
        ):
            await f

        logger.info(
            f"Imported {total_relationships} relationships for {relationship_type}"
        )

    async def drop_existing_graph(self):
        """Drop the existing graph in Memgraph."""
        logger.info("Dropping existing graph...")
        await self.execute_query("DROP GRAPH")
        logger.info("Existing graph dropped successfully")

    async def run_import(self):
        """Execute the full import process."""
        await self.drop_existing_graph()
        await self.create_indexes()
        await self.load_nodes()

        relationship_files = [
            ("contains_text.csv", "CONTAINS_TEXT"),
            ("contains_tag.csv", "CONTAINS_TAG"),
            ("has_comment.csv", "HAS_COMMENT"),
            ("has_doctype.csv", "HAS_DOCTYPE"),
            ("has_root.csv", "HAS_ROOT"),
            ("has_attribute.csv", "HAS_ATTRIBUTE"),
        ]

        for filename, rel_type in relationship_files:
            file_path = self.csv_dir / filename
            if file_path.exists():
                await self.import_relationships(file_path, rel_type)
            else:
                logger.warning(f"Relationship file {filename} not found. Skipping.")

        logger.info("Memgraph data import completed successfully.")

    async def create_nodes(self, nodes: List[Dict[str, Any]]):
        for node in nodes:
            query = f"""
            MERGE (n:{node['type']} {{id: $id}})
            SET n += $properties
            """
            await self.execute_query(query, id=node['id'], properties=node['properties'])


async def main():
    csv_dir = config.paths.import_files_dir
    importer = MemgraphImporter(
        csv_dir,
        batch_size=config.memgraph.batch_size,
        max_workers=config.processing.max_workers,
    )
    await importer.run_import()


if __name__ == "__main__":
    asyncio.run(main())