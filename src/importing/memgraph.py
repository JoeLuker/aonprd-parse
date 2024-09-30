import csv
import multiprocessing
import random
import time
from pathlib import Path
from typing import List

from tqdm import tqdm
from gqlalchemy import Memgraph

from config.config import config
from src.utils.logging import Logger
import asyncio

# Initialize Logger
logger = Logger.get_logger(
    "MemgraphImporterLogger",
    config.paths.log_dir / "memgraph_importer.log"
)

class MemgraphImporter:
    def __init__(self, csv_dir: Path, batch_size: int = 1000, max_workers: int = 16):
        self.csv_dir = csv_dir
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.memgraph_host = config.memgraph.host
        self.memgraph_port = config.memgraph.port

    def prepare_load_csv_queries(self) -> List[str]:
        queries = []
        csv_dir = "/var/lib/memgraph/import"
        
        node_files = {
            "Document": "documents.csv",
            "Doctype": "doctypes.csv",
            "TextNode": "texts.csv",
            "Tag": "tags.csv",
            "Attribute": "attributes.csv",
        }

        for node_type, filename in node_files.items():
            queries.append(f"""
            LOAD CSV FROM "{csv_dir}/{filename}" WITH HEADER AS row
            CREATE (:{node_type} {{id: row.id, filename: row.filename, doctype: row.doctype, content: row.content, name: row.name, attributes_id: row.attributes_id, attributes: row.attributes}})
            """)

        return queries

    def execute_query(self, query: str):
        db = Memgraph(host=self.memgraph_host, port=self.memgraph_port)
        db.execute(query)

    def create_indexes(self):
        index_queries = [
            "CREATE INDEX ON :Document(id)",
            "CREATE INDEX ON :Doctype(id)",
            "CREATE INDEX ON :Comment(id)",
            "CREATE INDEX ON :TextNode(id)",
            "CREATE INDEX ON :Tag(id)",
            "CREATE INDEX ON :Attribute(id)"
        ]
        db = Memgraph(host=self.memgraph_host, port=self.memgraph_port)
        for query in index_queries:
            db.execute(query)
        logger.info("Created indexes")

    def process_relationship_batch(self, args):
        batch, relationship_type, source_type, target_type, progress, lock, max_retries, backoff_factor, initial_wait = args
        query = f"""
        UNWIND $batch AS rel
        MATCH (a:{source_type} {{id: rel.source}}), (b:{target_type} {{id: rel.target}})
        CREATE (a)-[:{relationship_type} {{order: rel.order}}]->(b)
        """
        
        db = Memgraph(host=self.memgraph_host, port=self.memgraph_port)
        
        for attempt in range(max_retries):
            try:
                db.execute(query, {"batch": batch})
                with lock:
                    progress.value += len(batch)
                break
            except Exception as e:
                wait_time = initial_wait * (backoff_factor ** attempt) + random.uniform(0, 0.1)
                logger.error(f"Failed to execute batch, attempt {attempt + 1}: {e}. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                if attempt == max_retries - 1:
                    logger.error("Max retries reached, failing the batch.")
                    raise

    def batch_relationships(self, relationship_data, batch_size=100):
        for i in range(0, len(relationship_data), batch_size):
            yield relationship_data[i:i + batch_size]

    def import_relationships_parallel(self, csv_file_path, relationship_type):
        logger.info(f"Importing relationships from {csv_file_path} with type {relationship_type}")
        relationship_data = []
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            total_rows = 0
            for row in reader:
                total_rows += 1
                source_type = row.get("source_type", "SourceType")
                target_type = row.get("target_type", "TargetType")
                if "order" in row:
                    relationship_data.append({"source": row["source"], "target": row["target"], "order": int(row["order"])})
                else:
                    relationship_data.append({"source": row["source"], "target": row["target"]})
        
        logger.info(f"Total number of rows: {total_rows}")
        total_relationships = len(relationship_data)
        logger.info(f"Total number of relationships: {total_relationships}")
        batches = list(self.batch_relationships(relationship_data, batch_size=self.batch_size))
        
        manager = multiprocessing.Manager()
        progress = manager.Value('i', 0)
        lock = manager.Lock()
        
        max_retries = 5
        backoff_factor = 1.5
        initial_wait = 0.5

        with multiprocessing.Pool(self.max_workers) as pool:
            with tqdm(total=total_relationships, desc=f"Importing {relationship_type}") as pbar:
                for _ in pool.imap_unordered(self.process_relationship_batch, [
                    (batch, relationship_type, source_type, target_type, progress, lock, max_retries, backoff_factor, initial_wait)
                    for batch in batches
                ]):
                    pbar.update(self.batch_size)
        
        logger.info(f"Imported {progress.value}/{total_relationships} relationships for {relationship_type}")

    def run_import(self):
        logger.info("Starting Memgraph import process...")

        # Drop the existing graph
        logger.info("Dropping existing graph...")
        drop_query = "DROP GRAPH"
        self.execute_query(drop_query)
        logger.info("Existing graph dropped successfully")

        # Create indexes
        self.create_indexes()

        # Prepare and execute LOAD CSV queries
        queries = self.prepare_load_csv_queries()

        logger.info(f"Prepared {len(queries)} queries")

        # Execute queries
        for query in tqdm(queries, desc="Executing queries"):
            self.execute_query(query)

        logger.info("Node import completed successfully")

        # Import relationships
        relationship_files = [
            ("contains_text.csv", "CONTAINS_TEXT"),
            ("contains_tag.csv", "CONTAINS_TAG"),
            ("has_doctype.csv", "HAS_DOCTYPE"),
            ("has_root.csv", "HAS_ROOT"),
            ("has_attribute.csv", "HAS_ATTRIBUTE"),
        ]

        for filename, rel_type in relationship_files:
            file_path = self.csv_dir / filename
            if file_path.exists():
                self.import_relationships_parallel(file_path, rel_type)
            else:
                logger.warning(f"Relationship file {filename} not found. Skipping.")

        logger.info("Relationship import completed successfully")

async def main():
    csv_dir = config.paths.import_files_dir
    importer = MemgraphImporter(
        csv_dir,
        batch_size=config.memgraph.batch_size,
        max_workers=config.processing.max_workers,
    )
    try:
        await asyncio.to_thread(importer.run_import)
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}", exc_info=True)
        raise  # Re-raise the exception to be caught by the calling function

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())