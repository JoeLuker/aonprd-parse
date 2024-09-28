# src/cleaning/cleaner.py

import asyncio
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Any, Tuple, Set

from tqdm import tqdm
from bs4 import BeautifulSoup

from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations

# Initialize Logger
logger = Logger.get_logger(
    "CleanerLogger", config.paths.log_dir / config.logging.processor_log
)


async def connect_to_db(db_path: Path) -> sqlite3.Connection:
    try:
        conn = await asyncio.to_thread(sqlite3.connect, str(db_path))
        logger.debug(f"Connected to database at {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database at {db_path}: {e}", exc_info=True)
        raise


async def get_html_file_mapping(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    query = """
    SELECT 
        f1.file_name AS file_with_sort, 
        f2.file_name AS file_without_sort,
        f1.relative_url AS url_with_sort,
        f2.relative_url AS url_without_sort
    FROM files f1
    JOIN files f2 ON f1.relative_url = f2.relative_url || '&SchoolSort=true'
    WHERE f1.relative_url LIKE 'Spells.aspx?Class=%&SchoolSort=true'
    """
    try:
        cursor = await conn.cursor()
        await cursor.execute(query)
        rows = await cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        mapping = [dict(zip(columns, row)) for row in rows]
        logger.debug(f"Retrieved {len(mapping)} HTML file mappings from the database")
        return mapping
    except sqlite3.Error as e:
        logger.error(f"Failed to retrieve HTML file mappings: {e}", exc_info=True)
        raise


async def prepare_canonical_mapping(
    mapping_data: List[Dict[str, Any]]
) -> List[Tuple[str, str, str, str]]:
    canonical_mappings = [
        (
            item["file_without_sort"],
            item["file_with_sort"],
            item["url_without_sort"],
            item["url_with_sort"],
        )
        for item in mapping_data
    ]
    logger.debug(f"Prepared {len(canonical_mappings)} canonical mappings")
    return canonical_mappings


async def insert_canonical_mapping(
    conn: sqlite3.Connection, data_to_insert: List[Tuple[str, str, str, str]]
):
    try:
        cursor = await conn.cursor()
        await cursor.executemany(
            """
        INSERT OR REPLACE INTO canonical_mapping 
        (canonical_file, duplicate_file, canonical_url, duplicate_url)
        VALUES (?, ?, ?, ?)
        """,
            data_to_insert,
        )
        await conn.commit()
        logger.info(
            f"Inserted {len(data_to_insert)} entries into canonical_mapping table"
        )
    except sqlite3.Error as e:
        logger.error(f"Failed to insert canonical mappings: {e}", exc_info=True)
        raise


async def get_skip_files(conn: sqlite3.Connection) -> Set[str]:
    search_files = {
        "63634bccb56c98559dab055327186a07.html",
        "f6d4aaa54adebd6a564120102e5af5a8.html",
        "ed23e664aba0fb30e732b358bf07df25.html",
        "ebcc0f2ca46897ee6e13d42cd9e28517.html",
        "76f8753eaeb05d007c35258e2712db49.html",
    }

    mapping_data = await get_html_file_mapping(conn)
    duplicate_files = {item["file_with_sort"] for item in mapping_data}
    skip_files = search_files.union(duplicate_files)
    logger.debug(f"Total skip files: {len(skip_files)}")
    return skip_files


async def clean_and_copy_files(
    source_dir: Path, destination_dir: Path, skip_files: Set[str]
) -> Tuple[List[str], Dict[str, int]]:
    modifications = []
    replacement_counts = defaultdict(int)

    files = list(source_dir.iterdir())
    logger.info(f"Found {len(files)} files to process in {source_dir}")

    for file_path in tqdm(files, desc="Processing files"):
        if file_path.name in skip_files:
            logger.debug(f"Skipping file: {file_path.name}")
            continue
        destination_path = destination_dir / file_path.name
        try:
            content = await FileOperations.read_file_async(file_path)
            modified_content, applied_replacements = FileOperations.apply_replacements(
                content, config.cleaning.replacements
            )
            if applied_replacements:
                await FileOperations.write_file_async(
                    destination_path, modified_content
                )
                modifications.append(file_path.name)
                for replacement in applied_replacements:
                    replacement_counts[replacement] += 1
                logger.info(f"Applied replacements to file: {file_path.name}")
            else:
                await FileOperations.copy_async(file_path, destination_path)
                logger.info(
                    f"No replacements needed for file: {file_path.name}. Copied as-is."
                )
        except Exception as e:
            logger.error(f"Failed to process file {file_path.name}: {e}", exc_info=True)

    return modifications, replacement_counts


async def process_database(conn: sqlite3.Connection):
    try:
        mapping_data = await get_html_file_mapping(conn)
        logger.info(f"Total mappings retrieved: {len(mapping_data)}")

        if not mapping_data:
            logger.warning("No mappings found to process in the database.")
            return

        data_to_insert = await prepare_canonical_mapping(mapping_data)
        await insert_canonical_mapping(conn, data_to_insert)
    except Exception as e:
        logger.error(f"Error processing database mappings: {e}", exc_info=True)
        raise


async def parse_html_file(file_path: Path) -> Dict[str, Any]:
    try:
        content = await FileOperations.read_file_async(file_path)
        soup = BeautifulSoup(content, "lxml")

        title = soup.title.string.strip() if soup.title and soup.title.string else ""

        form = soup.find("form")
        form_action = form.get("action") if form else ""

        meta_tags = [
            (meta.get("name") or meta.get("property"), meta.get("content"))
            for meta in soup.find_all("meta")
            if (meta.get("name") or meta.get("property")) and meta.get("content")
        ]

        return {
            "file_name": file_path.name,
            "title": title,
            "form_action": form_action,
            "meta_tags": meta_tags,
        }
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return None


async def process_html_files(input_dir: Path) -> List[Dict[str, Any]]:
    try:
        files = [f for f in input_dir.iterdir() if f.suffix == ".html"]
        tasks = [parse_html_file(f) for f in files]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]
    except Exception as e:
        logger.error(f"Error processing HTML files: {e}")
        raise


async def main():
    logger.info("Starting Manual Cleaning Process...")

    # Connect to the database
    conn = await connect_to_db(config.database.consolidated_html_db)

    try:
        # Get files to skip
        skip_files = await get_skip_files(conn)

        # Clean and copy files
        modifications, replacement_counts = await clean_and_copy_files(
            source_dir=config.paths.input_folder,
            destination_dir=config.paths.manual_cleaned_html_data,
            skip_files=skip_files,
        )

        # Print summary
        logger.info("Files with replacements applied:")
        for file_name in modifications:
            logger.info(f"- {file_name}")

        logger.info("\nReplacement counts:")
        for replacement, count in replacement_counts.items():
            logger.info(f"- '{replacement}': {count} files")

        # Process database mappings
        await process_database(conn)

        # Process HTML files
        processed_files = await process_html_files(
            config.paths.manual_cleaned_html_data
        )
        logger.info(f"Processed {len(processed_files)} HTML files")

    finally:
        await conn.close()
        logger.debug("Database connection closed.")

    logger.info("Manual Cleaning Process Completed Successfully.")


if __name__ == "__main__":
    asyncio.run(main())
