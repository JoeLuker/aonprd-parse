# src/cleaning/manual_cleaning.py

import asyncio
import shutil
import sqlite3
from pathlib import Path
from typing import List, Set, Tuple, Dict, Any
from collections import defaultdict

from tqdm.asyncio import tqdm

from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations

# Initialize Logger
logger = Logger.get_logger(
    "ManualCleaningLogger", config.paths.log_dir / config.logging.processor_log
)

# Define replacements
REPLACEMENTS = {
    "<<": "<",
    "<istirge": "<i>stirge",
    "<blackmarrow": "<i>blackmarow",
    '<MONSTERS%Alchemical Golem">alchemical golem</a>': '<a style="text-decoration:underline" href="MonsterDisplay.aspx?ItemName=Alchemical Golem">alchemical golem</a>.',
    '<RULES%Assigning Damage&Category=Step 9: Damage">': '<a style="text-decoration:underline" href="/Rules.aspx?ID=2037">',
    "%>": '">',
    '"<img src="': '"><img src="',
    "<Sign": "Sign",
    '<a style="text-decoration:underline" href="DeityDisplay.aspx?ItemName=<a style="text-decoration:underline"': '<a style="text-decoration:underline"',
    "%%daemons": '">daemons',
    "ItemName=mummies": 'ItemName=mummies"',
    'ItemName=mummies"</a>,': 'ItemName=mummies">Mummies</a>,',
    "ItemName=Bashing%%": 'ItemName=Bashing">',
    "%%shapeable": '">shapeable',
    "Rules%%": '">Rules',
    "<Capital Earned* (Goods, Influence, Labor, or Magic)/b>": "Capital Earned* (Goods, Influence, Labor, or Magic)</b>",
    "<b><Plane</b>": "<b>Plane</b>",
    '<td colspan="2"</td>': '<td colspan="2"></td>',
    '<CAV.ORDERS%Order of the Lion">': '<a style="text-decoration:underline" href="CavalierOrders.aspx?ItemName=Order of the Lion">',
    '<CAV.ORDERS%Order of the Dragon">': '<a style="text-decoration:underline" href="CavalierOrders.aspx?ItemName=Order of the Dragon">',
    "<br /’>": "<br />",
    '<MONSTERS%Worg%">worg<%END>': '<a style="text-decoration:underline" href="MonsterDisplay.aspx?ItemName=Worg">worg</a>',
    "<hypnotism</i>": "<i>hypnotism</i>",
    "<b...": "",
    '<table class="inner")<tr>': '<table class="inner">)<tr>',
    "atthe GM": "at the GM",
    "<br /<i>": "<br /><i>",
    "<td21": "<td>21",
    '<RULES%Chases&Category=Advanced Topics">': '<a style="text-decoration:underline" href="/Rules.aspx?ID=874">',
    '<FEATS%Scribe Scroll">': '<a style="text-decoration:underline" href="FeatDisplay.aspx?ItemName=Scribe Scroll">',
    "<sup<": "<sup>",
    "colspace": "colspan",
    "<MAGIC.WONDROUS%Monk's Robe\">": '<a style="text-decoration:underline" href="MagicWondrousDisplay.aspx?FinalName=Monk\'s Robe">',
    '<h2 ="title">': '<h2 class="title">',
    '<MAGIC.ARMOR">Ghost Touch">': '<a style="text-decoration:underline" href="MagicArmorDisplay.aspx?ItemName=Ghost Touch">',
    '<DEITIES%SARENRAE">': '<a style="text-decoration:underline" href="DeityDisplay.aspx?ItemName=Sarenrae">',
    "<i<i>": "<i>",
    "i?=>": "i>",
    '<MAGIC.WEAPONS%Bane">': '<a style="text-decoration:underline" href="MagicWeaponsDisplay.aspx?ItemName=Bane">',
}


async def copy_file_async(source: Path, destination: Path):
    """Asynchronously copy a file from source to destination."""
    try:
        await asyncio.to_thread(shutil.copy2, source, destination)
        logger.debug(f"Copied file from {source} to {destination}")
    except Exception as e:
        logger.error(
            f"Failed to copy file from {source} to {destination}: {e}", exc_info=True
        )
        raise


async def process_file_async(
    source_path: Path, destination_path: Path, skip_files: Set[str]
) -> Tuple[bool, Set[str]]:
    """Asynchronously process a single file: apply replacements or copy as-is."""
    if source_path.name in skip_files:
        await copy_file_async(source_path, destination_path)
        logger.info(f"Skipped and copied file: {source_path.name}")
        return False, set()
    try:
        content = await FileOperations.read_file_async(source_path)
        modified_content, applied_replacements = FileOperations.apply_replacements(
            content, REPLACEMENTS
        )
        if applied_replacements:
            await FileOperations.write_file_async(destination_path, modified_content)
            logger.info(f"Applied replacements to file: {source_path.name}")
            return True, applied_replacements
        else:
            await copy_file_async(source_path, destination_path)
            logger.info(
                f"No replacements needed for file: {source_path.name}. Copied as-is."
            )
            return False, set()
    except Exception as e:
        logger.error(f"Error processing file {source_path.name}: {e}", exc_info=True)
        return False, set()


async def clean_and_copy_files_async(
    skip_files: Set[str],
) -> Tuple[List[str], Dict[str, int]]:
    """Asynchronously clean and copy files, returning modifications and replacement counts."""
    tasks = []
    modifications = []
    replacement_counts = defaultdict(int)

    for file_path in config.paths.input_folder.iterdir():
        if file_path.is_file():
            destination_path = config.paths.manual_cleaned_html_data / file_path.name
            tasks.append(process_file_async(file_path, destination_path, skip_files))

    for result in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), desc="Processing files"
    ):
        modified, applied_replacements = await result
        if modified:
            modifications.append(file_path.name)
            for replacement in applied_replacements:
                replacement_counts[replacement] += 1

    return modifications, replacement_counts


async def connect_to_db(db_path: Path) -> sqlite3.Connection:
    """Connect to the SQLite database."""
    try:
        conn = await asyncio.to_thread(sqlite3.connect, str(db_path))
        logger.debug(f"Connected to database at {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database at {db_path}: {e}", exc_info=True)
        raise


async def get_html_file_mapping(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Retrieve HTML file mappings from the database."""
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
        cursor = await conn.execute(query)
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
    """Prepare canonical mapping tuples for database insertion."""
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
    """Insert canonical mappings into the database."""
    try:
        await conn.executemany(
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
    """Retrieve a set of files to skip during processing."""
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


async def process_database(conn: sqlite3.Connection):
    """Process database mappings and insert canonical mappings."""
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


async def main():
    """Main asynchronous function to orchestrate the manual cleaning process."""
    logger.info("Starting Manual Cleaning Process...")

    # Ensure destination directory exists
    FileOperations.ensure_directory(config.paths.manual_cleaned_html_data)

    # Connect to the database
    conn = await connect_to_db(config.database.consolidated_html_db)

    try:
        # Get files to skip
        skip_files = await get_skip_files(conn)

        # Clean and copy files asynchronously
        modifications, replacement_counts = await clean_and_copy_files_async(skip_files)

        # Print summary
        logger.info("Files with replacements applied:")
        for file_name in modifications:
            logger.info(f"- {file_name}")

        logger.info("\nReplacement counts:")
        for replacement, count in replacement_counts.items():
            logger.info(f"- '{replacement}': {count} files")

        # Process database mappings
        await process_database(conn)

    finally:
        await conn.close()
        logger.debug("Database connection closed.")

    logger.info("Manual Cleaning Process Completed Successfully.")


if __name__ == "__main__":
    asyncio.run(main())
