# src/process.py

import asyncio
import sys
from pathlib import Path

# Add the project root directory to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.config import config
from src.utils.logging import Logger
from src.decomposing import decomposer, condense_decomposition
from src.processing import unwrap
from src.importing import csv_prep, memgraph
from src.cleaning import manual_cleaning, cleaner
from src.utils.file_operations import FileOperations

# Initialize Logger using the structured config
logger = Logger.get_logger(
    "ProcessLogger", 
    config.paths.log_dir / "processor.log"
)


async def check_files_exist():
    """Check if required files and directories exist."""
    db_path = config.database.crawler_db
    folder_path = config.paths.input_folder

    db_exists = await asyncio.to_thread(db_path.is_file)
    folder_exists = await asyncio.to_thread(folder_path.is_dir)

    if db_exists and folder_exists:
        logger.info("Both 'crawler_state.db' and 'raw_html_data/' exist.")
    else:
        if not db_exists:
            logger.error(f"Error: '{db_path}' does not exist.")
        if not folder_exists:
            logger.error(f"Error: '{folder_path}' directory does not exist.")
        raise FileNotFoundError("Required files or directories are missing.")


async def run_script(script_func, script_name: str):
    try:
        logger.info(f"Starting {script_name}...")
        await script_func()
        logger.info(f"Script {script_name} completed successfully.")
    except Exception as e:
        logger.error(f"Script {script_name} failed: {e}")
        logger.debug(f"Detailed error for {script_name}:", exc_info=True)
        raise


async def main():
    try:
        await check_files_exist()
        logger.info("Starting all processing scripts...")

        # Optionally, clean output directories
        await FileOperations.ensure_directory(config.paths.processed_output_dir)
        
        # Run scripts
        scripts = [
            (cleaner.main, "Cleaning"),
            (manual_cleaning.main, "Manual Cleaning"),
            (decomposer.main, "Decomposer"),
            (condense_decomposition.main, "Condense Decomposition"),
            (unwrap.main, "Unwrap"),
            (csv_prep.main, "CSV Preparation"),
            (memgraph.main, "Memgraph Importer"),
        ]

        for script_func, script_name in scripts:
            await run_script(script_func, script_name)

        logger.info("All processing scripts completed successfully.")

    except Exception as e:
        logger.error(f"Processing pipeline failed: {e}")
        logger.debug("Detailed error for processing pipeline:", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
