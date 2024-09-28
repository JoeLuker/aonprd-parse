# src/process.py

import asyncio
from pathlib import Path
from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations
from src.decomposing import decomposer, condense_decomposition
from src.processing import unwrap
from src.importing import csv_prep, memgraph
from src.cleaning import manual_cleaning

# Initialize Logger using the structured config
logger = Logger.get_logger(
    'ProcessLogger',
    config.paths.log_dir / config.logging.processor_log
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

async def run_script(script_coro, script_name: str):
    """Run an asynchronous script coroutine."""
    try:
        logger.info(f"Starting {script_name}...")
        await script_coro
        logger.info(f"Script {script_name} completed successfully.")
    except Exception as e:
        logger.error(f"Script {script_name} failed: {e}", exc_info=True)
        raise  # Re-raise the exception to stop the pipeline

async def main():
    try:
        await check_files_exist()
        logger.info("Starting all processing scripts...")

        # Define script coroutines
        scripts = [
            (manual_cleaning.main(), 'Manual Cleaning'),
            (decomposer.main(), 'Decomposer'),
            (condense_decomposition.main(), 'Condense Decomposition'),
            (unwrap.main(), 'Unwrap'),
            (csv_prep.main(), 'CSV Preparation'),
            (memgraph.main(), 'Memgraph Importer')
        ]

        # Run scripts sequentially
        for script_coro, script_name in scripts:
            await run_script(script_coro, script_name)

        logger.info("All processing scripts completed successfully.")

    except Exception as e:
        logger.error(f"Processing pipeline failed: {e}", exc_info=True)
        raise  # Re-raise the exception to indicate failure to the caller

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        logger.exception("Processing pipeline failed with an unhandled exception.")
        exit(1)  # Exit with a non-zero status code to indicate failure