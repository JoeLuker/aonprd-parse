# src/utils/file_operations.py

import aiofiles
import asyncio
import shutil
import xxhash
from pathlib import Path
from typing import Tuple, Set, Dict, List

from src.utils.logging import Logger
from config.config import config

# Initialize Logger
logger = Logger.get_logger(
    "FileOperationsLogger", config.paths.log_dir / "file_operations.log"
)

class FileOperations:
    @staticmethod
    async def read_file_async(file_path: Path) -> str:
        """Asynchronously read a file's content."""
        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                content = await f.read()
            logger.debug(f"Read file asynchronously: {file_path}")
            return content
        except Exception as e:
            logger.error(
                f"Failed to read file asynchronously {file_path}: {e}", exc_info=True
            )
            raise

    @staticmethod
    async def write_file_async(file_path: Path, content: str):
        """Asynchronously write content to a file."""
        try:
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                await f.write(content)
            logger.debug(f"Wrote file asynchronously: {file_path}")
        except Exception as e:
            logger.error(
                f"Failed to write file asynchronously {file_path}: {e}", exc_info=True
            )
            raise

    @staticmethod
    async def copy_async(source: Path, destination: Path):
        """Asynchronously copy a file from source to destination."""
        try:
            await asyncio.to_thread(shutil.copy2, source, destination)
            logger.debug(f"Copied file asynchronously from {source} to {destination}")
        except Exception as e:
            logger.error(
                f"Failed to copy file asynchronously from {source} to {destination}: {e}",
                exc_info=True,
            )
            raise

    @staticmethod
    async def apply_replacements(content: str, replacements: Dict[str, str]) -> Tuple[str, Set[str]]:
        applied = set()
        for old, new in replacements.items():
            if old in content:
                content = content.replace(old, new)
                applied.add(old)
        return content, applied
    
    @staticmethod
    async def ensure_directory(directory: Path):
        """Ensure that a directory exists asynchronously."""
        try:
            await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
            logger.debug(f"Ensured existence of directory: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}", exc_info=True)
            raise

    @staticmethod
    async def list_files(directory: Path, pattern: str = "*") -> List[Path]:
        """List files in a directory matching the given pattern."""
        return await asyncio.to_thread(list, directory.glob(pattern))

    @staticmethod
    async def remove_file(file_path: Path):
        """Remove a file asynchronously."""
        try:
            await asyncio.to_thread(file_path.unlink)
            logger.debug(f"Removed file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to remove file {file_path}: {e}", exc_info=True)
            raise

    @staticmethod
    async def move_file(source: Path, destination: Path):
        """Move a file from source to destination asynchronously."""
        try:
            await asyncio.to_thread(shutil.move, source, destination)
            logger.debug(f"Moved file from {source} to {destination}")
        except Exception as e:
            logger.error(
                f"Failed to move file from {source} to {destination}: {e}",
                exc_info=True,
            )
            raise

    @staticmethod
    async def get_file_hash(file_path: Path) -> str:
        """Get the xxHash of a file asynchronously."""
        try:
            hasher = xxhash.xxh64()
            async with aiofiles.open(file_path, "rb") as f:
                chunk = await f.read(8192)
                while chunk:
                    hasher.update(chunk)
                    chunk = await f.read(8192)
            file_hash = hasher.hexdigest()
            logger.debug(f"Calculated xxHash for file: {file_path}")
            return file_hash
        except Exception as e:
            logger.error(f"Failed to calculate xxHash for file {file_path}: {e}", exc_info=True)
            raise
