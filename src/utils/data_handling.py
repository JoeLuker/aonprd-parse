# src/utils/data_handling.py
import pickle
import yaml
import asyncio
import aiofiles
from pathlib import Path
from typing import Dict, Any

from src.utils.logging import Logger

logger = Logger.get_logger("DataHandlingLogger")


class DataHandler:
    @staticmethod
    async def load_yaml(filepath: Path) -> Dict[str, Any]:
        try:
            async with aiofiles.open(filepath, "r", encoding="utf-8") as file:
                content = await file.read()
                data = yaml.safe_load(content)
            logger.debug(f"Loaded YAML data from {filepath}")
            return data
        except Exception as e:
            error_msg = f"Failed to load YAML from {filepath}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

    @staticmethod
    async def save_yaml(data: Dict[str, Any], filepath: Path):
        try:
            yaml_content = yaml.dump(
                data, default_flow_style=False, allow_unicode=True, sort_keys=False
            )
            async with aiofiles.open(filepath, "w", encoding="utf-8") as file:
                await file.write(yaml_content)
            logger.debug(f"Saved YAML data to {filepath}")
        except Exception as e:
            error_msg = f"Failed to save YAML to {filepath}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

    @staticmethod
    async def load_pickle(filepath: Path) -> Any:
        try:
            async with aiofiles.open(filepath, "rb") as file:
                content = await file.read()
                data = await asyncio.to_thread(pickle.loads, content)
            logger.debug(f"Loaded pickle data from {filepath}")
            return data
        except Exception as e:
            error_msg = f"Failed to load pickle from {filepath}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e

    @staticmethod
    async def save_pickle(data: Any, filepath: Path):
        try:
            pickled_data = await asyncio.to_thread(
                pickle.dumps, data, protocol=pickle.HIGHEST_PROTOCOL
            )
            async with aiofiles.open(filepath, "wb") as file:
                await file.write(pickled_data)
            logger.debug(f"Saved pickle data to {filepath}")
        except Exception as e:
            error_msg = f"Failed to save pickle to {filepath}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg) from e
