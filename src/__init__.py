# src/__init__.py

"""
aonprd-parse - Source Package

This package contains modules for cleaning, parsing, deduplicating HTML data,
and importing it into Memgraph.
"""

# Optional: Define package version or metadata
__version__ = "1.0.0"

# Optional: Import submodules for easier access
from .cleaning import manual_cleaning, cleaner
from .decomposing import decomposer, condense_decomposition
from .importing import csv_prep
from .processing import unwrap
from .utils import logging, data_handling, file_operations

__all__ = ['manual_cleaning', 'cleaner', 'decomposer', 'condense_decomposition', 'csv_prep', 'unwrap', 'logging', 'data_handling', 'file_operations']
