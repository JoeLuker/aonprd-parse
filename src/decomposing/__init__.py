# src/decomposing/__init__.py

"""
Decomposing Package

Handles the decomposition of HTML data into graph structures.
"""

from .decomposer import Decomposer, main as decomposer_main
from .condense_decomposition import Condenser, main as condense_decomposition_main

__all__ = ["Decomposer", "Condenser", "condense_decomposition_main", "decomposer_main"]
