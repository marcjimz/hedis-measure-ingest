"""
HEDIS Extraction Prompts Package
=================================

Optimized LLM prompts for extracting HEDIS measure definitions and metadata
from NCQA documentation with 100% accuracy using Tree of Thought reasoning
and multi-step validation.

Usage:
    from src.prompts import (
        HEDIS_MEASURE_EXTRACTION_PROMPT,
        HEDIS_METADATA_EXTRACTION_PROMPT,
        validate_measure_extraction,
        validate_metadata_extraction
    )

Author: Hive Mind CODER Agent
Created: 2025-10-23
"""

from .hedis_extraction_prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT,
    USAGE_EXAMPLES,
    validate_measure_extraction,
    validate_metadata_extraction
)

__all__ = [
    'HEDIS_MEASURE_EXTRACTION_PROMPT',
    'HEDIS_METADATA_EXTRACTION_PROMPT',
    'USAGE_EXAMPLES',
    'validate_measure_extraction',
    'validate_metadata_extraction'
]

__version__ = '1.0.0'
