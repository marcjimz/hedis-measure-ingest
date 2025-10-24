"""
LLM Extraction Module for HEDIS Measures

Handles structured extraction of HEDIS measure definitions using LLM APIs.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Optional, List
import mlflow.deployments

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from agents.prompts.hedis_prompts import (
    MEASURE_EXTRACTION_PROMPT,
    MEASURE_EXTRACTION_SCHEMA
)

logger = logging.getLogger(__name__)


class LLMExtractor:
    """LLM-based extraction for HEDIS measure definitions"""

    def __init__(self, model_endpoint: str, temperature: float = 0.0):
        """
        Initialize LLM extractor

        Args:
            model_endpoint: Databricks model serving endpoint name
            temperature: LLM temperature (0.0 for deterministic output)

        Example:
            >>> extractor = LLMExtractor("databricks-meta-llama-3-3-70b-instruct")
        """
        self.model_endpoint = model_endpoint
        self.temperature = temperature
        self.client = mlflow.deployments.get_deploy_client("databricks")
        logger.info(f"Initialized LLM extractor with endpoint: {model_endpoint}")

    def extract_measure_definition(
        self,
        text: str,
        effective_year: int = 2025
    ) -> Dict:
        """
        Extract structured measure definition from text

        Args:
            text: Extracted text from measure pages
            effective_year: HEDIS effective year

        Returns:
            Structured measure definition dictionary

        Raises:
            RuntimeError: If extraction fails or returns invalid JSON

        Example:
            >>> text = "AMM - Antidepressant Medication Management\\n\\nSpecifications: ..."
            >>> measure = extractor.extract_measure_definition(text)
            >>> print(measure['measure'])
            'AMM - Antidepressant Medication Management'
        """
        logger.info("Extracting measure definition with LLM...")

        try:
            # Format prompt with document text
            prompt = MEASURE_EXTRACTION_PROMPT.format(
                document_text=text[:15000]  # Limit to ~15K chars to fit context
            )

            # Prepare messages
            messages = [
                {
                    "role": "user",
                    "content": prompt
                }
            ]

            # Call LLM with JSON schema enforcement
            response = self.client.predict(
                endpoint=self.model_endpoint,
                inputs={
                    "messages": messages,
                    "temperature": self.temperature,
                    "max_tokens": 4096,
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": MEASURE_EXTRACTION_SCHEMA
                    }
                }
            )

            # Parse response
            content = response["choices"][0]["message"]["content"]
            measure_dict = json.loads(content)

            # Validate required fields
            if not measure_dict.get("Specifications") or not measure_dict.get("measure"):
                raise ValueError("Missing required fields: Specifications or measure")

            logger.info(f"Successfully extracted measure: {measure_dict.get('measure', 'Unknown')}")
            return measure_dict

        except Exception as e:
            logger.error(f"LLM extraction failed: {str(e)}")
            raise RuntimeError(f"Failed to extract measure definition: {str(e)}") from e

    def extract_with_retry(
        self,
        text: str,
        effective_year: int = 2025,
        max_retries: int = 3
    ) -> Dict:
        """
        Extract with retry logic for transient failures

        Args:
            text: Extracted text from measure pages
            effective_year: HEDIS effective year
            max_retries: Maximum number of retry attempts

        Returns:
            Structured measure definition dictionary
        """
        import time

        for attempt in range(max_retries):
            try:
                return self.extract_measure_definition(text, effective_year)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Extraction attempt {attempt + 1} failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
