"""
Unity Catalog Functions Service

Service for calling Unity Catalog functions from the FastAPI backend.
Provides access to HEDIS-specific functions:
- measures_definition_lookup: Look up measure definitions
- measures_document_search: Semantic search over HEDIS documents
- measures_search_expansion: AI-powered query expansion
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession
from app.backend.config import settings

logger = logging.getLogger(__name__)


class UCFunctionsService:
    """
    Service for calling Unity Catalog functions.

    Provides typed methods for calling HEDIS-specific UC functions.
    """

    def __init__(self):
        """Initialize the UC functions service."""
        self.catalog = settings.uc_catalog
        self.schema = settings.uc_schema
        self.effective_year = settings.effective_year

        # Initialize Spark session
        try:
            self.spark = SparkSession.builder.getOrCreate()
            logger.info(f"Initialized UC functions service: {self.catalog}.{self.schema}")
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise

    def _get_function_name(self, function: str) -> str:
        """Get fully qualified function name."""
        return f"{self.catalog}.{self.schema}.{function}"

    async def measures_definition_lookup(
        self,
        measure_acronym: str,
        year: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Look up a HEDIS measure definition by acronym.

        Args:
            measure_acronym: Measure acronym (e.g., "CWP", "BCS")
            year: Optional year filter (defaults to effective_year from config)

        Returns:
            Dictionary with measure definition or None if not found
        """
        try:
            year = year or self.effective_year
            function_name = self._get_function_name("measures_definition_lookup")

            result = self.spark.sql(f"""
                SELECT *
                FROM {function_name}('{measure_acronym}', {year})
            """)

            if result.count() == 0:
                logger.warning(f"No measure found for acronym: {measure_acronym}, year: {year}")
                return None

            row = result.first()
            return {
                "measure_id": row.measure_id,
                "measure_acronym": row.measure_acronym,
                "measure": row.measure,
                "specifications": row.specifications,
                "initial_pop": row.initial_pop,
                "denominator": row.denominator,
                "numerator": row.numerator,
                "exclusion": row.exclusion,
                "effective_year": row.effective_year,
                "page_start": row.page_start,
                "page_end": row.page_end,
                "file_name": row.file_name
            }

        except Exception as e:
            logger.error(f"Error calling measures_definition_lookup: {e}", exc_info=True)
            return None

    async def measures_document_search(
        self,
        search_query: str,
        num_results: int = 5,
        filter_year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform semantic search over HEDIS document chunks.

        Args:
            search_query: Natural language search query
            num_results: Number of results to return (default: 5)
            filter_year: Optional year filter (defaults to effective_year)

        Returns:
            List of matching document chunks with metadata
        """
        try:
            filter_year = filter_year or self.effective_year
            function_name = self._get_function_name("measures_document_search")

            result = self.spark.sql(f"""
                SELECT *
                FROM {function_name}('{search_query}', {num_results}, {filter_year})
            """)

            chunks = []
            for row in result.collect():
                chunks.append({
                    "chunk_id": row.chunk_id,
                    "chunk_content": row.chunk_content,
                    "page_start": row.page_start,
                    "page_end": row.page_end,
                    "effective_year": row.effective_year,
                    "measure_name": row.measure_name,
                    "score": float(row.score)
                })

            logger.info(f"Document search returned {len(chunks)} results for query: {search_query[:50]}")
            return chunks

        except Exception as e:
            logger.error(f"Error calling measures_document_search: {e}", exc_info=True)
            return []

    async def measures_search_expansion(
        self,
        query_term: str,
        num_expansions: int = 3
    ) -> List[str]:
        """
        Generate search query expansions using AI.

        Args:
            query_term: Original search term
            num_expansions: Number of expansions to generate (default: 3)

        Returns:
            List of expanded query strings
        """
        try:
            function_name = self._get_function_name("measures_search_expansion")

            result = self.spark.sql(f"""
                SELECT expanded_query
                FROM {function_name}('{query_term}', {num_expansions})
                ORDER BY expansion_id
            """)

            expansions = [row.expanded_query for row in result.collect()]
            logger.info(f"Generated {len(expansions)} query expansions for: {query_term}")
            return expansions

        except Exception as e:
            logger.error(f"Error calling measures_search_expansion: {e}", exc_info=True)
            return []

    async def enhanced_document_search(
        self,
        query: str,
        num_results: int = 5,
        use_expansion: bool = True,
        filter_year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Enhanced document search with optional query expansion.

        Combines:
        1. Original query search
        2. AI-generated query expansions (if enabled)
        3. Deduplication and ranking

        Args:
            query: Search query
            num_results: Number of final results
            use_expansion: Whether to use AI query expansion
            filter_year: Optional year filter

        Returns:
            List of top matching document chunks
        """
        try:
            all_results = {}

            # Search with original query
            original_results = await self.measures_document_search(
                query, num_results * 2, filter_year
            )
            for result in original_results:
                chunk_id = result["chunk_id"]
                if chunk_id not in all_results or result["score"] > all_results[chunk_id]["score"]:
                    all_results[chunk_id] = result

            # Search with expanded queries if enabled
            if use_expansion:
                expansions = await self.measures_search_expansion(query, 3)
                for expanded_query in expansions:
                    expanded_results = await self.measures_document_search(
                        expanded_query, num_results, filter_year
                    )
                    for result in expanded_results:
                        chunk_id = result["chunk_id"]
                        if chunk_id not in all_results or result["score"] > all_results[chunk_id]["score"]:
                            all_results[chunk_id] = result

            # Sort by score and return top N
            sorted_results = sorted(
                all_results.values(),
                key=lambda x: x["score"],
                reverse=True
            )[:num_results]

            logger.info(f"Enhanced search returned {len(sorted_results)} results")
            return sorted_results

        except Exception as e:
            logger.error(f"Error in enhanced_document_search: {e}", exc_info=True)
            return []

    async def health_check(self) -> bool:
        """
        Check if UC functions are accessible.

        Returns:
            True if functions are accessible, False otherwise
        """
        try:
            # Try a simple function call
            function_name = self._get_function_name("measures_definition_lookup")
            self.spark.sql(f"SELECT * FROM {function_name}('CWP', {self.effective_year}) LIMIT 1")
            return True
        except Exception as e:
            logger.error(f"UC functions health check failed: {e}", exc_info=True)
            return False
