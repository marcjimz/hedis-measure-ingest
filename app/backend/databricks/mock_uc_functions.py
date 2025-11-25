"""
Mock Unity Catalog Functions

Mock implementations for local testing without Databricks.
"""

from typing import List, Dict, Any, Optional


class MockUCFunctionsService:
    """Mock implementation of Unity Catalog functions."""

    MOCK_MEASURES = {
        "BCS": {
            "acronym": "BCS",
            "name": "Breast Cancer Screening",
            "year": 2025,
            "description": "Percentage of women aged 50-74 who had a mammogram to screen for breast cancer in the past 2 years.",
            "initial_population": "Women aged 50-74",
            "denominator": "Women aged 50-74 enrolled for at least 2 years",
            "numerator": "Women who had a mammogram in the past 2 years",
            "exclusions": ["Bilateral mastectomy", "History of breast cancer"]
        },
        "COL": {
            "acronym": "COL",
            "name": "Colorectal Cancer Screening",
            "year": 2025,
            "description": "Percentage of adults aged 45-75 who had appropriate colorectal cancer screening.",
            "initial_population": "Adults aged 45-75",
            "denominator": "Adults aged 45-75 enrolled for required period",
            "numerator": "Adults with colonoscopy, FIT, or CT colonography",
            "exclusions": ["Colorectal cancer history", "Total colectomy"]
        },
        "HBD": {
            "acronym": "HBD",
            "name": "Hemoglobin A1c Control for Patients With Diabetes",
            "year": 2025,
            "description": "Percentage of patients with diabetes who had HbA1c testing and control.",
            "initial_population": "Patients with diabetes aged 18-75",
            "denominator": "Patients with diabetes diagnosis",
            "numerator": "Patients with HbA1c < 8.0%",
            "exclusions": ["Polycystic ovarian syndrome only", "Gestational diabetes only"]
        }
    }

    MOCK_CHUNKS = [
        {
            "content": "The BCS measure requires a mammogram within the past 2 years for women aged 50-74.",
            "measure": "BCS",
            "year": 2025,
            "score": 0.95
        },
        {
            "content": "Exclusions for BCS include bilateral mastectomy and history of breast cancer.",
            "measure": "BCS",
            "year": 2025,
            "score": 0.90
        },
        {
            "content": "COL screening can be performed via colonoscopy, FIT test, or CT colonography.",
            "measure": "COL",
            "year": 2025,
            "score": 0.92
        },
        {
            "content": "The COL measure applies to adults aged 45-75 years.",
            "measure": "COL",
            "year": 2025,
            "score": 0.88
        },
        {
            "content": "HbA1c testing should be performed at least annually for diabetes patients.",
            "measure": "HBD",
            "year": 2025,
            "score": 0.94
        }
    ]

    def __init__(self):
        """Initialize mock UC functions service."""
        pass

    async def measures_definition_lookup(
        self,
        acronym: str,
        year: int = 2025
    ) -> Optional[Dict[str, Any]]:
        """Look up measure definition by acronym."""
        measure = self.MOCK_MEASURES.get(acronym.upper())
        if measure and measure.get("year") == year:
            return measure
        return None

    async def measures_document_search(
        self,
        query: str,
        num_results: int = 5,
        filter_year: Optional[int] = None,
        filter_measure: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search for relevant document chunks."""
        # Filter chunks based on criteria
        results = []

        query_lower = query.lower()

        for chunk in self.MOCK_CHUNKS:
            # Filter by year
            if filter_year and chunk.get("year") != filter_year:
                continue

            # Filter by measure
            if filter_measure and chunk.get("measure") != filter_measure.upper():
                continue

            # Simple keyword matching
            chunk_content_lower = chunk["content"].lower()
            if any(word in chunk_content_lower for word in query_lower.split()):
                results.append(chunk)

        # Sort by score and return top results
        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        return results[:num_results]

    async def measures_search_expansion(
        self,
        query: str,
        num_expansions: int = 3
    ) -> List[str]:
        """Generate query expansions for better search."""
        # Simple keyword-based expansions
        query_lower = query.lower()

        expansions = []

        if "bcs" in query_lower or "breast" in query_lower:
            expansions = ["mammogram", "breast cancer screening", "bilateral mastectomy"]
        elif "col" in query_lower or "colorectal" in query_lower:
            expansions = ["colonoscopy", "FIT test", "colon cancer screening"]
        elif "hbd" in query_lower or "diabetes" in query_lower:
            expansions = ["HbA1c", "hemoglobin A1c", "diabetes control"]
        else:
            expansions = ["HEDIS measure", "clinical criteria", "quality measure"]

        return expansions[:num_expansions]

    async def enhanced_document_search(
        self,
        query: str,
        num_results: int = 10,
        use_expansion: bool = True
    ) -> List[Dict[str, Any]]:
        """Perform enhanced search with query expansion."""
        results = []

        # Original query search
        results.extend(await self.measures_document_search(query, num_results=num_results))

        # Expanded queries
        if use_expansion:
            expansions = await self.measures_search_expansion(query)
            for expansion in expansions:
                expanded_results = await self.measures_document_search(
                    expansion,
                    num_results=num_results // 2
                )
                results.extend(expanded_results)

        # Deduplicate and sort by score
        seen = set()
        unique_results = []
        for result in results:
            content = result["content"]
            if content not in seen:
                seen.add(content)
                unique_results.append(result)

        unique_results.sort(key=lambda x: x.get("score", 0), reverse=True)
        return unique_results[:num_results]
