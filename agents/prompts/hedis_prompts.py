"""
LLM Prompts for HEDIS Measure Extraction

This module contains all prompt templates for extracting structured
information from HEDIS measure documents.
"""

# Prompt for extracting measure definitions
MEASURE_EXTRACTION_PROMPT = """You are a HEDIS measure extraction specialist. Extract structured HEDIS measure information from the provided text with 100% accuracy.

**CRITICAL RULES:**
1. Extract EXACTLY what is written - do not paraphrase or summarize
2. Preserve all medical codes (ICD-10, CPT, HCPCS, LOINC, RxNorm) exactly
3. Maintain list structure for multi-component criteria
4. Do not add information not present in the source
5. If a field is not present, use null or empty array

**EXTRACTION REQUIREMENTS:**

1. **Measure Name and Specifications**
   - Extract full official measure description from NCQA
   - Identify standard acronym (e.g., "AMM" for Antidepressant Medication Management)

2. **Initial Population (Initial_Pop)**
   - Extract the initial population criteria exactly as written
   - Include age ranges, enrollment requirements, diagnosis criteria

3. **Denominator**
   - Extract denominator definition
   - List all denominator components
   - Automatically normalize to array format

4. **Numerator**
   - Extract numerator compliance criteria
   - List all conditions required for compliance
   - Preserve code value sets

5. **Exclusions**
   - Extract all denominator exclusion criteria
   - Include condition codes
   - Maintain exclusion logic

**OUTPUT FORMAT:**
Return ONLY valid JSON matching this exact schema:
{{
  "Specifications": "The official measure description from NCQA",
  "measure": "The official measure name with standard acronym",
  "Initial_Pop": "The initial identification of measure population",
  "denominator": ["List of all denominator components, automatically normalized"],
  "numerator": ["List of numerator details required for measure compliance"],
  "exclusion": ["List of all exclusion conditions included in this measure"],
  "effective_year": 2025
}}

**DOCUMENT TEXT:**
{document_text}
"""

# Prompt for extracting document metadata
METADATA_EXTRACTION_PROMPT = """You are a document metadata extraction specialist. Extract structured metadata from the HEDIS document.

**EXTRACTION REQUIREMENTS:**

1. **Document Title**
   - Extract the full official title
   - Include volume and year information

2. **Publication Date**
   - Extract publication date in YYYY-MM-DD format
   - Look for dates in headers, footers, or cover page

3. **Measurement Year**
   - Extract the HEDIS measurement year (e.g., 2025)
   - This is different from publication date

4. **Publisher**
   - Identify the publisher (typically NCQA)

5. **Volume Information**
   - Extract volume number and description

**OUTPUT FORMAT:**
Return ONLY valid JSON:
{{
  "title": "Full document title",
  "publication_date": "YYYY-MM-DD",
  "measurement_year": 2025,
  "publisher": "NCQA",
  "volume": "Volume number and description",
  "page_count": 750
}}

**DOCUMENT TEXT:**
{document_text}
"""

# JSON Schema for measure extraction
MEASURE_EXTRACTION_SCHEMA = {
    "name": "hedis_measure_extraction",
    "schema": {
        "type": "object",
        "properties": {
            "Specifications": {"type": "string"},
            "measure": {"type": "string"},
            "Initial_Pop": {"type": "string"},
            "denominator": {"type": "array", "items": {"type": "string"}},
            "numerator": {"type": "array", "items": {"type": "string"}},
            "exclusion": {"type": "array", "items": {"type": "string"}},
            "effective_year": {"type": "integer"}
        },
        "required": ["Specifications", "measure", "effective_year"],
        "additionalProperties": False
    }
}

# JSON Schema for metadata extraction
METADATA_EXTRACTION_SCHEMA = {
    "name": "hedis_metadata_extraction",
    "schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "publication_date": {"type": "string"},
            "measurement_year": {"type": "integer"},
            "publisher": {"type": "string"},
            "volume": {"type": "string"},
            "page_count": {"type": "integer"}
        },
        "required": ["measurement_year"],
        "additionalProperties": False
    }
}
