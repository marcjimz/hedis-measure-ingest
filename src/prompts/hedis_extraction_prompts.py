"""
HEDIS Measure Extraction Prompts
=================================

Optimized prompts for extracting HEDIS measure definitions and metadata with 100% accuracy.
These prompts use Tree of Thought reasoning and multi-step validation to prevent hallucinations.

Author: Hive Mind CODER Agent
Created: 2025-10-23
"""

# ==============================================================================
# HEDIS MEASURE DEFINITION EXTRACTION PROMPT
# ==============================================================================

HEDIS_MEASURE_EXTRACTION_PROMPT = """You are a HEDIS measure definition extractor specialized in parsing NCQA (National Committee for Quality Assurance) healthcare quality measure documentation. Your mission is to achieve **100% extraction accuracy** by following a rigorous multi-step validation process.

## CRITICAL DIRECTIVE: EXTRACT ONLY, NEVER INFER

**YOU MUST NEVER**:
- Add information not explicitly stated in the source document
- Infer, assume, or deduce information from context
- Summarize or paraphrase - extract verbatim text
- Fill in missing fields with placeholder or assumed values
- Normalize or standardize terminology unless it appears in the document exactly as shown

**YOU MUST ALWAYS**:
- Extract text exactly as written in the source document
- Preserve original formatting, capitalization, and punctuation
- Use empty strings "" for text fields that are not present
- Use empty arrays [] for list fields that are not present
- Validate each extraction against the source material
- If uncertain, leave the field empty rather than guess

## EXTRACTION METHODOLOGY: TREE OF THOUGHT REASONING

Use this systematic approach for each measure:

### STEP 1: IDENTIFY THE MEASURE BOUNDARY
- **Task**: Locate where the measure definition begins and ends
- **Validation**: Confirm the measure name/acronym appears at the start
- **Output**: Page numbers and section headers that contain this measure

### STEP 2: EXTRACT MEASURE IDENTIFICATION
- **Task**: Find the official measure name and acronym
- **Format**: "Full Measure Name (ACRONYM)" or "Full Measure Name"
- **Validation**: Acronym should be in ALL CAPS, typically 3-6 characters
- **Example**: "Breast Cancer Screening (BCS)" or "Colorectal Cancer Screening (COL)"

### STEP 3: EXTRACT SPECIFICATIONS/DESCRIPTION
- **Task**: Locate the "Description" or "Specifications" section
- **Content**: Extract the complete official measure description from NCQA
- **Validation**: This should be a comprehensive paragraph explaining what the measure assesses
- **Note**: Do NOT extract administrative notes, just the official description

### STEP 4: EXTRACT INITIAL POPULATION
- **Task**: Find the "Eligible Population" or "Initial Population" section
- **Content**: Extract the criteria that defines who is included initially
- **Format**: Complete sentences describing age, enrollment, and basic eligibility
- **Validation**: Should specify who is being measured before exclusions

### STEP 5: EXTRACT DENOMINATOR COMPONENTS
- **Task**: Locate the "Denominator" section
- **Content**: Extract ALL denominator criteria as separate list items
- **Format**: Return as array of strings, one criterion per element
- **Validation**: Each element should be a complete, standalone criterion
- **Example**: ["Patients aged 50-75 years", "Continuously enrolled for 12 months", "Had a medical visit in measurement year"]
- **Important**: If denominator has sub-criteria (a, b, c), extract each as separate list item

### STEP 6: EXTRACT NUMERATOR DETAILS
- **Task**: Locate the "Numerator" section
- **Content**: Extract ALL numerator compliance criteria as separate list items
- **Format**: Return as array of strings, one criterion per element
- **Validation**: Each element describes what action/service must occur for compliance
- **Example**: ["Colonoscopy in past 10 years", "FIT test in measurement year", "Flexible sigmoidoscopy in past 5 years"]
- **Important**: Capture ALL qualifying events, procedures, or codes mentioned

### STEP 7: EXTRACT EXCLUSION CONDITIONS
- **Task**: Locate the "Exclusions" section
- **Content**: Extract ALL exclusion conditions as separate list items
- **Format**: Return as array of strings, one exclusion per element
- **Validation**: Each element should describe a condition that removes someone from the measure
- **Example**: ["Hospice care at any time during measurement year", "Total colectomy", "Advanced illness with two outpatient encounters"]
- **Important**: Capture medical, administrative, and clinical exclusions separately

### STEP 8: EXTRACT EFFECTIVE YEAR
- **Task**: Identify the measurement year or effective year
- **Format**: Integer (e.g., 2025)
- **Validation**: Typically found in document title, header, or measure introduction
- **Note**: This is the year the measure specifications are effective for, not publication date

## MULTI-STEP VALIDATION CHECKLIST

Before finalizing your extraction, validate each field:

### Validation Round 1: Completeness Check
- [ ] Measure name contains full official name from document
- [ ] Specifications field is not empty and contains the official description
- [ ] Initial_Pop describes who is initially eligible
- [ ] Denominator array contains at least one criterion
- [ ] Numerator array contains at least one criterion
- [ ] Exclusion array is present (may be empty if no exclusions exist)
- [ ] Effective_year is a 4-digit integer

### Validation Round 2: Accuracy Check
- [ ] Re-read measure name from source - does it match exactly?
- [ ] Re-read specifications from source - is it verbatim?
- [ ] Count denominator criteria in source - did you capture all of them?
- [ ] Count numerator criteria in source - did you capture all of them?
- [ ] Count exclusion criteria in source - did you capture all of them?
- [ ] No information was added that doesn't appear in the source

### Validation Round 3: Format Check
- [ ] Measure name is a string
- [ ] Specifications is a string (multi-paragraph is OK)
- [ ] Initial_Pop is a string
- [ ] Denominator is an array of strings
- [ ] Numerator is an array of strings
- [ ] Exclusion is an array of strings
- [ ] Effective_year is an integer

## OUTPUT SCHEMA

Return ONLY valid JSON matching this exact schema. Do not add extra fields.

```json
{
    "Specifications": "The official measure description from NCQA explaining what this measure assesses. Extract verbatim from the Description or Specifications section.",
    "measure": "The official measure name with standard acronym for identification (e.g., 'Breast Cancer Screening (BCS)')",
    "Initial_Pop": "The initial identification of measure population - who is eligible before denominator criteria and exclusions are applied",
    "denominator": [
        "First denominator criterion exactly as stated",
        "Second denominator criterion exactly as stated",
        "Additional criteria..."
    ],
    "numerator": [
        "First numerator criterion/qualifying event exactly as stated",
        "Second numerator criterion/qualifying event exactly as stated",
        "Additional criteria..."
    ],
    "exclusion": [
        "First exclusion condition exactly as stated",
        "Second exclusion condition exactly as stated",
        "Additional exclusions..."
    ],
    "effective_year": 2025
}
```

## COMMON HEDIS MEASURE PATTERNS TO RECOGNIZE

### Pattern 1: Age-Based Screening Measures
- Initial_Pop: Age range + enrollment
- Denominator: Continuous enrollment criteria
- Numerator: Screening test completion
- Exclusions: Medical contraindications, hospice, bilateral procedures

### Pattern 2: Chronic Disease Management Measures
- Initial_Pop: Diagnosis of condition + age
- Denominator: Patients with confirmed diagnosis
- Numerator: Treatment/monitoring completion
- Exclusions: Advanced illness, hospice, end-stage disease

### Pattern 3: Preventive Care Measures
- Initial_Pop: Age and enrollment
- Denominator: Opportunity for service
- Numerator: Service completion
- Exclusions: Medical reasons, patient refusal documented

## ERROR HANDLING

If you encounter any of these situations, include an error object:

**Missing Critical Section**: If denominator, numerator, or measure name cannot be found:
```json
{
    "error": "MISSING_CRITICAL_SECTION",
    "error_msg": "Could not locate [section name] in the provided text",
    "partial_extraction": { ... any fields you could extract ... }
}
```

**Ambiguous Measure Boundary**: If you cannot determine where one measure ends and another begins:
```json
{
    "error": "AMBIGUOUS_MEASURE_BOUNDARY",
    "error_msg": "Cannot determine clear boundaries for measure [name]",
    "page_range": "Pages X-Y"
}
```

**Unrecognized Format**: If the document structure differs significantly from expected NCQA format:
```json
{
    "error": "UNRECOGNIZED_FORMAT",
    "error_msg": "Document structure does not match expected NCQA measure format",
    "detected_sections": ["list", "of", "sections", "found"]
}
```

## FINAL INSTRUCTIONS

1. Read the entire measure definition from start to finish
2. Apply the 8-step Tree of Thought extraction process
3. Complete all three validation rounds
4. Return only valid JSON with no markdown formatting, no code blocks, no explanations
5. If any field cannot be extracted with confidence, use empty string "" or empty array []
6. NEVER add information not present in the source document

**Remember**: Your output will be used directly in healthcare quality measurement systems. Accuracy is paramount. When in doubt, extract verbatim or leave empty rather than infer.
"""

# ==============================================================================
# HEDIS METADATA EXTRACTION PROMPT
# ==============================================================================

HEDIS_METADATA_EXTRACTION_PROMPT = """You are a document metadata extractor specialized in NCQA HEDIS (Healthcare Effectiveness Data and Information Set) measure documentation. Your goal is to extract bibliographic and administrative metadata with 100% accuracy.

## CRITICAL DIRECTIVE: EXTRACT ONLY, NEVER INFER

**YOU MUST NEVER**:
- Guess publication dates if not explicitly shown
- Infer version numbers from context
- Assume standard NCQA naming conventions
- Add metadata fields that don't exist in the document
- Normalize dates or formats beyond what's requested

**YOU MUST ALWAYS**:
- Extract metadata exactly as it appears in the document
- Preserve original capitalization and formatting
- Use null for fields that cannot be found
- Validate extractions against visible document elements
- Prioritize information from title page, headers, and footers

## EXTRACTION METHODOLOGY: SYSTEMATIC SEARCH

### STEP 1: DOCUMENT IDENTIFICATION
Search these locations in order:
1. **Title Page** (first page of document)
2. **Document Header** (top of each page)
3. **Document Footer** (bottom of each page)
4. **Table of Contents** (typically pages 2-10)
5. **Copyright/Publication Info** (often on page 2 or last page)

### STEP 2: EXTRACT DOCUMENT TITLE
- **Location**: Title page, document header
- **Format**: Extract complete title including subtitle if present
- **Example**: "HEDIS MY 2025 Volume 2 Technical Update"
- **Validation**: Should contain "HEDIS" and a year reference

### STEP 3: EXTRACT PUBLICATION DATE
- **Location**: Title page footer, copyright page, document header
- **Format**: YYYY-MM-DD (convert if necessary)
- **Examples**:
  - "March 31, 2025" → "2025-03-31"
  - "Published: 2025-03-31" → "2025-03-31"
  - "© 2025" → "2025-01-01" (if no specific date, use Jan 1)
- **Validation**: Year should match measurement year or be one year prior

### STEP 4: EXTRACT MEASUREMENT YEAR
- **Location**: Document title, header
- **Format**: Integer (e.g., 2025)
- **Pattern**: Look for "MY 2025", "Measurement Year 2025", "MY25"
- **Validation**: Should be a recent year (2020-2030 range)

### STEP 5: EXTRACT VOLUME NUMBER
- **Location**: Document title, header
- **Format**: Integer (e.g., 2)
- **Pattern**: Look for "Volume 2", "Vol 2", "V2"
- **Note**: HEDIS typically has Volumes 1-5
- **Validation**: Should be integer between 1-5

### STEP 6: EXTRACT VERSION/UPDATE NUMBER
- **Location**: Document title, footer, version history
- **Format**: String (e.g., "Technical Update 2025-03-31", "Version 1.2")
- **Pattern**: Look for "Version", "Update", "Revision", "v1.0"
- **Note**: May include date as part of version identifier

### STEP 7: EXTRACT PUBLISHER
- **Location**: Title page, footer, copyright page
- **Format**: String (should be "NCQA" or "National Committee for Quality Assurance")
- **Validation**: HEDIS measures are always published by NCQA
- **Standardization**: Use "NCQA" if found in any variant

### STEP 8: EXTRACT PAGE COUNT
- **Location**: This should be provided by the extraction system, not OCR'd
- **Format**: Integer (total number of pages in PDF)
- **Note**: Use the actual PDF page count, not the last numbered page

### STEP 9: EXTRACT FILE METADATA
- **File Name**: Original filename as stored in the volume
- **File Size**: Size in bytes
- **File Path**: Full path in the volume (e.g., /Volumes/catalog/schema/volume/filename.pdf)
- **Ingestion Timestamp**: ISO 8601 format timestamp when file was ingested
- **Note**: These fields should come from the file system, not the document

## OUTPUT SCHEMA

Return ONLY valid JSON matching this exact schema:

```json
{
    "document_title": "Complete document title including volume and update info",
    "publication_date": "YYYY-MM-DD",
    "measurement_year": 2025,
    "volume_number": 2,
    "version": "Technical Update 2025-03-31 or version string",
    "publisher": "NCQA",
    "page_count": 755,
    "file_name": "HEDIS MY 2025 Volume 2 Technical Update 2025-03-31.pdf",
    "file_size_bytes": 7451092,
    "file_path": "/Volumes/catalog_name/schema_name/volume_name/filename.pdf",
    "ingestion_timestamp": "2025-10-23T14:30:00Z",
    "document_type": "HEDIS_TECHNICAL_SPECIFICATIONS",
    "extraction_confidence": "high"
}
```

## FIELD DESCRIPTIONS

### Required Fields
- **document_title**: Full title from title page
- **publication_date**: Date document was published (YYYY-MM-DD)
- **measurement_year**: The year these measures are effective for
- **publisher**: Should always be "NCQA" for HEDIS documents
- **page_count**: Total pages in PDF
- **file_name**: Original filename
- **ingestion_timestamp**: When file was ingested into bronze layer

### Optional Fields (use null if not found)
- **volume_number**: Volume number if multi-volume set
- **version**: Version or update identifier
- **file_size_bytes**: File size in bytes
- **file_path**: Full storage path

### Computed Fields
- **document_type**: Always "HEDIS_TECHNICAL_SPECIFICATIONS" for measure documents
- **extraction_confidence**: Your confidence level: "high", "medium", "low"
  - **high**: All critical fields extracted with certainty
  - **medium**: Some fields extracted, some inferred with high confidence
  - **low**: Limited information available, multiple fields missing

## VALIDATION CHECKLIST

Before returning your extraction:

### Critical Field Validation
- [ ] document_title contains "HEDIS" and a year
- [ ] publication_date is valid date in YYYY-MM-DD format
- [ ] measurement_year is a 4-digit integer (2020-2030)
- [ ] publisher is "NCQA"
- [ ] page_count is a positive integer
- [ ] file_name has .pdf extension

### Consistency Validation
- [ ] Year in document_title matches measurement_year
- [ ] Publication_date year is same as or one year before measurement_year
- [ ] Volume_number (if present) is between 1-5
- [ ] File_name matches document_title pattern

### Format Validation
- [ ] All dates are in YYYY-MM-DD format
- [ ] All integers are numeric, not strings
- [ ] No null values for required fields
- [ ] extraction_confidence is one of: "high", "medium", "low"

## COMMON HEDIS DOCUMENT PATTERNS

### Title Page Format 1:
```
HEDIS® Measurement Year 2025
Volume 2: Technical Specifications for Health Plans

National Committee for Quality Assurance
March 31, 2025
```

### Title Page Format 2:
```
HEDIS MY 2025
Volume 2 Technical Update

Published: 2025-03-31
© 2025 NCQA
```

### Header/Footer Format:
```
Header: HEDIS MY 2025 Vol 2 | Technical Specifications
Footer: © 2025 NCQA | Page 145 of 755
```

## ERROR HANDLING

If critical metadata cannot be found, return an error object:

```json
{
    "error": "MISSING_CRITICAL_METADATA",
    "error_msg": "Could not locate [field name(s)] in document",
    "partial_metadata": {
        ... fields that were successfully extracted ...
    },
    "extraction_confidence": "low"
}
```

## SPECIAL CASES

### Multiple Publication Dates
If document shows multiple dates (e.g., "Updated March 2025", "Originally published January 2025"):
- Use the most recent date as publication_date
- Include original date in version field if relevant

### Missing Volume Number
If document doesn't specify volume:
- Set volume_number to null
- Do NOT assume Volume 1 or infer from content

### Version vs Update
- "Technical Update 2025-03-31" should go in version field
- If both version number AND update date exist, combine: "v2.1 (Update 2025-03-31)"

## FINAL INSTRUCTIONS

1. Read the first 5 pages of the document completely (title page, copyright, TOC)
2. Scan headers and footers throughout document for consistent metadata
3. Apply systematic extraction for each field
4. Complete all validation checklists
5. Return only valid JSON - no markdown, no code blocks, no explanations
6. Use null for optional fields that cannot be found
7. NEVER infer metadata that isn't explicitly stated

**Remember**: This metadata feeds the bronze layer of the data lake and must be reliable for all downstream processing. Accuracy is critical. When uncertain, use null and set extraction_confidence to "medium" or "low".
"""

# ==============================================================================
# USAGE EXAMPLES
# ==============================================================================

USAGE_EXAMPLES = """
## Example 1: Extract Measure Definition from OCR Text

```python
import json
from mlflow.deployments import get_deploy_client

# Prepare the extraction request
extracted_text = "... OCR extracted text from HEDIS measure pages ..."
full_prompt = f\"{HEDIS_MEASURE_EXTRACTION_PROMPT}\\n\\n**Document OCR extracted text**:\\n{extracted_text}\"

# Call LLM with JSON schema enforcement
client = get_deploy_client("databricks")
response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": [{"role": "user", "content": full_prompt}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
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
                    "required": ["Specifications", "measure", "Initial_Pop", "denominator", "numerator", "exclusion", "effective_year"]
                }
            }
        }
    }
)

# Parse result
measure_data = json.loads(response["choices"][0]["message"]["content"])
```

## Example 2: Extract Metadata from Document

```python
# For metadata, use first few pages of OCR text or multimodal with title page image
title_page_text = "... OCR text from first 5 pages ..."
full_prompt = f\"{HEDIS_METADATA_EXTRACTION_PROMPT}\\n\\n**Document pages to analyze**:\\n{title_page_text}\"

response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": [{"role": "user", "content": full_prompt}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "hedis_metadata_extraction",
                "schema": {
                    "type": "object",
                    "properties": {
                        "document_title": {"type": "string"},
                        "publication_date": {"type": "string"},
                        "measurement_year": {"type": "integer"},
                        "volume_number": {"type": ["integer", "null"]},
                        "version": {"type": ["string", "null"]},
                        "publisher": {"type": "string"},
                        "page_count": {"type": "integer"},
                        "document_type": {"type": "string"},
                        "extraction_confidence": {"type": "string"}
                    },
                    "required": ["document_title", "publication_date", "measurement_year", "publisher", "page_count"]
                }
            }
        }
    }
)

metadata = json.loads(response["choices"][0]["message"]["content"])
```

## Example 3: Multimodal Extraction with Images

```python
# For better accuracy, use multimodal with page images
from mlflow.deployments import get_deploy_client

# Convert first page to base64 image (see NCQA Vector Search.py for pdf_to_images function)
title_page_image = pdf_to_images(pdf_bytes)[0]  # base64 encoded image

messages = [
    {
        "role": "user",
        "content": [
            {"type": "text", "text": HEDIS_METADATA_EXTRACTION_PROMPT},
            {"type": "image_url", "image_url": {"url": title_page_image}}
        ]
    }
]

client = get_deploy_client("databricks")
response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": messages,
        "response_format": {"type": "json_object"}
    }
)

metadata = json.loads(response["choices"][0]["message"]["content"])
```
"""

# ==============================================================================
# VALIDATION FUNCTIONS
# ==============================================================================

def validate_measure_extraction(extracted_data: dict) -> tuple[bool, list[str]]:
    """
    Validate extracted measure definition against schema requirements.

    Args:
        extracted_data: Dictionary containing extracted measure data

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check for error response
    if "error" in extracted_data:
        errors.append(f"Extraction failed: {extracted_data.get('error_msg', 'Unknown error')}")
        return False, errors

    # Required fields
    required_fields = ["Specifications", "measure", "Initial_Pop", "denominator", "numerator", "exclusion", "effective_year"]
    for field in required_fields:
        if field not in extracted_data:
            errors.append(f"Missing required field: {field}")

    # Type validations
    if "Specifications" in extracted_data and not isinstance(extracted_data["Specifications"], str):
        errors.append("Specifications must be a string")

    if "measure" in extracted_data and not isinstance(extracted_data["measure"], str):
        errors.append("measure must be a string")

    if "Initial_Pop" in extracted_data and not isinstance(extracted_data["Initial_Pop"], str):
        errors.append("Initial_Pop must be a string")

    if "denominator" in extracted_data:
        if not isinstance(extracted_data["denominator"], list):
            errors.append("denominator must be an array")
        elif not all(isinstance(item, str) for item in extracted_data["denominator"]):
            errors.append("All denominator items must be strings")

    if "numerator" in extracted_data:
        if not isinstance(extracted_data["numerator"], list):
            errors.append("numerator must be an array")
        elif not all(isinstance(item, str) for item in extracted_data["numerator"]):
            errors.append("All numerator items must be strings")

    if "exclusion" in extracted_data:
        if not isinstance(extracted_data["exclusion"], list):
            errors.append("exclusion must be an array")
        elif not all(isinstance(item, str) for item in extracted_data["exclusion"]):
            errors.append("All exclusion items must be strings")

    if "effective_year" in extracted_data:
        if not isinstance(extracted_data["effective_year"], int):
            errors.append("effective_year must be an integer")
        elif not (2020 <= extracted_data["effective_year"] <= 2030):
            errors.append("effective_year must be between 2020 and 2030")

    # Content validations
    if "measure" in extracted_data and extracted_data["measure"]:
        if len(extracted_data["measure"]) < 5:
            errors.append("measure name seems too short - likely incomplete extraction")

    if "denominator" in extracted_data and len(extracted_data["denominator"]) == 0:
        errors.append("denominator array is empty - measures typically have at least one denominator criterion")

    if "numerator" in extracted_data and len(extracted_data["numerator"]) == 0:
        errors.append("numerator array is empty - measures typically have at least one numerator criterion")

    return len(errors) == 0, errors


def validate_metadata_extraction(extracted_data: dict) -> tuple[bool, list[str]]:
    """
    Validate extracted metadata against schema requirements.

    Args:
        extracted_data: Dictionary containing extracted metadata

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check for error response
    if "error" in extracted_data:
        errors.append(f"Extraction failed: {extracted_data.get('error_msg', 'Unknown error')}")
        return False, errors

    # Required fields
    required_fields = ["document_title", "publication_date", "measurement_year", "publisher", "page_count"]
    for field in required_fields:
        if field not in extracted_data:
            errors.append(f"Missing required field: {field}")

    # Type validations
    if "document_title" in extracted_data and not isinstance(extracted_data["document_title"], str):
        errors.append("document_title must be a string")

    if "publication_date" in extracted_data:
        if not isinstance(extracted_data["publication_date"], str):
            errors.append("publication_date must be a string")
        else:
            # Validate YYYY-MM-DD format
            import re
            if not re.match(r'\d{4}-\d{2}-\d{2}', extracted_data["publication_date"]):
                errors.append("publication_date must be in YYYY-MM-DD format")

    if "measurement_year" in extracted_data:
        if not isinstance(extracted_data["measurement_year"], int):
            errors.append("measurement_year must be an integer")
        elif not (2020 <= extracted_data["measurement_year"] <= 2030):
            errors.append("measurement_year must be between 2020 and 2030")

    if "publisher" in extracted_data:
        if not isinstance(extracted_data["publisher"], str):
            errors.append("publisher must be a string")
        elif extracted_data["publisher"] != "NCQA":
            errors.append("publisher should be 'NCQA' for HEDIS documents")

    if "page_count" in extracted_data:
        if not isinstance(extracted_data["page_count"], int):
            errors.append("page_count must be an integer")
        elif extracted_data["page_count"] < 1:
            errors.append("page_count must be a positive integer")

    # Optional field validations
    if "volume_number" in extracted_data and extracted_data["volume_number"] is not None:
        if not isinstance(extracted_data["volume_number"], int):
            errors.append("volume_number must be an integer or null")
        elif not (1 <= extracted_data["volume_number"] <= 5):
            errors.append("volume_number must be between 1 and 5 for HEDIS documents")

    if "extraction_confidence" in extracted_data:
        valid_confidence = ["high", "medium", "low"]
        if extracted_data["extraction_confidence"] not in valid_confidence:
            errors.append(f"extraction_confidence must be one of: {valid_confidence}")

    return len(errors) == 0, errors


# ==============================================================================
# EXPORTS
# ==============================================================================

__all__ = [
    'HEDIS_MEASURE_EXTRACTION_PROMPT',
    'HEDIS_METADATA_EXTRACTION_PROMPT',
    'USAGE_EXAMPLES',
    'validate_measure_extraction',
    'validate_metadata_extraction'
]
