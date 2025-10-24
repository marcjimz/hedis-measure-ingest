# HEDIS Extraction Prompts

This directory contains optimized LLM prompts for extracting HEDIS measure definitions and metadata from NCQA documentation with 100% accuracy.

## Overview

The prompts implement a **Tree of Thought reasoning** approach combined with **multi-step validation** to prevent hallucinations and ensure precise extraction. They are designed to work with both text-based OCR extraction and multimodal (image + text) LLM processing.

## Prompt Files

### `hedis_extraction_prompts.py`

Contains two main prompts:

1. **HEDIS_MEASURE_EXTRACTION_PROMPT** - Extracts measure definitions
2. **HEDIS_METADATA_EXTRACTION_PROMPT** - Extracts document metadata

Plus validation functions to verify extraction quality.

## Design Principles

### 1. Extract Only, Never Infer
Both prompts enforce strict extraction rules:
- Extract text verbatim from source documents
- Never add, summarize, or paraphrase content
- Use empty strings/arrays for missing fields rather than guessing
- Preserve original formatting and capitalization

### 2. Tree of Thought Reasoning
The prompts guide the LLM through systematic multi-step reasoning:
- **Measure Extraction**: 8-step process (identify boundaries → extract components → validate)
- **Metadata Extraction**: 9-step process (scan locations → extract fields → validate)

Each step has clear tasks, expected formats, and validation criteria.

### 3. Multi-Layer Validation
Three validation rounds ensure accuracy:
- **Round 1 - Completeness**: All required fields present
- **Round 2 - Accuracy**: Re-read source to verify exact matches
- **Round 3 - Format**: Correct data types and schema compliance

### 4. Error Handling
Explicit error conditions with structured error responses:
- Missing critical sections
- Ambiguous measure boundaries
- Unrecognized document formats
- Missing metadata fields

## HEDIS Measure Extraction

### Target Schema

```json
{
    "Specifications": "Official measure description from NCQA",
    "measure": "Full Measure Name (ACRONYM)",
    "Initial_Pop": "Initial population eligibility criteria",
    "denominator": ["criterion 1", "criterion 2", "..."],
    "numerator": ["criterion 1", "criterion 2", "..."],
    "exclusion": ["exclusion 1", "exclusion 2", "..."],
    "effective_year": 2025
}
```

### 8-Step Extraction Process

1. **Identify Measure Boundary** - Locate start/end of measure definition
2. **Extract Measure Identification** - Get official name and acronym
3. **Extract Specifications** - Get official description
4. **Extract Initial Population** - Get base eligibility criteria
5. **Extract Denominator** - Get all denominator criteria as array
6. **Extract Numerator** - Get all numerator criteria as array
7. **Extract Exclusions** - Get all exclusion conditions as array
8. **Extract Effective Year** - Get measurement year

### Common Measure Patterns

The prompt recognizes three common HEDIS patterns:
- **Age-Based Screening** (e.g., BCS, COL)
- **Chronic Disease Management** (e.g., CDC, CBP)
- **Preventive Care** (e.g., CIS, IMA)

## HEDIS Metadata Extraction

### Target Schema

```json
{
    "document_title": "HEDIS MY 2025 Volume 2 Technical Update",
    "publication_date": "2025-03-31",
    "measurement_year": 2025,
    "volume_number": 2,
    "version": "Technical Update 2025-03-31",
    "publisher": "NCQA",
    "page_count": 755,
    "file_name": "HEDIS MY 2025 Volume 2.pdf",
    "file_size_bytes": 7451092,
    "file_path": "/Volumes/catalog/schema/volume/file.pdf",
    "ingestion_timestamp": "2025-10-23T14:30:00Z",
    "document_type": "HEDIS_TECHNICAL_SPECIFICATIONS",
    "extraction_confidence": "high"
}
```

### 9-Step Extraction Process

1. **Document Identification** - Scan title page, headers, footers
2. **Extract Title** - Get complete document title
3. **Extract Publication Date** - Find and format publication date
4. **Extract Measurement Year** - Get effective measurement year
5. **Extract Volume Number** - Get volume number (1-5)
6. **Extract Version** - Get version/update identifier
7. **Extract Publisher** - Verify NCQA as publisher
8. **Extract Page Count** - Get total PDF page count
9. **Extract File Metadata** - Get file system metadata

### Metadata Search Locations

The prompt instructs the LLM to search in priority order:
1. Title page (page 1)
2. Document headers (top of pages)
3. Document footers (bottom of pages)
4. Table of contents (pages 2-10)
5. Copyright/publication info (page 2 or last page)

## Usage Examples

### Example 1: Extract Measure Definition (Text-based)

```python
import json
from mlflow.deployments import get_deploy_client
from src.prompts.hedis_extraction_prompts import HEDIS_MEASURE_EXTRACTION_PROMPT

# OCR extracted text from measure pages
extracted_text = extract_text_with_ocr(pdf_bytes, start_page=75, max_pages=3)
text_content = "\n".join([page['text'] for page in extracted_text])

# Build prompt
full_prompt = f"{HEDIS_MEASURE_EXTRACTION_PROMPT}\n\n**Document OCR extracted text**:\n{text_content}"

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

# Parse and validate
measure_data = json.loads(response["choices"][0]["message"]["content"])

# Validate extraction
from src.prompts.hedis_extraction_prompts import validate_measure_extraction
is_valid, errors = validate_measure_extraction(measure_data)
if not is_valid:
    print(f"Validation errors: {errors}")
```

### Example 2: Extract Metadata (Multimodal)

```python
from src.prompts.hedis_extraction_prompts import HEDIS_METADATA_EXTRACTION_PROMPT

# Convert title page to image
title_page_image = pdf_to_images(pdf_bytes)[0]  # base64 encoded

# Build multimodal message
messages = [
    {
        "role": "user",
        "content": [
            {"type": "text", "text": HEDIS_METADATA_EXTRACTION_PROMPT},
            {"type": "image_url", "image_url": {"url": title_page_image}}
        ]
    }
]

# Call LLM
response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": messages,
        "response_format": {"type": "json_object"}
    }
)

# Parse and validate
metadata = json.loads(response["choices"][0]["message"]["content"])

from src.prompts.hedis_extraction_prompts import validate_metadata_extraction
is_valid, errors = validate_metadata_extraction(metadata)
if not is_valid:
    print(f"Validation errors: {errors}")
```

### Example 3: Batch Process Multiple Measures

```python
from concurrent.futures import ThreadPoolExecutor
import json

def extract_single_measure(page_range: tuple[int, int]) -> dict:
    """Extract a single measure from a page range."""
    start_page, end_page = page_range
    num_pages = end_page - start_page + 1

    # Extract text
    extracted_text = extract_text_with_ocr(pdf_bytes, start_page=start_page, max_pages=num_pages)
    text_content = "\n".join([page['text'] for page in extracted_text])

    # Build prompt
    full_prompt = f"{HEDIS_MEASURE_EXTRACTION_PROMPT}\n\n**Document OCR extracted text**:\n{text_content}"

    # Call LLM
    response = client.predict(
        endpoint="databricks-meta-llama-3-3-70b-instruct",
        inputs={
            "messages": [{"role": "user", "content": full_prompt}],
            "response_format": {"type": "json_object"}
        }
    )

    return json.loads(response["choices"][0]["message"]["content"])

# Define measure page ranges (from TOC)
measure_ranges = [
    (75, 78),   # BCS - Breast Cancer Screening
    (85, 88),   # COL - Colorectal Cancer Screening
    (120, 124), # CBP - Controlling High Blood Pressure
    # ... more measures
]

# Extract in parallel
with ThreadPoolExecutor(max_workers=5) as executor:
    measures = list(executor.map(extract_single_measure, measure_ranges))

# Validate all extractions
from src.prompts.hedis_extraction_prompts import validate_measure_extraction
for i, measure in enumerate(measures):
    is_valid, errors = validate_measure_extraction(measure)
    if not is_valid:
        print(f"Measure {i} validation failed: {errors}")
```

## Validation Functions

### `validate_measure_extraction(extracted_data: dict) -> tuple[bool, list[str]]`

Validates extracted measure data against schema requirements:
- Checks for required fields
- Validates data types (strings, arrays, integers)
- Validates value ranges (e.g., year 2020-2030)
- Checks for reasonable content (non-empty arrays, minimum lengths)

**Returns**: `(is_valid, list_of_errors)`

### `validate_metadata_extraction(extracted_data: dict) -> tuple[bool, list[str]]`

Validates extracted metadata against schema requirements:
- Checks for required fields
- Validates date format (YYYY-MM-DD)
- Validates publisher is "NCQA"
- Validates volume number range (1-5)
- Validates confidence level ("high", "medium", "low")

**Returns**: `(is_valid, list_of_errors)`

## Integration with Pipeline

These prompts are designed to integrate into the modular data ingestion pipeline:

### Bronze Layer - Metadata Extraction
```python
# Module: bronze_metadata_table.py
from src.prompts.hedis_extraction_prompts import HEDIS_METADATA_EXTRACTION_PROMPT

def create_bronze_metadata(volume_path: str) -> dict:
    """Extract metadata for bronze layer."""
    pdf_bytes = read_pdf_from_volume(volume_path)
    title_page = pdf_to_images(pdf_bytes)[0]

    metadata = extract_with_multimodal_llm(
        images_data=title_page,
        prompt=HEDIS_METADATA_EXTRACTION_PROMPT
    )

    # Add file system metadata
    metadata['file_path'] = volume_path
    metadata['ingestion_timestamp'] = datetime.now().isoformat()

    return metadata
```

### Silver Layer - Measure Definitions
```python
# Module: hedis_measures_definitions.py
from src.prompts.hedis_extraction_prompts import HEDIS_MEASURE_EXTRACTION_PROMPT

def extract_measure_definitions(pdf_bytes: bytes, toc_data: list) -> list[dict]:
    """Extract all measure definitions from HEDIS document."""
    measures = []

    for measure_info in toc_data:
        # Get page range from TOC
        start_page = measure_info['start_page']
        end_page = measure_info['end_page']

        # Extract text
        text = extract_text_with_ocr(pdf_bytes, start_page, end_page - start_page + 1)

        # Extract measure
        measure = extract_with_text_llm(
            text=json.dumps(text),
            prompt=HEDIS_MEASURE_EXTRACTION_PROMPT
        )

        # Validate
        is_valid, errors = validate_measure_extraction(measure)
        if is_valid:
            measures.append(measure)
        else:
            print(f"Validation failed for {measure_info['name']}: {errors}")

    return measures
```

## Best Practices

### 1. Use JSON Schema Enforcement
Always use the `response_format` parameter with `json_schema` type to enforce strict schema compliance:

```python
"response_format": {
    "type": "json_schema",
    "json_schema": {
        "name": "extraction_name",
        "schema": { ... }
    }
}
```

### 2. Validate After Extraction
Always run validation functions after extraction:

```python
is_valid, errors = validate_measure_extraction(result)
if not is_valid:
    # Handle validation errors
    log_errors(errors)
    retry_extraction()
```

### 3. Use Multimodal for Title Pages
For metadata extraction, multimodal (text + image) provides better accuracy:

```python
# Better accuracy
metadata = extract_with_multimodal_llm(
    images_data=title_page_image,
    prompt=HEDIS_METADATA_EXTRACTION_PROMPT
)

# vs text-only (lower accuracy)
metadata = extract_with_text_llm(
    text=title_page_text,
    prompt=HEDIS_METADATA_EXTRACTION_PROMPT
)
```

### 4. Handle Errors Gracefully
Check for error responses in extraction results:

```python
if "error" in result:
    error_type = result.get("error")
    error_msg = result.get("error_msg")
    partial_data = result.get("partial_extraction", {})

    # Log error and decide on retry strategy
    handle_extraction_error(error_type, error_msg, partial_data)
```

### 5. Batch Process with Concurrency
For multiple measures, use parallel processing with rate limiting:

```python
from concurrent.futures import ThreadPoolExecutor
from time import sleep

def extract_with_rate_limit(page_range):
    result = extract_single_measure(page_range)
    sleep(0.5)  # Rate limit to avoid overwhelming endpoint
    return result

with ThreadPoolExecutor(max_workers=3) as executor:
    measures = list(executor.map(extract_with_rate_limit, measure_ranges))
```

## Troubleshooting

### Issue: Incomplete Extractions
**Symptom**: Some fields are empty or missing
**Solution**:
- Check OCR quality - may need higher resolution images
- Verify page range includes complete measure definition
- Use multimodal extraction for complex layouts

### Issue: Validation Failures
**Symptom**: Validation function returns errors
**Solution**:
- Review extracted data structure
- Check LLM response format settings
- Verify JSON schema matches expected fields
- Check for hallucinated content (re-read source)

### Issue: Inconsistent Results
**Symptom**: Different results for same input
**Solution**:
- Set temperature to 0 for deterministic results
- Use `json_schema` enforcement, not just `json_object`
- Add more specific examples in prompt

### Issue: Slow Extraction
**Symptom**: Processing takes too long
**Solution**:
- Use parallel processing for multiple measures
- Optimize page ranges to avoid unnecessary pages
- Cache OCR results to avoid re-processing
- Consider using smaller, faster model for simple extractions

## Monitoring & Quality Assurance

### Extraction Quality Metrics

Track these metrics to monitor extraction quality:

```python
def calculate_extraction_metrics(results: list[dict]) -> dict:
    """Calculate quality metrics for batch extraction."""
    total = len(results)
    valid = sum(1 for r in results if validate_measure_extraction(r)[0])
    with_errors = sum(1 for r in results if "error" in r)
    high_confidence = sum(1 for r in results if r.get("extraction_confidence") == "high")

    return {
        "total_extractions": total,
        "valid_extractions": valid,
        "validation_rate": valid / total if total > 0 else 0,
        "error_rate": with_errors / total if total > 0 else 0,
        "high_confidence_rate": high_confidence / total if total > 0 else 0
    }
```

### Recommended QA Process

1. **Sample Validation**: Manually review 10% of extractions
2. **Cross-Reference**: Compare extracted measure names with TOC
3. **Completeness Check**: Ensure all measures from TOC are extracted
4. **Consistency Check**: Verify effective_year matches across all measures
5. **Downstream Testing**: Test extractions in downstream applications

## Additional Resources

- **NCQA HEDIS Documentation**: https://www.ncqa.org/hedis/
- **Example Notebook**: `/examples/measurement/NCQA Vector Search.py`
- **Pipeline Instructions**: `/INSTRUCTIONS.md`
- **Databricks MLflow Deployments**: https://docs.databricks.com/mlflow/deployments.html

## Support

For issues or questions:
1. Check troubleshooting section above
2. Review validation error messages
3. Examine example notebook for working implementations
4. Verify LLM endpoint availability and permissions

---

**Version**: 1.0
**Author**: Hive Mind CODER Agent
**Last Updated**: 2025-10-23
