# HEDIS Extraction Prompts - Quick Reference

## At a Glance

### Two Prompts Available

| Prompt | Purpose | Input | Output |
|--------|---------|-------|--------|
| `HEDIS_MEASURE_EXTRACTION_PROMPT` | Extract measure definitions | OCR text from measure pages | Measure schema with specifications, denominator, numerator, exclusions |
| `HEDIS_METADATA_EXTRACTION_PROMPT` | Extract document metadata | Title page text/image | Document title, publication date, volume, publisher, etc. |

---

## Measure Extraction - Quick Start

### Input Requirements
- OCR extracted text from HEDIS measure pages
- Typically 3-5 pages per measure
- Should include: Description, Population, Denominator, Numerator, Exclusions sections

### Output Schema
```json
{
    "Specifications": "string - official description",
    "measure": "string - Full Name (ACRONYM)",
    "Initial_Pop": "string - eligibility criteria",
    "denominator": ["array", "of", "criteria"],
    "numerator": ["array", "of", "criteria"],
    "exclusion": ["array", "of", "conditions"],
    "effective_year": 2025
}
```

### Usage Pattern
```python
from src.prompts import HEDIS_MEASURE_EXTRACTION_PROMPT, validate_measure_extraction

# 1. Extract text from measure pages
text = extract_text_with_ocr(pdf_bytes, start_page=75, max_pages=3)

# 2. Build prompt
prompt = f"{HEDIS_MEASURE_EXTRACTION_PROMPT}\n\n**Document OCR extracted text**:\n{text}"

# 3. Call LLM
result = client.predict(endpoint="...", inputs={"messages": [{"role": "user", "content": prompt}]})

# 4. Validate
measure = json.loads(result["choices"][0]["message"]["content"])
is_valid, errors = validate_measure_extraction(measure)
```

### Common Issues
| Issue | Cause | Fix |
|-------|-------|-----|
| Empty arrays | Incomplete page range | Include more pages around measure |
| Missing measure name | OCR quality | Use higher resolution or multimodal |
| Hallucinated content | LLM inference | Validate against source, use JSON schema enforcement |

---

## Metadata Extraction - Quick Start

### Input Requirements
- First 5 pages of HEDIS document (title page, copyright, TOC)
- Best results with multimodal (image + text)

### Output Schema
```json
{
    "document_title": "string",
    "publication_date": "YYYY-MM-DD",
    "measurement_year": 2025,
    "volume_number": 2,
    "publisher": "NCQA",
    "page_count": 755,
    "extraction_confidence": "high|medium|low"
}
```

### Usage Pattern
```python
from src.prompts import HEDIS_METADATA_EXTRACTION_PROMPT, validate_metadata_extraction

# 1. Get title page as image (better accuracy)
title_page = pdf_to_images(pdf_bytes)[0]

# 2. Build multimodal message
messages = [{
    "role": "user",
    "content": [
        {"type": "text", "text": HEDIS_METADATA_EXTRACTION_PROMPT},
        {"type": "image_url", "image_url": {"url": title_page}}
    ]
}]

# 3. Call LLM
result = client.predict(endpoint="...", inputs={"messages": messages})

# 4. Validate
metadata = json.loads(result["choices"][0]["message"]["content"])
is_valid, errors = validate_metadata_extraction(metadata)
```

---

## JSON Schema Enforcement (Recommended)

### For Measure Extraction
```python
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
```

### For Metadata Extraction
```python
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
                "publisher": {"type": "string"},
                "page_count": {"type": "integer"},
                "extraction_confidence": {"type": "string"}
            },
            "required": ["document_title", "publication_date", "measurement_year", "publisher", "page_count"]
        }
    }
}
```

---

## Validation Functions

### Measure Validation
```python
is_valid, errors = validate_measure_extraction(measure_data)

# Returns:
# is_valid: bool - True if all validations pass
# errors: list[str] - List of validation error messages
```

**Checks performed:**
- All required fields present
- Correct data types (strings, arrays, integers)
- Year in valid range (2020-2030)
- Non-empty arrays for denominator/numerator
- Measure name has minimum length

### Metadata Validation
```python
is_valid, errors = validate_metadata_extraction(metadata)

# Returns:
# is_valid: bool - True if all validations pass
# errors: list[str] - List of validation error messages
```

**Checks performed:**
- All required fields present
- Date in YYYY-MM-DD format
- Publisher is "NCQA"
- Volume number 1-5 (if present)
- Confidence level is valid
- Positive page count

---

## Performance Tips

### 1. Parallel Processing
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=3) as executor:
    measures = list(executor.map(extract_measure, page_ranges))
```

### 2. Caching OCR Results
```python
# Cache OCR results to avoid re-processing
ocr_cache = {}
page_key = f"{file_path}:{start_page}:{end_page}"
if page_key not in ocr_cache:
    ocr_cache[page_key] = extract_text_with_ocr(pdf_bytes, start_page, num_pages)
text = ocr_cache[page_key]
```

### 3. Rate Limiting
```python
import time

def extract_with_rate_limit(page_range):
    result = extract_single_measure(page_range)
    time.sleep(0.5)  # 500ms delay between requests
    return result
```

### 4. Deterministic Results
```python
# Set temperature to 0 for consistent outputs
inputs = {
    "messages": messages,
    "temperature": 0,
    "response_format": {"type": "json_schema", ...}
}
```

---

## Error Handling

### Check for Extraction Errors
```python
result = extract_measure(text)

if "error" in result:
    error_type = result["error"]
    error_msg = result["error_msg"]

    if error_type == "MISSING_CRITICAL_SECTION":
        # Expand page range and retry
        retry_with_more_pages()
    elif error_type == "AMBIGUOUS_MEASURE_BOUNDARY":
        # Manual review needed
        flag_for_review(result)
```

### Common Error Types

| Error | Meaning | Resolution |
|-------|---------|------------|
| `MISSING_CRITICAL_SECTION` | Denominator/numerator/measure name not found | Expand page range, check OCR quality |
| `AMBIGUOUS_MEASURE_BOUNDARY` | Cannot determine measure start/end | Check TOC for correct page numbers |
| `UNRECOGNIZED_FORMAT` | Document doesn't match NCQA structure | Verify correct document, manual extraction |
| `MISSING_CRITICAL_METADATA` | Required metadata not found | Check title page, use multimodal extraction |

---

## Quality Assurance

### Extraction Quality Metrics
```python
def qa_metrics(results):
    total = len(results)
    valid = sum(1 for r in results if validate_measure_extraction(r)[0])

    return {
        "validation_rate": valid / total,
        "error_rate": sum(1 for r in results if "error" in r) / total,
        "avg_denominator_count": avg([len(r["denominator"]) for r in results]),
        "avg_numerator_count": avg([len(r["numerator"]) for r in results])
    }
```

### Sample Validation
- Manually review 10% of extractions
- Compare measure names against TOC
- Verify all measures from TOC are extracted
- Cross-check effective_year consistency

---

## Import Patterns

### Basic Import
```python
from src.prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT
)
```

### With Validation
```python
from src.prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT,
    validate_measure_extraction,
    validate_metadata_extraction
)
```

### Full Import
```python
from src.prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT,
    USAGE_EXAMPLES,
    validate_measure_extraction,
    validate_metadata_extraction
)
```

---

## Pipeline Integration

### Bronze Layer (Metadata)
```python
def create_bronze_metadata(volume_path: str):
    pdf_bytes = read_pdf(volume_path)
    title_page = pdf_to_images(pdf_bytes)[0]

    metadata = extract_metadata(title_page)
    metadata['file_path'] = volume_path
    metadata['ingestion_timestamp'] = datetime.now().isoformat()

    return metadata
```

### Silver Layer (Measure Definitions)
```python
def extract_all_measures(pdf_bytes: bytes, toc: list):
    measures = []

    for measure_info in toc:
        text = extract_text(pdf_bytes, measure_info['start_page'], measure_info['end_page'])
        measure = extract_measure(text)

        if validate_measure_extraction(measure)[0]:
            measures.append(measure)

    return measures
```

---

## Key Design Principles

1. **Extract Only, Never Infer** - No hallucinations, only verbatim extraction
2. **Tree of Thought Reasoning** - Systematic 8-step process for measures, 9-step for metadata
3. **Multi-Layer Validation** - 3 validation rounds (completeness, accuracy, format)
4. **Explicit Error Handling** - Structured error responses for missing/ambiguous data
5. **Schema Enforcement** - Use JSON schema for guaranteed structure compliance

---

## Additional Resources

- **Full Documentation**: [README.md](./README.md)
- **Example Implementation**: `/examples/measurement/NCQA Vector Search.py`
- **Source Code**: [hedis_extraction_prompts.py](./hedis_extraction_prompts.py)
- **Pipeline Instructions**: `/INSTRUCTIONS.md`

---

**Quick Tip**: Always use `json_schema` enforcement over `json_object` for guaranteed schema compliance and use validation functions after every extraction to catch issues early.
