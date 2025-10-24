"""
Test and Example Usage for HEDIS Extraction Prompts

This file demonstrates how to use the HEDIS extraction prompts and validation functions.
Run with: python test_prompts.py
"""

import json
from hedis_extraction_prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT,
    validate_measure_extraction,
    validate_metadata_extraction
)


def test_measure_validation():
    """Test measure extraction validation with example data."""
    print("=" * 80)
    print("TEST 1: Valid Measure Extraction")
    print("=" * 80)

    valid_measure = {
        "Specifications": "The percentage of women 50-74 years of age who had a mammogram to screen for breast cancer in the 27 months prior to the end of the measurement period.",
        "measure": "Breast Cancer Screening (BCS)",
        "Initial_Pop": "Women 50-74 years of age as of December 31 of the measurement year.",
        "denominator": [
            "Women 50-74 years of age as of December 31 of the measurement year",
            "Continuously enrolled during the measurement year and the year prior to the measurement year",
            "At least one medical visit during the measurement year or year prior"
        ],
        "numerator": [
            "Mammogram (bilateral or unilateral) during the measurement period"
        ],
        "exclusion": [
            "Women who had a bilateral mastectomy any time on or before December 31 of the measurement year",
            "Women who had a unilateral mastectomy with a unilateral mastectomy on the other breast on different dates any time on or before December 31 of the measurement year",
            "Women who are in hospice or using hospice services any time during the measurement year"
        ],
        "effective_year": 2025
    }

    is_valid, errors = validate_measure_extraction(valid_measure)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    if errors:
        print(f"Errors: {errors}")
    else:
        print("No validation errors detected.")

    print(f"\nExtracted Measure: {valid_measure['measure']}")
    print(f"Denominator Criteria: {len(valid_measure['denominator'])} items")
    print(f"Numerator Criteria: {len(valid_measure['numerator'])} items")
    print(f"Exclusions: {len(valid_measure['exclusion'])} items")


def test_invalid_measure():
    """Test measure validation with invalid data."""
    print("\n" + "=" * 80)
    print("TEST 2: Invalid Measure Extraction (Missing Fields)")
    print("=" * 80)

    invalid_measure = {
        "measure": "Incomplete Measure",
        "denominator": ["Some criterion"],
        # Missing: Specifications, Initial_Pop, numerator, exclusion, effective_year
    }

    is_valid, errors = validate_measure_extraction(invalid_measure)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    print(f"Errors detected: {len(errors)}")
    for i, error in enumerate(errors, 1):
        print(f"  {i}. {error}")


def test_wrong_types():
    """Test measure validation with wrong data types."""
    print("\n" + "=" * 80)
    print("TEST 3: Invalid Data Types")
    print("=" * 80)

    wrong_types = {
        "Specifications": "Valid specification text",
        "measure": "Test Measure (TST)",
        "Initial_Pop": "Valid population",
        "denominator": "Should be array but is string",  # Wrong type
        "numerator": ["Valid array"],
        "exclusion": ["Valid array"],
        "effective_year": "2025"  # Wrong type - should be int
    }

    is_valid, errors = validate_measure_extraction(wrong_types)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    print(f"Errors detected: {len(errors)}")
    for i, error in enumerate(errors, 1):
        print(f"  {i}. {error}")


def test_metadata_validation():
    """Test metadata extraction validation with example data."""
    print("\n" + "=" * 80)
    print("TEST 4: Valid Metadata Extraction")
    print("=" * 80)

    valid_metadata = {
        "document_title": "HEDIS MY 2025 Volume 2 Technical Update",
        "publication_date": "2025-03-31",
        "measurement_year": 2025,
        "volume_number": 2,
        "version": "Technical Update 2025-03-31",
        "publisher": "NCQA",
        "page_count": 755,
        "file_name": "HEDIS MY 2025 Volume 2 Technical Update 2025-03-31.pdf",
        "file_size_bytes": 7451092,
        "file_path": "/Volumes/catalog/schema/volume/file.pdf",
        "ingestion_timestamp": "2025-10-23T14:30:00Z",
        "document_type": "HEDIS_TECHNICAL_SPECIFICATIONS",
        "extraction_confidence": "high"
    }

    is_valid, errors = validate_metadata_extraction(valid_metadata)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    if errors:
        print(f"Errors: {errors}")
    else:
        print("No validation errors detected.")

    print(f"\nDocument: {valid_metadata['document_title']}")
    print(f"Publisher: {valid_metadata['publisher']}")
    print(f"Measurement Year: {valid_metadata['measurement_year']}")
    print(f"Confidence: {valid_metadata['extraction_confidence']}")


def test_invalid_metadata():
    """Test metadata validation with invalid data."""
    print("\n" + "=" * 80)
    print("TEST 5: Invalid Metadata (Wrong Publisher, Invalid Date)")
    print("=" * 80)

    invalid_metadata = {
        "document_title": "HEDIS MY 2025 Volume 2",
        "publication_date": "March 31, 2025",  # Wrong format
        "measurement_year": 2025,
        "publisher": "Wrong Publisher",  # Should be NCQA
        "page_count": -10,  # Invalid - negative
        "extraction_confidence": "very_high"  # Invalid - not in enum
    }

    is_valid, errors = validate_metadata_extraction(invalid_metadata)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    print(f"Errors detected: {len(errors)}")
    for i, error in enumerate(errors, 1):
        print(f"  {i}. {error}")


def test_error_response():
    """Test handling of error responses from extraction."""
    print("\n" + "=" * 80)
    print("TEST 6: Error Response Handling")
    print("=" * 80)

    error_response = {
        "error": "MISSING_CRITICAL_SECTION",
        "error_msg": "Could not locate denominator section in the provided text",
        "partial_extraction": {
            "measure": "Partial Measure (PM)",
            "Specifications": "Some text was extracted"
        }
    }

    is_valid, errors = validate_measure_extraction(error_response)
    print(f"\nValidation Result: {'✅ PASS' if is_valid else '❌ FAIL'}")
    print(f"Error Type: {error_response['error']}")
    print(f"Error Message: {error_response['error_msg']}")
    print(f"Partial Data Available: {list(error_response['partial_extraction'].keys())}")


def print_prompt_stats():
    """Print statistics about the prompts."""
    print("\n" + "=" * 80)
    print("PROMPT STATISTICS")
    print("=" * 80)

    measure_prompt_length = len(HEDIS_MEASURE_EXTRACTION_PROMPT)
    metadata_prompt_length = len(HEDIS_METADATA_EXTRACTION_PROMPT)

    print(f"\nMeasure Extraction Prompt:")
    print(f"  Length: {measure_prompt_length:,} characters")
    print(f"  Estimated tokens: ~{measure_prompt_length // 4:,}")
    print(f"  Lines: {HEDIS_MEASURE_EXTRACTION_PROMPT.count(chr(10))}")

    print(f"\nMetadata Extraction Prompt:")
    print(f"  Length: {metadata_prompt_length:,} characters")
    print(f"  Estimated tokens: ~{metadata_prompt_length // 4:,}")
    print(f"  Lines: {HEDIS_METADATA_EXTRACTION_PROMPT.count(chr(10))}")

    # Count key instruction sections
    measure_sections = [
        "STEP 1:", "STEP 2:", "STEP 3:", "STEP 4:",
        "STEP 5:", "STEP 6:", "STEP 7:", "STEP 8:"
    ]
    print(f"\nMeasure Extraction Steps: {sum(1 for s in measure_sections if s in HEDIS_MEASURE_EXTRACTION_PROMPT)}")

    metadata_sections = [
        "STEP 1:", "STEP 2:", "STEP 3:", "STEP 4:", "STEP 5:",
        "STEP 6:", "STEP 7:", "STEP 8:", "STEP 9:"
    ]
    print(f"Metadata Extraction Steps: {sum(1 for s in metadata_sections if s in HEDIS_METADATA_EXTRACTION_PROMPT)}")


def demonstrate_usage():
    """Show example usage patterns."""
    print("\n" + "=" * 80)
    print("USAGE DEMONSTRATION")
    print("=" * 80)

    print("\n📋 Example 1: Basic Import Pattern")
    print("-" * 80)
    print("""
from src.prompts import (
    HEDIS_MEASURE_EXTRACTION_PROMPT,
    HEDIS_METADATA_EXTRACTION_PROMPT,
    validate_measure_extraction,
    validate_metadata_extraction
)
    """)

    print("\n📋 Example 2: Measure Extraction Flow")
    print("-" * 80)
    print("""
# 1. Extract text from PDF
text = extract_text_with_ocr(pdf_bytes, start_page=75, max_pages=3)

# 2. Build prompt
prompt = f"{HEDIS_MEASURE_EXTRACTION_PROMPT}\\n\\n**Document OCR extracted text**:\\n{text}"

# 3. Call LLM
response = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_schema", "json_schema": {...}}
    }
)

# 4. Parse and validate
measure = json.loads(response["choices"][0]["message"]["content"])
is_valid, errors = validate_measure_extraction(measure)

if is_valid:
    save_to_database(measure)
else:
    log_errors(errors)
    """)

    print("\n📋 Example 3: Metadata Extraction with Multimodal")
    print("-" * 80)
    print("""
# 1. Convert title page to image
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
response = client.predict(endpoint="...", inputs={"messages": messages})

# 4. Validate
metadata = json.loads(response["choices"][0]["message"]["content"])
is_valid, errors = validate_metadata_extraction(metadata)
    """)


if __name__ == "__main__":
    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  HEDIS Extraction Prompts - Test & Validation Suite".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝")

    # Run all tests
    test_measure_validation()
    test_invalid_measure()
    test_wrong_types()
    test_metadata_validation()
    test_invalid_metadata()
    test_error_response()
    print_prompt_stats()
    demonstrate_usage()

    print("\n" + "=" * 80)
    print("✅ ALL TESTS COMPLETED")
    print("=" * 80)
    print("\nFor full documentation, see README.md")
    print("For quick reference, see QUICK_REFERENCE.md")
    print("\n")
