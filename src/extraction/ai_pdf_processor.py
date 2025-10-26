"""
AI-Powered PDF Processing Module for HEDIS Documents

Uses Databricks ai_parse_document SQL function for OCR-free text extraction.
This replaces PyMuPDF with Databricks native AI capabilities.
"""

import logging
import hashlib
import json
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


@dataclass
class PageContent:
    """Represents extracted content from a single PDF page"""
    page_number: int
    text: str
    blocks: List[Dict]
    char_count: int
    word_count: int
    elements: List[Dict]  # Raw elements from ai_parse_document


class AIPDFProcessor:
    """
    AI-powered PDF processor using Databricks ai_parse_document function.

    This processor leverages Databricks' native AI capabilities for document
    parsing, providing superior handling of complex layouts, tables, and figures
    compared to traditional OCR methods.

    Key advantages over PyMuPDF:
    - Native Databricks integration
    - Better table extraction (HTML format)
    - AI-generated figure descriptions
    - Handles complex layouts automatically
    - No external dependencies

    Requirements:
    - Databricks Runtime 17.1+
    - Unity Catalog access
    - US or EU region (or cross-geography routing)
    """

    def __init__(
        self,
        spark: SparkSession,
        workspace_client: Optional[WorkspaceClient] = None,
        batch_size: int = 10
    ):
        """
        Initialize AI PDF processor

        Args:
            spark: SparkSession instance
            workspace_client: Databricks WorkspaceClient (optional)
            batch_size: Batch size for processing multiple files (default: 10)

        Example:
            >>> spark = SparkSession.builder.getOrCreate()
            >>> processor = AIPDFProcessor(spark)
        """
        self.spark = spark
        self.w = workspace_client or WorkspaceClient()
        self.batch_size = batch_size
        logger.info(f"Initialized AI PDF processor with batch_size={batch_size}")

    def read_pdf_from_volume(self, file_path: str) -> bytes:
        """
        Read PDF file from Databricks volume as binary data

        Args:
            file_path: Full path to PDF in volume (e.g., /Volumes/catalog/schema/volume/file.pdf)

        Returns:
            PDF content as bytes

        Example:
            >>> pdf_bytes = processor.read_pdf_from_volume("/Volumes/main/hedis/data/HEDIS_2025.pdf")
        """
        logger.info(f"Reading PDF from volume: {file_path}")
        try:
            response = self.w.files.download(file_path=file_path)
            pdf_bytes = response.contents.read()
            logger.info(f"Successfully read {len(pdf_bytes):,} bytes from {file_path}")
            return pdf_bytes
        except Exception as e:
            logger.error(f"Failed to read PDF from volume: {str(e)}")
            raise

    def parse_document_with_ai(self, file_path: str, image_output_path: Optional[str] = None) -> Dict:
        """
        Parse PDF using Databricks ai_parse_document SQL function

        Args:
            file_path: Path to PDF file in Unity Catalog volume (or directory)
            image_output_path: Optional path to store intermediate page images

        Returns:
            Parsed document structure as dictionary with pages and elements

        Example:
            >>> result = processor.parse_document_with_ai("/Volumes/main/hedis/data/HEDIS_2025.pdf")
            >>> print(f"Extracted {len(result.get('document', {}).get('pages', []))} pages")
        """
        logger.info(f"Parsing document with AI: {file_path}")

        try:
            # Determine image output path (optional but recommended)
            if image_output_path is None:
                # Default to same directory with /output suffix
                import os
                base_dir = os.path.dirname(file_path)
                image_output_path = f"{base_dir}/output/"

            # Use SQL with READ_FILES and ai_parse_document
            # This is the working syntax from the debug notebook
            sql = f"""
                SELECT
                    path,
                    ai_parse_document(
                        content,
                        map(
                            'version', '2.0',
                            'imageOutputPath', '{image_output_path}',
                            'descriptionElementTypes', '*'
                        )
                    ) as parsed_doc
                FROM READ_FILES('{file_path}', format => 'binaryFile')
            """

            # Execute SQL
            result = self.spark.sql(sql).first()

            if result is None:
                raise ValueError(f"No data returned from ai_parse_document for {file_path}")

            # Extract parsed document (VARIANT type returns as dict)
            parsed_doc = result["parsed_doc"]

            # Handle potential parsing errors
            if isinstance(parsed_doc, dict):
                if "error_status" in parsed_doc and parsed_doc["error_status"]:
                    logger.warning(f"Parsing errors: {parsed_doc['error_status']}")

                document = parsed_doc.get('document', {})
                pages = document.get('pages', [])
                logger.info(f"Successfully parsed document: {len(pages)} pages")
            else:
                logger.info(f"Parsed document (type: {type(parsed_doc)})")

            return parsed_doc

        except Exception as e:
            logger.error(f"AI document parsing failed: {str(e)}")
            raise RuntimeError(f"Failed to parse document with AI: {str(e)}") from e

    def extract_text_from_pages(
        self,
        file_path: str,
        start_page: int = 1,
        end_page: Optional[int] = None
    ) -> List[PageContent]:
        """
        Extract text from specified page range using AI parsing

        Args:
            file_path: Path to PDF file in Unity Catalog volume
            start_page: Starting page number (1-indexed)
            end_page: Ending page number (inclusive), None for last page

        Returns:
            List of PageContent objects

        Example:
            >>> pages = processor.extract_text_from_pages("/Volumes/main/hedis/data/HEDIS_2025.pdf",
            ...                                           start_page=10, end_page=20)
            >>> for page in pages:
            ...     print(f"Page {page.page_number}: {page.char_count} characters")
        """
        logger.info(f"Extracting text from pages {start_page} to {end_page or 'end'}")

        # Parse entire document
        parsed_doc = self.parse_document_with_ai(file_path)

        document = parsed_doc.get("document", {})
        pages_info = document.get("pages", [])
        elements = document.get("elements", [])

        # Filter pages based on range
        total_pages = len(pages_info)
        start_idx = start_page - 1  # Convert to 0-based
        end_idx = min(total_pages, end_page) if end_page else total_pages

        logger.info(f"Processing pages {start_page} to {start_page + (end_idx - start_idx) - 1}")

        # Group elements by page
        pages_content = []

        for page_idx in range(start_idx, end_idx):
            page_info = pages_info[page_idx]
            page_id = page_info.get("page_id", page_idx)
            page_number = page_idx + 1

            # Filter elements for this page
            page_elements = [
                elem for elem in elements
                if elem.get("page_id") == page_id
            ]

            # Extract text from elements
            text_parts = []
            blocks = []

            for elem in page_elements:
                elem_type = elem.get("type", "")
                content = elem.get("content", "")

                if elem_type == "text":
                    text_parts.append(content)
                    blocks.append({
                        "type": "text",
                        "content": content,
                        "bbox": elem.get("bounding_box", {}),
                        "element_type": elem_type
                    })
                elif elem_type == "table":
                    # Tables are in HTML format in v2.0
                    text_parts.append(f"[TABLE: {content[:100]}...]")
                    blocks.append({
                        "type": "table",
                        "content": content,
                        "bbox": elem.get("bounding_box", {}),
                        "element_type": elem_type
                    })
                elif elem_type in ["title", "section_header"]:
                    text_parts.append(f"\n{content}\n")
                    blocks.append({
                        "type": "header",
                        "content": content,
                        "bbox": elem.get("bounding_box", {}),
                        "element_type": elem_type
                    })
                else:
                    text_parts.append(content)
                    blocks.append({
                        "type": "other",
                        "content": content,
                        "bbox": elem.get("bounding_box", {}),
                        "element_type": elem_type
                    })

            # Combine text
            full_text = "\n".join(text_parts)

            pages_content.append(PageContent(
                page_number=page_number,
                text=full_text,
                blocks=blocks,
                char_count=len(full_text),
                word_count=len(full_text.split()),
                elements=page_elements
            ))

        logger.info(f"Extracted text from {len(pages_content)} pages")
        return pages_content

    def extract_table_of_contents(self, file_path: str) -> List[Dict[str, any]]:
        """
        Extract table of contents with measure boundaries

        Args:
            file_path: Path to PDF file in Unity Catalog volume

        Returns:
            List of TOC entries with measure names and page numbers

        Example:
            >>> toc = processor.extract_table_of_contents("/Volumes/main/hedis/data/HEDIS_2025.pdf")
            >>> for entry in toc[:3]:
            ...     print(f"{entry['measure_name']}: pages {entry['start_page']}-{entry['end_page']}")
        """
        import re

        logger.info("Parsing table of contents with AI...")

        # Parse document
        parsed_doc = self.parse_document_with_ai(file_path)

        document = parsed_doc.get("document", {})
        elements = document.get("elements", [])

        # Search first 30 pages for TOC entries
        toc_entries = []
        toc_pattern = re.compile(r"([A-Z]{2,5})\s*[-–—]\s*(.+?)\s+\.{2,}\s+(\d+)")

        for elem in elements:
            page_id = elem.get("page_id", 0)

            # Only check first 30 pages
            if page_id >= 30:
                break

            content = elem.get("content", "")

            # Find TOC entries (pattern: "AMM - Antidepressant Medication Management ... 45")
            matches = toc_pattern.findall(content)

            for acronym, name, page_str in matches:
                toc_entries.append({
                    "measure_acronym": acronym.strip(),
                    "measure_name": f"{acronym.strip()} - {name.strip()}",
                    "start_page": int(page_str)
                })

        # Infer end_page from next measure's start_page
        for i in range(len(toc_entries) - 1):
            toc_entries[i]["end_page"] = toc_entries[i + 1]["start_page"] - 1

        # Last entry: assume 10-15 pages
        if toc_entries:
            total_pages = len(parsed_doc.get("document", {}).get("pages", []))
            toc_entries[-1]["end_page"] = min(
                toc_entries[-1]["start_page"] + 15,
                total_pages
            )

        logger.info(f"Extracted {len(toc_entries)} TOC entries using AI")
        return toc_entries

    def get_page_count(self, file_path: str) -> int:
        """
        Get total number of pages in PDF using AI parsing

        Args:
            file_path: Path to PDF file

        Returns:
            Number of pages
        """
        parsed_doc = self.parse_document_with_ai(file_path)
        pages = parsed_doc.get("document", {}).get("pages", [])
        return len(pages)

    def calculate_checksum(self, pdf_bytes: bytes) -> str:
        """
        Calculate SHA256 checksum for PDF deduplication

        Args:
            pdf_bytes: PDF content as bytes

        Returns:
            Hexadecimal checksum string
        """
        return hashlib.sha256(pdf_bytes).hexdigest()

    def batch_process_files(self, file_paths: List[str]) -> List[Dict]:
        """
        Process multiple PDF files in batches to avoid throttling

        Args:
            file_paths: List of PDF file paths

        Returns:
            List of parsed documents

        Example:
            >>> paths = ["/Volumes/main/hedis/data/file1.pdf", "/Volumes/main/hedis/data/file2.pdf"]
            >>> results = processor.batch_process_files(paths)
        """
        logger.info(f"Batch processing {len(file_paths)} files with batch_size={self.batch_size}")

        results = []

        for i in range(0, len(file_paths), self.batch_size):
            batch = file_paths[i:i + self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}: {len(batch)} files")

            for file_path in batch:
                try:
                    parsed_doc = self.parse_document_with_ai(file_path)
                    results.append({
                        "file_path": file_path,
                        "status": "success",
                        "parsed_doc": parsed_doc
                    })
                except Exception as e:
                    logger.error(f"Failed to process {file_path}: {str(e)}")
                    results.append({
                        "file_path": file_path,
                        "status": "failed",
                        "error": str(e)
                    })

        success_count = sum(1 for r in results if r["status"] == "success")
        logger.info(f"Batch processing complete: {success_count}/{len(file_paths)} successful")

        return results
