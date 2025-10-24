"""
PDF Processing Module for HEDIS Documents

Handles PDF reading, text extraction, OCR, and table of contents parsing.
"""

import io
import hashlib
import logging
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass
class PageContent:
    """Represents extracted content from a single PDF page"""
    page_number: int
    text: str
    blocks: List[Dict]
    char_count: int
    word_count: int


class PDFProcessor:
    """Handles PDF operations for HEDIS documents"""

    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        """
        Initialize PDF processor

        Args:
            workspace_client: Databricks WorkspaceClient for file operations
        """
        self.w = workspace_client or WorkspaceClient()

    def read_pdf_from_volume(self, file_path: str) -> bytes:
        """
        Read PDF file from Databricks volume

        Args:
            file_path: Full path to PDF in volume (e.g., /Volumes/catalog/schema/volume/file.pdf)

        Returns:
            PDF content as bytes

        Example:
            >>> processor = PDFProcessor()
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

    def get_page_count(self, pdf_bytes: bytes) -> int:
        """
        Get total number of pages in PDF

        Args:
            pdf_bytes: PDF content as bytes

        Returns:
            Number of pages
        """
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(doc)
        doc.close()
        return page_count

    def calculate_checksum(self, pdf_bytes: bytes) -> str:
        """
        Calculate SHA256 checksum for PDF deduplication

        Args:
            pdf_bytes: PDF content as bytes

        Returns:
            Hexadecimal checksum string
        """
        return hashlib.sha256(pdf_bytes).hexdigest()

    def extract_text_from_pages(
        self,
        pdf_bytes: bytes,
        start_page: int = 1,
        end_page: Optional[int] = None
    ) -> List[PageContent]:
        """
        Extract text from specified page range

        Args:
            pdf_bytes: PDF content as bytes
            start_page: Starting page number (1-indexed)
            end_page: Ending page number (inclusive), None for last page

        Returns:
            List of PageContent objects

        Example:
            >>> pages = processor.extract_text_from_pages(pdf_bytes, start_page=10, end_page=20)
            >>> for page in pages:
            ...     print(f"Page {page.page_number}: {page.char_count} characters")
        """
        logger.info(f"Extracting text from pages {start_page} to {end_page or 'end'}")

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)

        # Convert to 0-based indexing
        start_idx = max(0, start_page - 1)
        end_idx = min(total_pages, end_page) if end_page else total_pages

        pages = []

        for page_num in range(start_idx, end_idx):
            page = doc[page_num]

            # Extract structured blocks
            blocks_dict = page.get_text("dict")["blocks"]
            text = page.get_text()

            # Extract words with positions
            words = page.get_text("words")

            pages.append(PageContent(
                page_number=page_num + 1,
                text=text.strip(),
                blocks=blocks_dict,
                char_count=len(text),
                word_count=len(words)
            ))

        doc.close()
        logger.info(f"Extracted text from {len(pages)} pages")
        return pages

    def extract_table_of_contents(self, pdf_bytes: bytes) -> List[Dict[str, any]]:
        """
        Extract table of contents with measure boundaries

        Args:
            pdf_bytes: PDF content as bytes

        Returns:
            List of TOC entries with measure names and page numbers

        Example:
            >>> toc = processor.extract_table_of_contents(pdf_bytes)
            >>> for entry in toc[:3]:
            ...     print(f"{entry['measure_name']}: pages {entry['start_page']}-{entry['end_page']}")
        """
        import re

        logger.info("Parsing table of contents...")

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")

        # Search first 30 pages for TOC
        toc_entries = []
        toc_pattern = re.compile(r"([A-Z]{2,5})\s*[-–—]\s*(.+?)\s+\.{2,}\s+(\d+)")

        for page_num in range(min(30, len(doc))):
            page = doc[page_num]
            text = page.get_text()

            # Find TOC entries (pattern: "AMM - Antidepressant Medication Management ... 45")
            matches = toc_pattern.findall(text)

            for acronym, name, page_str in matches:
                toc_entries.append({
                    "measure_acronym": acronym.strip(),
                    "measure_name": f"{acronym.strip()} - {name.strip()}",
                    "start_page": int(page_str)
                })

        # Infer end_page from next measure's start_page
        for i in range(len(toc_entries) - 1):
            toc_entries[i]["end_page"] = toc_entries[i + 1]["start_page"] - 1

        # Last entry: assume 10-15 pages or end of document
        if toc_entries:
            toc_entries[-1]["end_page"] = min(
                toc_entries[-1]["start_page"] + 15,
                len(doc)
            )

        doc.close()

        logger.info(f"Extracted {len(toc_entries)} TOC entries")
        return toc_entries
