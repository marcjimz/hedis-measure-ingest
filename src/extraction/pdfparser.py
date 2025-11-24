"""
PDF Parser module for extracting structured text from PDF documents.

Based on patterns from dbx-hls-vector-search example - uses PyMuPDF (fitz)
with get_text("words") for better text extraction with position preservation.

Uses Databricks SDK WorkspaceClient to download files from Unity Catalog Volumes.
"""

import fitz  # PyMuPDF
import base64
from typing import List, Dict, Any, Tuple, Optional

from databricks.sdk import WorkspaceClient


class PDFParser:
    """
    PDF document parser using PyMuPDF for text extraction with positional classification.

    Extracts text elements classified as:
    - page_header: Text in the top portion of the page
    - page_footer: Text in the bottom portion of the page
    - text: Body text content

    Uses Databricks SDK to download files from Unity Catalog Volumes.

    Usage:
        parser = PDFParser()
        elements = parser.document_parser(
            file_path="/Volumes/catalog/schema/volume/document.pdf",
            file_id="unique-id",
            file_name="document.pdf",
            effective_year=2025
        )
    """

    def __init__(
        self,
        header_threshold_pct: float = 0.08,
        footer_threshold_pct: float = 0.92,
        line_grouping_threshold: float = 5.0,
        workspace_client: Optional[WorkspaceClient] = None
    ):
        """
        Initialize the PDF parser.

        Args:
            header_threshold_pct: Percentage of page height for header zone (default 8%)
            footer_threshold_pct: Percentage of page height where footer starts (default 92%)
            line_grouping_threshold: Pixel threshold for grouping words into lines
            workspace_client: Optional WorkspaceClient instance (creates new one if not provided)
        """
        self.header_threshold_pct = header_threshold_pct
        self.footer_threshold_pct = footer_threshold_pct
        self.line_grouping_threshold = line_grouping_threshold
        self._workspace_client = workspace_client

    @property
    def workspace_client(self) -> WorkspaceClient:
        """Lazy initialization of WorkspaceClient."""
        if self._workspace_client is None:
            self._workspace_client = WorkspaceClient()
        return self._workspace_client

    def _download_file_from_volume(self, file_path: str, verbose: bool = True) -> bytes:
        """
        Download a file from Databricks Unity Catalog Volume.

        Args:
            file_path: Path to the file (e.g., /Volumes/catalog/schema/volume/file.pdf)
            verbose: Whether to print progress messages

        Returns:
            File content as bytes
        """
        if verbose:
            print(f"  📥 Downloading from volume: {file_path}")

        response = self.workspace_client.files.download(file_path=file_path)
        pdf_bytes_encoded = response.contents.read()

        # The SDK returns base64-encoded content
        try:
            pdf_bytes = base64.b64decode(pdf_bytes_encoded)
        except Exception:
            # If it's not base64 encoded, use as-is
            pdf_bytes = pdf_bytes_encoded

        if verbose:
            print(f"  📦 Downloaded {len(pdf_bytes):,} bytes")

        return pdf_bytes

    def document_parser(
        self,
        file_path: str,
        file_id: str,
        file_name: str,
        effective_year: int,
        max_pages: Optional[int] = None,
        verbose: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Parse a PDF file and extract elements with positional classification.

        Args:
            file_path: Path to the PDF file (supports /Volumes/ paths for Databricks)
            file_id: Unique identifier for the file
            file_name: Name of the file
            effective_year: Year associated with the document
            max_pages: Maximum number of pages to process (None for all)
            verbose: Whether to print progress messages

        Returns:
            List of element dictionaries with:
            - file_id, file_name, effective_year
            - element_type: 'page_header', 'page_footer', or 'text'
            - element_content: the text content
            - page_number: 1-based page number
            - is_page_metadata: True for headers/footers
        """
        elements = []

        try:
            # For Databricks volumes, use the SDK to download the file
            if file_path.startswith("/Volumes/"):
                pdf_bytes = self._download_file_from_volume(file_path, verbose)
                doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            else:
                # Local file path
                doc = fitz.open(file_path)
            n_pages = len(doc)

            if max_pages:
                n_pages = min(n_pages, max_pages)

            if verbose:
                print(f"  📄 Document has {len(doc)} pages (processing {n_pages})")

            for page_idx in range(n_pages):
                page = doc[page_idx]
                page_num = page_idx + 1
                page_height = page.rect.height
                page_width = page.rect.width

                # Calculate thresholds based on page height
                header_threshold = page_height * self.header_threshold_pct
                footer_threshold = page_height * self.footer_threshold_pct

                # Get words with positions for header/footer detection
                # word_tuple format: (x0, y0, x1, y1, "word", block_no, line_no, word_no)
                words_data = page.get_text("words")

                # Group words by their vertical position
                header_words = []
                footer_words = []

                for word_tuple in words_data:
                    x0, y0, x1, y1, text, block_no, line_no, word_no = word_tuple

                    if not text.strip():
                        continue

                    # Classify by vertical position
                    if y0 < header_threshold:
                        header_words.append((text, y0, line_no, block_no))
                    elif y1 > footer_threshold:
                        footer_words.append((text, y0, line_no, block_no))

                # Process header elements
                header_blocks = self._words_to_text_blocks(header_words)
                for block_text in header_blocks:
                    if block_text:
                        elements.append({
                            "file_id": file_id,
                            "file_name": file_name,
                            "effective_year": effective_year,
                            "element_type": "page_header",
                            "element_content": block_text,
                            "page_number": page_num,
                            "is_page_metadata": True
                        })

                # Process body elements using block-level extraction for better paragraph structure
                blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]

                for block in blocks:
                    if block.get("type") == 0:  # Text block
                        block_top = block.get("bbox", [0, 0, 0, 0])[1]
                        block_bottom = block.get("bbox", [0, 0, 0, 0])[3]

                        # Skip headers and footers (already processed)
                        if block_top < header_threshold or block_bottom > footer_threshold:
                            continue

                        # Extract text from lines within the block
                        block_text = ""
                        for line in block.get("lines", []):
                            line_text = ""
                            for span in line.get("spans", []):
                                text = span.get("text", "")
                                if text:
                                    line_text += text
                            if line_text.strip():
                                block_text += line_text + "\n"

                        block_text = block_text.strip()

                        if block_text:
                            elements.append({
                                "file_id": file_id,
                                "file_name": file_name,
                                "effective_year": effective_year,
                                "element_type": "text",
                                "element_content": block_text,
                                "page_number": page_num,
                                "is_page_metadata": False
                            })

                # Process footer elements
                footer_blocks = self._words_to_text_blocks(footer_words)
                for block_text in footer_blocks:
                    if block_text:
                        elements.append({
                            "file_id": file_id,
                            "file_name": file_name,
                            "effective_year": effective_year,
                            "element_type": "page_footer",
                            "element_content": block_text,
                            "page_number": page_num,
                            "is_page_metadata": True
                        })

            doc.close()

            # Summary stats
            if verbose:
                header_count = sum(1 for e in elements if e["element_type"] == "page_header")
                footer_count = sum(1 for e in elements if e["element_type"] == "page_footer")
                text_count = sum(1 for e in elements if e["element_type"] == "text")
                print(f"  📊 Extracted: {header_count} headers, {text_count} text blocks, {footer_count} footers")

        except Exception as e:
            print(f"Error parsing {file_name}: {str(e)}")
            raise

        return elements

    def _words_to_text_blocks(self, words: List[Tuple]) -> List[str]:
        """
        Group words into text blocks based on line proximity.

        Args:
            words: List of tuples (text, y_position, line_no, block_no)

        Returns:
            List of text blocks as strings
        """
        if not words:
            return []

        # Sort by y position, then by block/line
        sorted_words = sorted(words, key=lambda w: (w[1], w[3], w[2]))

        blocks = []
        current_block = [sorted_words[0][0]]
        current_y = sorted_words[0][1]

        for word, y, line_no, block_no in sorted_words[1:]:
            # If y position jumps significantly, start new block
            if abs(y - current_y) > self.line_grouping_threshold:
                block_text = ' '.join(current_block).strip()
                if block_text:
                    blocks.append(block_text)
                current_block = [word]
                current_y = y
            else:
                current_block.append(word)
                current_y = y

        # Don't forget the last block
        if current_block:
            block_text = ' '.join(current_block).strip()
            if block_text:
                blocks.append(block_text)

        return blocks

    def extract_text_only(
        self,
        file_path: str,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extract plain text from PDF without element classification.

        Simpler extraction method that returns page-level text data,
        similar to the extract_text_with_ocr function in the example.

        Args:
            file_path: Path to the PDF file
            max_pages: Maximum number of pages to process

        Returns:
            List of page dictionaries with:
            - page_number: 1-based page number
            - text: Full page text
            - char_count: Character count
            - word_count: Word count
        """
        # For Databricks volumes, use the SDK to download the file
        if file_path.startswith("/Volumes/"):
            pdf_bytes = self._download_file_from_volume(file_path, verbose=False)
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        else:
            doc = fitz.open(file_path)
        n_pages = min(len(doc), max_pages) if max_pages else len(doc)

        pages_data = []

        for page_num in range(n_pages):
            page = doc[page_num]

            # Get words with positions
            words_data = page.get_text("words")

            # Process words
            words = []
            for word_tuple in words_data:
                x0, y0, x1, y1, text = word_tuple[:5]
                if text.strip():
                    words.append({
                        'text': text,
                        'x': int(x0),
                        'y': int(y0),
                        'width': int(x1 - x0),
                        'height': int(y1 - y0),
                    })

            # Get full text
            full_text = page.get_text()

            pages_data.append({
                'page_number': page_num + 1,
                'text': full_text.strip(),
                'words': words,
                'char_count': len(full_text),
                'word_count': len(words)
            })

        doc.close()

        return pages_data
