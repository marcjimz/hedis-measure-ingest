"""
HEDIS Document Chunker

Chunks HEDIS PDF documents with header preservation and semantic coherence.
Based on the pdf_chunker.py with enhancements for HEDIS measure boundaries.
"""

import io
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass
class TextChunk:
    """Represents a chunk of text with metadata"""
    chunk_id: int
    text: str
    token_count: int
    start_page: int
    end_page: int
    headers: List[str]
    char_start: int
    char_end: int


@dataclass
class PageContent:
    """Structured page content with headers"""
    page_number: int
    blocks: List[Dict[str, Any]]
    full_text: str


def extract_structured_text(pdf_bytes: bytes, start_page: int = 1, max_pages: int = None) -> List[PageContent]:
    """Extract text from PDF with structural information"""
    print(f"📝 Extracting structured text from PDF ({len(pdf_bytes):,} bytes)")
    
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    total_pages = len(doc)
    
    # Calculate page range
    start_idx = max(0, start_page - 1)  # Convert to 0-based index
    end_idx = min(total_pages, start_idx + max_pages) if max_pages else total_pages
    n_pages = end_idx - start_idx
    
    print(f"  Processing pages {start_page} to {start_page + n_pages - 1} (total: {n_pages} pages)")
    
    pages = []
    
    for page_num in range(start_idx, end_idx):
        print(f"  Processing page {page_num + 1}/{total_pages}...")
        page = doc[page_num]
        
        blocks = page.get_text("dict")["blocks"]
        structured_blocks = []
        full_text = []
        
        for block in blocks:
            if block.get("type") == 0:  # Text block
                # Aggregate spans within each line to capture complete text
                for line in block.get("lines", []):
                    line_text = []
                    line_font_sizes = []
                    line_fonts = []
                    line_bbox = None
                    
                    for span in line.get("spans", []):
                        text = span.get("text", "").strip()
                        if text:
                            line_text.append(text)
                            line_font_sizes.append(span.get("size", 0))
                            line_fonts.append(span.get("font", ""))
                            if line_bbox is None:
                                line_bbox = span.get("bbox", [])
                    
                    if line_text:
                        # Use max font size and check if any span is bold
                        combined_text = " ".join(line_text)
                        max_font_size = max(line_font_sizes) if line_font_sizes else 0
                        is_bold = any("bold" in font.lower() for font in line_fonts)
                        
                        structured_blocks.append({
                            "text": combined_text,
                            "font_size": max_font_size,
                            "font_name": line_fonts[0] if line_fonts else "",
                            "is_bold": is_bold,
                            "bbox": line_bbox
                        })
                        full_text.append(combined_text)
        
        pages.append(PageContent(
            page_number=page_num + 1,
            blocks=structured_blocks,
            full_text=" ".join(full_text)
        ))
    
    doc.close()
    print(f"✅ Extracted {n_pages} pages with structure")
    return pages


def identify_headers(blocks: List[Dict[str, Any]], base_font_size: float = None) -> List[Dict[str, Any]]:
    """Identify headers based on font size and formatting"""
    if not blocks:
        return []
    
    if base_font_size is None:
        from collections import Counter
        font_sizes = [b["font_size"] for b in blocks]
        base_font_size = Counter(font_sizes).most_common(1)[0][0]
    
    annotated_blocks = []
    
    for i, block in enumerate(blocks):
        block_copy = block.copy()
        font_size = block["font_size"]
        is_bold = block["is_bold"]
        text = block["text"]
        
        # Special patterns for HEDIS documents
        is_all_caps = text.isupper() and len(text) > 3
        is_title_case = text.istitle()
        
        # Determine header level with improved heuristics
        if font_size > base_font_size * 1.5 or (is_bold and font_size > base_font_size * 1.3):
            block_copy["header_level"] = 1  # H1 - Main title
        elif font_size > base_font_size * 1.2 or (is_bold and is_all_caps):
            block_copy["header_level"] = 2  # H2 - Section headers
        elif is_bold and font_size >= base_font_size * 0.95:
            block_copy["header_level"] = 3  # H3 - Subsections
        elif is_all_caps and font_size >= base_font_size * 0.9:
            block_copy["header_level"] = 3  # H3 - CAPS subsections
        else:
            block_copy["header_level"] = 0  # Body text
        
        annotated_blocks.append(block_copy)
    
    return annotated_blocks


def format_text_with_headers(blocks: List[Dict[str, Any]]) -> str:
    """Format text with markdown-style headers"""
    formatted_lines = []
    
    for block in blocks:
        text = block["text"]
        header_level = block.get("header_level", 0)
        
        if header_level > 0:
            prefix = "#" * header_level
            formatted_lines.append(f"\n{prefix} {text}\n")
        else:
            formatted_lines.append(text)
    
    return " ".join(formatted_lines)


def approximate_token_count(text: str) -> int:
    """Approximate token count (1 token ≈ 4 characters)"""
    return len(text) // 4


def get_current_headers(blocks: List[Dict[str, Any]], end_idx: int) -> List[str]:
    """Get the current header hierarchy at a given position"""
    headers = {1: [], 2: [], 3: []}  # Changed to lists to collect consecutive headers
    last_level = 0
    
    for i in range(end_idx):
        block = blocks[i]
        header_level = block.get("header_level", 0)
        
        if header_level > 0:
            text = block["text"]
            
            # If same level as last header, append to it (title continuation)
            if header_level == last_level and headers[header_level]:
                # Concatenate with the last header at this level
                headers[header_level][-1] = headers[header_level][-1] + " " + text
            else:
                # New header at this level
                headers[header_level] = [text]
                # Clear lower-level headers
                for level in range(header_level + 1, 4):
                    headers[level] = []
            
            last_level = header_level
        else:
            # Body text - reset the tracking
            last_level = 0
    
    # Return only the most recent header at each level (join if multiple)
    result = []
    for level in [1, 2, 3]:
        if headers[level]:
            result.append(headers[level][-1])  # Take the most recent (concatenated) header
    
    return result


def debug_headers(pages: List[PageContent], max_blocks: int = 50):
    """
    Debug function to visualize detected headers across pages.
    Prints the first max_blocks text blocks with their detected header levels.
    """
    print("\n" + "="*80)
    print("HEADER DETECTION DEBUG")
    print("="*80)
    
    block_count = 0
    for page in pages:
        annotated = identify_headers(page.blocks)
        
        print(f"\n📄 Page {page.page_number}")
        print("-" * 80)
        
        for block in annotated:
            if block_count >= max_blocks:
                print(f"\n... (showing first {max_blocks} blocks)")
                return
            
            level = block.get("header_level", 0)
            text = block["text"]
            font_size = block["font_size"]
            is_bold = block["is_bold"]
            
            if level > 0:
                marker = "H" + str(level)
                print(f"[{marker}] {text[:80]}")
                print(f"     └─ Font: {font_size:.1f}, Bold: {is_bold}")
            else:
                # Show a few body text examples
                if block_count < 20:
                    print(f"[ - ] {text[:80]}")
            
            block_count += 1
    
    print("\n" + "="*80)


def chunk_text_with_overlap(
    pages: List[PageContent],
    chunk_size: int = 1024,
    overlap_percent: float = 0.20
) -> List[TextChunk]:
    """Chunk text across pages with overlap and header tracking"""
    print(f"\n📦 Chunking text with {chunk_size} token limit and {overlap_percent*100}% overlap")
    
    all_blocks = []
    for page in pages:
        annotated = identify_headers(page.blocks)
        for block in annotated:
            block["page_number"] = page.page_number
            all_blocks.append(block)
    
    if not all_blocks:
        return []
    
    chunks = []
    chunk_id = 0
    i = 0
    char_position = 0
    overlap_size = int(chunk_size * overlap_percent)
    
    while i < len(all_blocks):
        chunk_blocks = []
        chunk_tokens = 0
        start_idx = i
        start_page = all_blocks[i]["page_number"]
        chunk_start_char = char_position
        
        while i < len(all_blocks) and chunk_tokens < chunk_size:
            block = all_blocks[i]
            block_text = block["text"]
            block_tokens = approximate_token_count(block_text)
            
            if chunk_tokens + block_tokens > chunk_size and chunk_blocks:
                break
            
            chunk_blocks.append(block)
            chunk_tokens += block_tokens
            char_position += len(block_text) + 1
            i += 1
        
        if not chunk_blocks:
            break
        
        chunk_text = format_text_with_headers(chunk_blocks)
        end_page = chunk_blocks[-1]["page_number"]
        headers = get_current_headers(all_blocks, start_idx + len(chunk_blocks))
        
        chunks.append(TextChunk(
            chunk_id=chunk_id,
            text=chunk_text.strip(),
            token_count=chunk_tokens,
            start_page=start_page,
            end_page=end_page,
            headers=headers,
            char_start=chunk_start_char,
            char_end=char_position
        ))
        
        chunk_id += 1
        
        if i < len(all_blocks):
            overlap_tokens = 0
            overlap_idx = i - 1
            
            while overlap_idx >= start_idx and overlap_tokens < overlap_size:
                block = all_blocks[overlap_idx]
                block_tokens = approximate_token_count(block["text"])
                overlap_tokens += block_tokens
                overlap_idx -= 1
            
            i = max(start_idx + 1, overlap_idx + 1)
    
    print(f"✅ Created {len(chunks)} chunks")
    print(f"   Average chunk size: {sum(c.token_count for c in chunks) / len(chunks):.0f} tokens")
    print(f"   Page range: {chunks[0].start_page} - {chunks[-1].end_page}")
    
    return chunks


def process_pdf_to_chunks(
    pdf_bytes: bytes,
    start_page: int = 1,
    max_pages: int = None,
    chunk_size: int = 1024,
    overlap_percent: float = 0.20
) -> List[TextChunk]:
    """
    Complete pipeline: PDF → Structured Text → Chunks
    
    Args:
        pdf_bytes: PDF content as bytes
        start_page: Page number to start processing from (1-indexed, default: 1)
        max_pages: Maximum number of pages to process from start_page (None = all remaining)
        chunk_size: Target chunk size in tokens (default: 1024)
        overlap_percent: Overlap between chunks (default: 0.20 = 20%)
    
    Returns:
        List of TextChunk objects with headers preserved
    
    Examples:
        # Process first 20 pages
        chunks = process_pdf_to_chunks(pdf_bytes, start_page=1, max_pages=20)
        
        # Process pages 10-30 (21 pages total)
        chunks = process_pdf_to_chunks(pdf_bytes, start_page=10, max_pages=21)
        
        # Process from page 50 to end
        chunks = process_pdf_to_chunks(pdf_bytes, start_page=50, max_pages=None)
    """
    pages = extract_structured_text(pdf_bytes, start_page=start_page, max_pages=max_pages)
    chunks = chunk_text_with_overlap(pages, chunk_size=chunk_size, overlap_percent=overlap_percent)
    return chunks


# ============================================================================
# READY TO USE! Examples:
# ============================================================================

"""
# In your Databricks notebook:

# 1. Download PDF
response = w.files.download(file_path=policy_pdf_path)
pdf_bytes = response.contents.read()

# 2. DEBUG: See what headers are detected (OPTIONAL - use this to troubleshoot)
pages = extract_structured_text(pdf_bytes, start_page=72, max_pages=3)
debug_headers(pages, max_blocks=100)  # Shows first 100 blocks with header levels

# 3. Process to chunks - EXAMPLES:

# Example 1: Process first 20 pages
chunks = process_pdf_to_chunks(
    pdf_bytes=pdf_bytes,
    start_page=1,
    max_pages=20,
    chunk_size=1024,
    overlap_percent=0.20
)

# Example 2: Process pages 10-30 (21 pages)
chunks = process_pdf_to_chunks(
    pdf_bytes=pdf_bytes,
    start_page=10,
    max_pages=21
)

# Example 3: Process from page 50 to end of document
chunks = process_pdf_to_chunks(
    pdf_bytes=pdf_bytes,
    start_page=50,
    max_pages=None
)

# Example 4: Skip first 5 pages, process next 15 pages
chunks = process_pdf_to_chunks(
    pdf_bytes=pdf_bytes,
    start_page=6,
    max_pages=15
)

# 4. Use the chunks
for chunk in chunks[:3]:
    print(f"Chunk {chunk.chunk_id}")
    print(f"  Headers: {' > '.join(chunk.headers)}")
    print(f"  Pages: {chunk.start_page}-{chunk.end_page}")
    print(f"  Tokens: {chunk.token_count}")
    print(f"  Text: {chunk.text[:200]}...")
"""


# HEDIS-specific wrapper class
class HEDISChunker:
    """
    HEDIS-aware chunker with measure boundary support

    Example:
        >>> chunker = HEDISChunker(chunk_size=1536, overlap_percent=0.15)
        >>> chunks = chunker.chunk_document(pdf_bytes)
    """

    def __init__(self, chunk_size: int = 1536, overlap_percent: float = 0.15):
        """
        Initialize HEDIS chunker

        Args:
            chunk_size: Target tokens per chunk (default: 1536 for HEDIS)
            overlap_percent: Overlap between chunks (default: 0.15 = 15%)
        """
        self.chunk_size = chunk_size
        self.overlap_percent = overlap_percent
        logger.info(f"Initialized HEDIS chunker: size={chunk_size}, overlap={overlap_percent}")

    def chunk_document(
        self,
        pdf_bytes: bytes,
        start_page: int = 1,
        max_pages: Optional[int] = None
    ) -> List[TextChunk]:
        """
        Chunk HEDIS document with header preservation

        Args:
            pdf_bytes: PDF content as bytes
            start_page: Starting page number (1-indexed)
            max_pages: Maximum pages to process (None for all)

        Returns:
            List of TextChunk objects
        """
        return process_pdf_to_chunks(
            pdf_bytes=pdf_bytes,
            start_page=start_page,
            max_pages=max_pages,
            chunk_size=self.chunk_size,
            overlap_percent=self.overlap_percent
        )