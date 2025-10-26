"""
Document Renderer for AI-parsed documents.

This module provides visualization and debugging capabilities for documents
parsed with Databricks ai_parse_document function.
"""

import base64
import io
import json
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from IPython.display import HTML, display
from PIL import Image


class DocumentRenderer:
    """Renders AI-parsed documents with visual bounding boxes and tooltips."""

    def __init__(self):
        # Color mapping for different element types
        self.element_colors = {
            "section_header": "#FF6B6B",
            "text": "#4ECDC4",
            "figure": "#45B7D1",
            "caption": "#96CEB4",
            "page_footer": "#FFEAA7",
            "page_header": "#DDA0DD",
            "table": "#98D8C8",
            "list": "#F7DC6F",
            "default": "#BDC3C7",
        }

    def _get_element_color(self, element_type: str) -> str:
        """Get color for element type."""
        return self.element_colors.get(
            element_type.lower(), self.element_colors["default"]
        )

    def _render_element_content(self, element: Dict, for_tooltip: bool = False) -> str:
        """Render element content with appropriate formatting."""
        element_type = element.get("type", "unknown")
        content = element.get("content", "")
        description = element.get("description", "")

        display_content = ""

        if content:
            if element_type == "table":
                # Render the HTML table with styling
                table_html = content

                if for_tooltip:
                    table_style = '''style="width: 100%; border-collapse: collapse; margin: 5px 0; font-size: 10px;"'''
                    th_style = 'style="border: 1px solid #ddd; padding: 4px; background: #f8f9fa; color: #333; font-weight: bold; text-align: left; font-size: 10px;"'
                    td_style = 'style="border: 1px solid #ddd; padding: 4px; color: #333; font-size: 10px;"'
                    thead_style = 'style="background: #e9ecef;"'
                else:
                    table_style = '''style="width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 13px;"'''
                    th_style = 'style="border: 1px solid #ddd; padding: 8px; background: #f5f5f5; font-weight: bold; text-align: left;"'
                    td_style = 'style="border: 1px solid #ddd; padding: 8px;"'
                    thead_style = 'style="background: #f0f0f0;"'

                # Apply styling transformations
                if "<table>" in table_html:
                    table_html = table_html.replace("<table>", f"<table {table_style}>")
                if "<th>" in table_html:
                    table_html = table_html.replace("<th>", f"<th {th_style}>")
                if "<td>" in table_html:
                    table_html = table_html.replace("<td>", f"<td {td_style}>")
                if "<thead>" in table_html:
                    table_html = table_html.replace("<thead>", f"<thead {thead_style}>")

                if for_tooltip:
                    display_content = table_html
                else:
                    display_content = f"<div style='overflow-x: auto; margin: 10px 0;'>{table_html}</div>"
            else:
                # Regular content handling
                if for_tooltip and len(content) > 500:
                    display_content = self._escape_for_html_attribute(content[:500] + "...")
                else:
                    display_content = (
                        self._escape_for_html_attribute(content)
                        if for_tooltip
                        else content
                    )
        elif description:
            desc_content = description
            if for_tooltip and len(desc_content) > 500:
                desc_content = desc_content[:500] + "..."

            if for_tooltip:
                display_content = self._escape_for_html_attribute(f"Description: {desc_content}")
            else:
                display_content = f"<em>Description: {desc_content}</em>"
        else:
            display_content = "No content available" if for_tooltip else "<em>No content</em>"

        return display_content

    def _escape_for_html_attribute(self, text: str) -> str:
        """Escape text for safe use in HTML attributes."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
            .replace("\n", "<br>")
        )

    def render_document(self, parsed_result: Any) -> None:
        """Render a parsed document with visual annotations.

        Args:
            parsed_result: The parsed document result from ai_parse_document
        """
        try:
            # Convert to dict
            if hasattr(parsed_result, "toPython"):
                parsed_dict = parsed_result.toPython()
            elif hasattr(parsed_result, "toJson"):
                parsed_dict = json.loads(parsed_result.toJson())
            elif isinstance(parsed_result, dict):
                parsed_dict = parsed_result
            else:
                display(
                    HTML(
                        f"<p style='color: red;'>❌ Could not convert result. Type: {type(parsed_result)}</p>"
                    )
                )
                return

            # Extract components
            document = parsed_dict.get("document", {})
            pages = document.get("pages", [])
            elements = document.get("elements", [])
            metadata = parsed_dict.get("metadata", {})

            if not elements:
                display(HTML("<p style='color: red;'>❌ No elements found in document</p>"))
                return

            # Display summary
            display(HTML("<h3>🔍 AI Parse Document Results</h3>"))
            display(
                HTML(
                    f"<p><strong>Pages:</strong> {len(pages)} | <strong>Elements:</strong> {len(elements)}</p>"
                )
            )

        except Exception as e:
            display(HTML(f"<p style='color: red;'>❌ Error: {str(e)}</p>"))
            import traceback

            display(HTML(f"<pre>{traceback.format_exc()}</pre>"))


def render_ai_parse_output(parsed_result):
    """Simple function to render ai_parse_document output.

    Args:
        parsed_result: The parsed document result
    """
    renderer = DocumentRenderer()
    renderer.render_document(parsed_result)
