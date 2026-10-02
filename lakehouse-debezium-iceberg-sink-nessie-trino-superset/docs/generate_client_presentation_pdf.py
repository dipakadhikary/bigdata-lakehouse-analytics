"""
Generate Smart Data Platform Client Presentation PDF from HTML.
Uses Microsoft Edge or Google Chrome headless print-to-PDF.

Usage:
    python generate_client_presentation_pdf.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parent
HTML = DOCS_DIR / "client-presentation.html"
OUTPUT = DOCS_DIR / "Smart-Data-Platform-Client-Presentation.pdf"

BROWSERS = [
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
]


def find_browser() -> Path | None:
    for browser in BROWSERS:
        if browser.exists():
            return browser
    return None


def generate_with_browser(browser: Path) -> None:
    html_uri = HTML.resolve().as_uri()
    cmd = [
        str(browser),
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--run-all-compositor-stages-before-draw",
        "--virtual-time-budget=10000",
        f"--print-to-pdf={OUTPUT.resolve()}",
        "--no-pdf-header-footer",
        html_uri,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(f"Browser PDF failed (exit {result.returncode}): {result.stderr}")


def generate_with_fpdf() -> None:
    """Fallback if no browser available."""
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos

    pdf = FPDF()
    pdf.set_auto_page_break(True, margin=12)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "Smart Data Platform - Client Presentation", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "", 9)
    pdf.multi_cell(0, 5, (
        "Please open docs/client-presentation.html in Chrome or Edge and use "
        "Print > Save as PDF for the full formatted document. "
        "Install Chrome/Edge headless support was not available on this machine."
    ))
    pdf.output(str(OUTPUT))


def main() -> None:
    if not HTML.exists():
        print(f"ERROR: HTML not found: {HTML}", file=sys.stderr)
        sys.exit(1)

    browser = find_browser()
    if browser:
        print(f"Using browser: {browser.name}")
        generate_with_browser(browser)
        time.sleep(2)
    else:
        print("No Chrome/Edge found. Using basic fallback PDF.")
        generate_with_fpdf()

    if not OUTPUT.exists():
        print(f"ERROR: PDF was not created at {OUTPUT}", file=sys.stderr)
        sys.exit(1)

    size_kb = OUTPUT.stat().st_size / 1024
    print(f"PDF generated: {OUTPUT}")
    print(f"File size: {size_kb:.1f} KB")


if __name__ == "__main__":
    main()
