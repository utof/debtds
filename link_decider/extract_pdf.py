import pandas as pd
import requests
import re
from io import BytesIO
import PyPDF2
from typing import List, Tuple, Optional
from functools import reduce
from pdf_session import PDFSession  # or paste the class in same file
from collections import defaultdict

def read_csv(file_path: str) -> pd.DataFrame:
    """Read a CSV file into a Pandas DataFrame."""
    return pd.read_csv(file_path)

def get_first_n_with_links(csv_path: str, n: int) -> pd.DataFrame:
    """Return the first `n` rows from the CSV that have at least one 'https://' link in the 'links' column."""
    df = pd.read_csv(csv_path)
    has_link = df['links'].astype(str).str.contains("https://", na=False)
    return df[has_link].head(n).reset_index(drop=True)  # Reset index to ensure sequential indices

def parse_links(cell: str) -> List[Tuple[Optional[str], str]]:
    """Parse a cell containing links in 'dd.mm.yyyy: link' or 'link' format."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    links = []
    for line in cell.strip().split("\n"):
        if ": " in line:
            date, url = line.split(": ", 1)
            links.append((date, url))
        else:
            links.append((None, line))
    return links

def extract_links(df: pd.DataFrame, column: str = 'links') -> List[Tuple[int, Optional[str], str]]:
    """Extract URLs from the specified column, preserving row index and date."""
    return [(i, date, link) for i, links in df[column].items() for date, link in parse_links(links)]


import regex as re  # not re — use `pip install regex`

def extract_decision_text(pdf_text: Optional[str]) -> Optional[str]:
    """
    Extract the part after 'РЕШИЛ' (with arbitrary spaces between letters),
    and ending before 'судья' (with arbitrary spacing). Case-insensitive.
    """
    if not pdf_text:
        return None

    # R\s*E\s*Ш\s*И\s*Л → handles Р Е Ш И Л and РЕШИЛ and messy variants
    # same for C У Д Ь Я at the end
    pattern = (
        r"(?i)"                          # case-insensitive
        r"(?:Р\s*Е\s*Ш\s*И\s*Л)"         # fuzzy "РЕШИЛ"
        r"[\s:\.\-–—]*"                  # separator chars like ":" or space
        r"(.*?)"                         # lazily capture content
        r"(?:\n?.{0,15}С\s*У\s*Д\s*Ь\s*Я)"  # fuzzy "судья" anchor
    )

    match = re.search(pattern, pdf_text, re.DOTALL)
    return match.group(1).strip() if match else None

def initialize_output_csv(input_df: pd.DataFrame, output_path: str) -> None:
    """Initialize the output CSV with the same structure as input plus 'links_texts'."""
    output_df = input_df.copy()
    output_df['links_texts'] = pd.NA
    output_df.to_csv(output_path, index=False)

def format_output(date: Optional[str], text: Optional[str]) -> str:
    """Format the output text based on date availability."""
    text = text if text else "Failed to extract content"
    if not date:
        return text
    if date == "???.??.????":
        return f"{date}: {text}"
    try:
        pd.to_datetime(date, format="%d.%m.%Y")  # Validate date format
        return f"{date}: {text}"
    except ValueError:
        return f"???.??.????: {text}"

def append_to_csv(output_path: str, row_index: int, date: Optional[str], link: str, text: Optional[str]) -> None:
    output_df = pd.read_csv(output_path)
    formatted_text = text if text else "Failed to extract content"
    if pd.isna(output_df.at[row_index, 'links_texts']):
        output_df.at[row_index, 'links_texts'] = formatted_text
    else:
        output_df.at[row_index, 'links_texts'] += f"\n{formatted_text}"
    output_df.to_csv(output_path, index=False)

def process_pdf_links(links: List[Tuple[int, Optional[str], str]], output_path: str) -> None:
    """Process PDF links, group results by row, and write incrementally to CSV."""
    # session = PDFSession(wait_sec=15, sleep_range=(1,2), headless=False)
    session = PDFSession(wait_sec=15, headless=False)

    # Group links by row index
    grouped_links = defaultdict(list)
    for row_index, date, link in links:
        grouped_links[row_index].append((date, link))

    try:
        for row_index, date_links in grouped_links.items():
            print(f"[INFO] Row {row_index} → {len(date_links)} link(s)")
            extracted_texts = []

            for date, link in date_links:
                print(f"    [LINK] {link}")
                pdf_text = session.fetch_pdf_content(link)
                decision_text = extract_decision_text(pdf_text)
                formatted = format_output(date or "???.??.????", decision_text)
                extracted_texts.append(formatted)

            full_text = "\n".join(extracted_texts)
            append_to_csv(output_path, row_index, None, "", full_text)
    finally:
        session.close()

def main(input_path: str, output_path: str) -> None:
    """Main function to read CSV, extract links, process PDFs, and save incrementally."""
    df = get_first_n_with_links(input_path, 30)  # Adjust n as needed
    initialize_output_csv(df, output_path)
    links = extract_links(df)
    if not links:
        return
    process_pdf_links(links, output_path)

if __name__ == "__main__":
    # Example usage
    input_path = "link_decider\\deleted_columns.csv"  # Replace with your input CSV path
    output_path = "link_decider\\deleted_columns_output.csv"  # Replace with your output CSV path
    main(input_path, output_path)