import pandas as pd
import requests
import re
from io import BytesIO
import PyPDF2
from typing import List, Tuple, Optional
from functools import reduce

def read_csv(file_path: str) -> pd.DataFrame:
    """Read a CSV file into a Pandas DataFrame."""
    return pd.read_csv(file_path)


def get_first_n_with_links(csv_path: str, n: int) -> pd.DataFrame:
    """Return the first `n` rows from the CSV that have at least one 'https://' link in the 'links' column."""
    df = pd.read_csv(csv_path)
    has_link = df['links'].astype(str).str.contains("https://", na=False)
    return df[has_link].head(n)

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

def fetch_pdf_content(url: str) -> Optional[str]:
    """Fetch PDF text using Selenium to bypass CAPTCHA on kad.arbitr.ru."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
        import tempfile

        # Configure headless Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=ru-RU")
        chrome_options.add_argument("user-agent=Mozilla/5.0")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=chrome_options
        )
        driver.get(url)

        # Wait for CAPTCHA to auto-complete (if it does)
        import time
        time.sleep(15)

        # Get cookies and close browser
        cookies = driver.get_cookies()
        driver.quit()

        # Use cookies to fetch the PDF
        import requests
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        response = session.get(url, timeout=20)
        response.raise_for_status()
        if response.headers.get("Content-Type", "").lower() != "application/pdf":
            return None

        # Parse PDF text
        from PyPDF2 import PdfReader
        from io import BytesIO
        reader = PdfReader(BytesIO(response.content))
        return "\n".join([page.extract_text() or "" for page in reader.pages])
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return None


def extract_decision_text(pdf_text: Optional[str]) -> Optional[str]:
    """Extract text between 'РЕШИЛ:' and 'судья' (case-insensitive)."""
    if not pdf_text:
        return None
    pattern = r"(?i)РЕШИЛ:(.*?)(?:судья|$)"
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
    """Append or update a row in the output CSV with the extracted text."""
    output_df = pd.read_csv(output_path)
    formatted_text = format_output(date if date else "???.??.????", text)
    if pd.isna(output_df.at[row_index, 'links_texts']):
        output_df.at[row_index, 'links_texts'] = formatted_text
    else:
        current = output_df.at[row_index, 'links_texts']
        output_df.at[row_index, 'links_texts'] = f"{current}\n{formatted_text}"
    output_df.to_csv(output_path, index=False)

def process_pdf_links(links: List[Tuple[int, Optional[str], str]], output_path: str) -> None:
    """Process a list of PDF links and save results incrementally to CSV."""
    for row_index, date, link in links:
        pdf_text = fetch_pdf_content(link)
        decision_text = extract_decision_text(pdf_text)
        append_to_csv(output_path, row_index, date, link, decision_text)

def main(input_path: str, output_path: str) -> None:
    """Main function to read CSV, extract links, process PDFs, and save incrementally."""
    df = get_first_n_with_links(input_path, 30)  # Adjust n as needed
    # df = read_csv(input_path)
    initialize_output_csv(df, output_path)
    links = extract_links(df)
    if not links:
        return
    process_pdf_links(links, output_path)

if __name__ == "__main__":
    # Example usage
    input_path = "link_decider\\data_.csv"  # Replace with your input CSV path
    output_path = "link_decider\\output.csv"  # Replace with your output CSV path
    main(input_path, output_path)