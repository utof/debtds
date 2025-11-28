import pandas as pd
import requests
import re
import json # NEW: For handling JSON cache file
from pathlib import Path # NEW: For handling file paths easily
from io import BytesIO
import PyPDF2
from typing import List, Tuple, Optional, Dict
from pdf_session import PDFSession
from collections import defaultdict
import regex as re_regex

# --- NEW: Caching Functions ---

def load_cache(cache_path: Path) -> Dict[str, str]:
    """Loads the results cache from a JSON file."""
    if not cache_path.exists():
        print(f"[INFO] Cache file not found at {cache_path}. Starting with an empty cache.")
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
            print(f"[INFO] Successfully loaded {len(cache)} cached results from {cache_path}.")
            return cache
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARNING] Could not read or parse cache file {cache_path}: {e}. Starting fresh.")
        return {}

def save_cache(cache_path: Path, data: Dict[str, str]):
    """Saves the cache to a JSON file."""
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            # indent=4 makes the JSON file human-readable
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError as e:
        print(f"[ERROR] Could not save cache to {cache_path}: {e}")


# --- Helper Functions (unchanged) ---

def parse_links(cell: str) -> List[Tuple[Optional[str], str]]:
    """Parse a cell containing links in 'dd.mm.yyyy: link' format with \r or \n line endings."""
    if not isinstance(cell, str) or not cell.strip():
        return []
    
    links = []
    for line in re.split(r'[\r\n]+', cell.strip()):
        line = line.strip()
        if not line:
            continue
        if ": " in line and "http" in line.split(": ", 1)[1]:
            date, url = line.split(": ", 1)
            links.append((date.strip(), url.strip()))
        else:
            links.append((None, line.strip()))
    return links

def extract_decision_text(pdf_text: Optional[str]) -> Optional[str]:
    """Extracts the main decision text from a PDF, trying multiple patterns."""
    if not pdf_text:
        return None
    
    patterns = [
        r"(?i)(?:Р\s*Е\s*Ш\s*И\s*Л)\s*[:\.\-–—]*\s*(.*?)(?=\s*(?:С\s*У\s*Д\s*Ь\s*Я|$))",
        r"(?i)(?:Р\s*Е\s*Ш\s*И\s*Л)\s*(.*)",
        r"(.*)"
    ]
    
    for pattern in patterns:
        match = re_regex.search(pattern, pdf_text, re_regex.DOTALL)
        if match:
            text = match.group(1).strip()
            if text:
                return text
    
    return pdf_text

def format_output(date: Optional[str], text: Optional[str]) -> str:
    """Formats the output text with a date prefix and flattens the text to a single line."""
    if not text:
        flat_text = "Failed to extract content"
    else:
        flat_text = " ".join(text.strip().splitlines())

    if not date:
        return flat_text
    if date == "???.??.????":
        return f"{date}: {flat_text}"
    try:
        pd.to_datetime(date, format="%d.%m.%Y")  # validate format
        return f"{date}: {flat_text}"
    except ValueError:
        return f"???.??.????: {flat_text}"

# --- Core Processing Logic (unchanged) ---

def process_links_for_pair(links_to_process: List[Tuple[Optional[str], str]], session: PDFSession) -> str:
    """
    Takes a list of (date, link) tuples for a single unique pair,
    fetches content for each, and returns a single formatted string.
    """
    extracted_texts = []
    for date, link in links_to_process:
        print(f"    [LINK] Processing: {link}")
        pdf_text = session.fetch_pdf_content(link)
        decision_text = extract_decision_text(pdf_text)
        formatted = format_output(date or "???.??.????", decision_text)
        extracted_texts.append(formatted)
    
    return "\n".join(extracted_texts)

# --- MODIFIED: Main function now uses caching ---

def main(input_path: str, output_path: str, cache_path: Path) -> None:
    """
    Main function to read CSV, process links for unique creditor/debtor pairs,
    and save the results to a new CSV, using a persistent cache.
    """
    # 1. Load data
    try:
        df = pd.read_csv(input_path)
        required_cols = ['creditor_inn', 'debtor_inn', 'links']
        if not all(col in df.columns for col in required_cols):
            print(f"[ERROR] Input CSV must contain the columns: {required_cols}")
            return
    except FileNotFoundError:
        print(f"[ERROR] Input file not found at: {input_path}")
        return
    except Exception as e:
        print(f"[ERROR] Failed to read CSV: {e}")
        return

    # 2. Load the cache of already processed pairs
    results_cache = load_cache(cache_path)

    # 3. Create a unique key and aggregate all links for each unique pair
    df['pair_key'] = df['creditor_inn'].astype(str) + '|' + df['debtor_inn'].astype(str)
    df_with_links = df[df['links'].astype(str).str.contains("https://", na=False)].copy()
    
    pair_to_links = defaultdict(list)
    for _, row in df_with_links.iterrows():
        key = row['pair_key']
        links_in_cell = parse_links(row['links'])
        pair_to_links[key].extend(links_in_cell)

    # 4. Deduplicate links and identify which pairs need processing
    unique_pair_links: Dict[str, List[Tuple[Optional[str], str]]] = {}
    for key, links in pair_to_links.items():
        unique_links_dict = {url: (date, url) for date, url in links}
        unique_pair_links[key] = list(unique_links_dict.values())

    # Filter out pairs that are already in the cache
    pairs_to_process = {
        key: links for key, links in unique_pair_links.items() if key not in results_cache
    }

    # 5. Process only the new pairs
    if not pairs_to_process:
        print("[INFO] All pairs with links are already in the cache. No new processing needed.")
    else:
        session = PDFSession(wait_sec=5, headless=False)
        try:
            total_new_pairs = len(pairs_to_process)
            print(f"[INFO] Found {len(unique_pair_links)} total pairs. {total_new_pairs} are new and will be processed.")
            
            for i, (key, links) in enumerate(pairs_to_process.items()):
                print(f"\n[INFO] Processing new pair {i+1}/{total_new_pairs}: {key} ({len(links)} unique link(s))")
                combined_text = process_links_for_pair(links, session)
                
                # Add result to the in-memory cache
                results_cache[key] = combined_text
                
                # Save the updated cache to disk immediately after processing the pair
                save_cache(cache_path, results_cache)
                print(f"    [CACHE] Progress for pair {key} saved to {cache_path}")
        finally:
            session.close()

    # 6. Map results from the (now complete) cache back to the original DataFrame
    print("\n[INFO] Mapping all results back to DataFrame...")
    df['links_texts'] = df['pair_key'].map(results_cache)
    
    # 7. Clean up and save the final DataFrame
    df.drop(columns=['pair_key'], inplace=True)
    df.to_csv(output_path, index=False)
    print(f"[INFO] Processing complete. Output saved to {output_path}")

if __name__ == "__main__":
    # Define file paths
    input_path = "link_decider/filtered_rdsng_1_output.csv"
    output_path = "link_decider/filtered_rdsng_1_pdf.csv"
    
    # NEW: Define the path for the JSON cache file
    cache_path = Path("link_decider/pdf_content_cache.json")
    
    # Create the directory for the cache if it doesn't exist
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    main(input_path, output_path, cache_path)