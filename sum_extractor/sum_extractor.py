import pandas as pd
import requests
import os
import json
import re
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

# --- Constants ---
API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL_NAME = "qwen/qwen3-235b-a22b-07-25"

# --- Caching Functions (Unchanged) ---

def load_cache(cache_path: Path) -> Dict:
    """Loads a generic cache from a JSON file."""
    if not cache_path.exists():
        print(f"[INFO] Cache file not found at {cache_path}. Starting fresh.")
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cache = json.load(f)
            print(f"[INFO] Loaded {len(cache)} items from cache: {cache_path}")
            return cache
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARNING] Could not read cache file {cache_path}: {e}. Starting with an empty cache.")
        return {}

def save_cache(cache_path: Path, data: Dict):
    """Saves a generic cache to a JSON file."""
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError as e:
        print(f"[ERROR] Could not save cache to {cache_path}: {e}")

# --- Data Parsing Function (Unchanged) ---

def parse_texts_from_cell(cell_content: str) -> List[Tuple[str, str]]:
    """
    Parses the 'links_texts' column content into a list of (date, text) tuples.
    """
    if not isinstance(cell_content, str) or not cell_content.strip():
        return []
    pattern = re.compile(r"^(\d{2}\.\d{2}\.\d{4}):\s*(.*)", re.DOTALL)
    parsed_data = []
    for line in cell_content.splitlines():
        line = line.strip()
        if not line or "Failed to extract content" in line:
            continue
        match = pattern.match(line)
        if match:
            date, text = match.groups()
            if len(text.strip()) > 50: 
                parsed_data.append((date.strip(), text.strip()))
    return parsed_data

# --- AI Interaction Function (Unchanged) ---

def get_debt_from_ai(text: str) -> Optional[int]:
    """
    Sends text to the OpenRouter API and asks for the total debt sum.
    """
    if not API_KEY:
        print("[ERROR] OPENROUTER_API_KEY not found.")
        return None
    prompt = f"""
посчитай сумму долга в решении суда. Ответ в виде числа без копеек и наименования валюты. Верни ИТОГОВУЮ ОБЩУЮ СУММУ.

Требования к ответу:
- Ответ должен быть ТОЛЬКО ОДНИМ ЧИСЛОМ.
- Без копеек (округли до ближайшего целого).
- Без текста, без объяснений, без символа рубля или пробелов.
- Пример правильного ответа: 123456

Текст решения для анализа:
"{text}"
"""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    payload = {"model": MODEL_NAME, "messages": [{"role": "user", "content": prompt}]}
    try:
        response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        api_response = response.json()
        ai_result_text = api_response['choices'][0]['message']['content'].strip()
        cleaned_number = re.sub(r'\D', '', ai_result_text)
        if cleaned_number:
            return int(cleaned_number)
        else:
            print(f"    [AI_WARN] AI returned non-numeric text: '{ai_result_text}'")
            return None
    except requests.exceptions.RequestException as e:
        print(f"    [API_ERROR] Network error: {e}")
        return None
    except (KeyError, IndexError) as e:
        print(f"    [API_ERROR] Unexpected API response format: {e}")
        return None
    except Exception as e:
        print(f"    [API_ERROR] An unexpected error occurred: {e}")
        return None

# --- Helper function to process texts for a single pair (Unchanged) ---

def process_pair_texts(
    texts_to_process: List[Tuple[str, str]], 
    ai_cache: Dict[str, Optional[int]]
) -> Tuple[str, bool]:
    """
    Processes a list of (date, text) tuples for a single INN pair.
    Returns the formatted result string and a flag indicating if the AI cache was updated.
    """
    pair_results = []
    ai_cache_updated = False
    for date, text in texts_to_process:
        print(f"  - Analyzing text for date: {date}")
        if text in ai_cache and ai_cache[text] is not None: # Also retry if the cached result was a failure (None)
            print("    [AI CACHE HIT] Found text snippet in AI cache.")
            debt_sum = ai_cache[text]
        else:
            print("    [AI CACHE MISS or RETRY] Calling AI API...")
            debt_sum = get_debt_from_ai(text)
            ai_cache[text] = debt_sum
            ai_cache_updated = True

        if debt_sum is not None:
            pair_results.append(f"{date}: {debt_sum}")
            print(f"    [SUCCESS] Found debt sum: {debt_sum}")
        else:
            pair_results.append(f"{date}: AI_FAILED")
            print("    [FAILURE] Could not determine debt sum.")
            
    return "\n".join(pair_results), ai_cache_updated

# --- Main Execution Block (Refactored) ---

def main(input_path: str, output_path: str, results_cache_path: Path, ai_cache_path: Path):
    """
    Main function to run the process based on unique INN pairs.
    """
    try:
        df = pd.read_csv(input_path)
        if 'links_texts' not in df.columns or 'creditor_inn' not in df.columns or 'debtor_inn' not in df.columns:
            print("[ERROR] CSV must contain 'creditor_inn', 'debtor_inn', and 'links_texts' columns.")
            return
    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {input_path}")
        return

    results_cache: Dict[str, str] = load_cache(results_cache_path)
    ai_cache: Dict[str, Optional[int]] = load_cache(ai_cache_path)

    df['pair_key'] = df['creditor_inn'].astype(str) + '|' + df['debtor_inn'].astype(str)
    unique_pairs_df = df.dropna(subset=['links_texts']).drop_duplicates(subset=['pair_key'])
    pairs_to_process_map = dict(zip(unique_pairs_df['pair_key'], unique_pairs_df['links_texts']))

    # Filter for pairs that are either NOT in the cache OR have a failed result in the cache.
    new_pairs_to_process = {
        key: text for key, text in pairs_to_process_map.items() 
        if key not in results_cache or "AI_FAILED" in results_cache.get(key, "") # <<< MODIFIED LINE
    }

    if not new_pairs_to_process:
        print("\n[INFO] All unique pairs are already successfully processed. No new work to do.")
    else:
        total_new_pairs = len(new_pairs_to_process)
        print(f"\n[INFO] Found {total_new_pairs} new or failed pairs to process.")
        
        for i, (key, links_texts_content) in enumerate(new_pairs_to_process.items()):
            print(f"\n--- Processing Pair {i + 1}/{total_new_pairs}: {key} ---")
            
            texts_for_pair = parse_texts_from_cell(links_texts_content)
            if not texts_for_pair:
                print("  No valid texts found for this pair. Skipping.")
                results_cache[key] = "NO_VALID_TEXTS"
                continue

            final_result_string, ai_cache_was_updated = process_pair_texts(texts_for_pair, ai_cache)
            
            results_cache[key] = final_result_string
            save_cache(results_cache_path, results_cache)
            print(f"  [RESULTS CACHE SAVED] Progress for pair {key} saved.")

            if ai_cache_was_updated:
                save_cache(ai_cache_path, ai_cache)
                print("  [AI CACHE SAVED] AI text cache updated.")

    print("\n[INFO] Mapping results back to the DataFrame...")
    df['ai_debt_sums'] = df['pair_key'].map(results_cache)
    df.drop(columns=['pair_key'], inplace=True)

    df.to_csv(output_path, index=False)
    print(f"\n--- All Done! ---\nResults saved to: {output_path}")


if __name__ == "__main__":
    this_folder = Path(__file__).parent
    input_file = this_folder / "filtered_rdsng_1_pdf.csv"
    output_file = this_folder / "rdsng_1.1_sum.csv"
    
    # Define two separate cache files
    results_cache_file = this_folder / "rdsng_1_sum_pair_cache.json" # Caches final results per INN pair
    ai_cache_file = this_folder / "rdsng_1_sum_ai_text_cache.json" # Caches results per unique text snippet

    results_cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    main(
        input_path=input_file, 
        output_path=output_file, 
        results_cache_path=results_cache_file,
        ai_cache_path=ai_cache_file
    )