import pandas as pd
import json
import re
from pathlib import Path
from typing import List, Dict, Optional, Any

# --- Caching Functions (Reused from previous scripts) ---

def load_cache(cache_path: Path) -> Dict[str, str]:
    """Loads the results cache from a JSON file."""
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

def save_cache(cache_path: Path, data: Dict[str, str]):
    """Saves the cache to a JSON file."""
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except IOError as e:
        print(f"[ERROR] Could not save cache to {cache_path}: {e}")

# --- Data Parsing Functions ---

def parse_key_value_lines(cell_content: str) -> Dict[str, str]:
    """Parses a cell with 'key: value' lines into a dictionary."""
    if not isinstance(cell_content, str):
        return {}
    
    data_dict = {}
    # This regex handles dates and potential leading text before the date.
    pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4}):\s*(.*)")
    for line in cell_content.splitlines():
        match = pattern.search(line.strip())
        if match:
            key, value = match.groups()
            data_dict[key.strip()] = value.strip()
    return data_dict

def parse_sum_value(sum_str: str) -> Optional[int]:
    """Safely converts a string from 'ai_debt_sums' into an integer."""
    if not sum_str or "AI_FAILED" in sum_str:
        return None
    cleaned_number = re.sub(r'\D', '', sum_str)
    return int(cleaned_number) if cleaned_number else None

# --- Core Decision Logic Function ---

def select_best_decision(summa_ip: float, decision_data: List[Dict[str, Any]]) -> str:
    """
    Applies the decision logic to select the best link(s).

    Args:
        summa_ip: The target sum from the 'summa' column.
        decision_data: A list of dicts, e.g., 
                       [{'date': '...', 'link': '...', 'sum': 12345}, ...]
    """
    if not decision_data:
        return "No decision data found to process."
    
    # Filter for decisions where the AI successfully found a sum
    valid_decisions = [d for d in decision_data if d.get('sum') is not None]

    if not valid_decisions:
        return "No valid decision sums found (all failed AI)."

    # Case 1: Only one valid decision was found
    if len(valid_decisions) == 1:
        decision = valid_decisions[0]
        diff_percent = abs(decision['sum'] - summa_ip) / summa_ip * 100
        
        comment = ""
        if diff_percent > 35:
            comment = " (Найдена единственная ссылка. Разница в сумме судебного решения и ИП более 35%.)"
            
        return f"{decision['date']}: {decision['link']}{comment}"

    # Case 2: Multiple valid decisions, find the one with the minimum difference
    if len(valid_decisions) > 1:
        min_diff = float('inf')
        best_decision = None

        for decision in valid_decisions:
            diff = abs(decision['sum'] - summa_ip)
            if diff < min_diff:
                min_diff = diff
                best_decision = decision
        
        # The prompt implies only one final link is chosen.
        # If multiple are needed, the logic here would change.
        return f"{best_decision['date']}: {best_decision['link']}"

    return "Logic error: This state should not be reachable."


# --- Main Execution Block ---

def main(input_path: str, output_path: str, cache_path: Path):
    """
    Main function to run the decision selection process.
    """
    try:
        df = pd.read_csv(input_path)
        required_cols = ['summa', 'creditor_inn', 'debtor_inn', 'links', 'ai_debt_sums']
        if not all(col in df.columns for col in required_cols):
            print(f"[ERROR] CSV must contain all required columns: {required_cols}")
            return
    except FileNotFoundError:
        print(f"[ERROR] Input file not found: {input_path}")
        return

    results_cache = load_cache(cache_path)

    df['pair_key'] = df['creditor_inn'].astype(str) + '|' + df['debtor_inn'].astype(str)
    df['final_decision_link'] = pd.NA

    # Get unique pairs to process
    unique_pairs_df = df.dropna(subset=['links', 'ai_debt_sums']).drop_duplicates(subset=['pair_key'])
    
    pairs_to_process = [
        (row['pair_key'], row['summa'], row['links'], row['ai_debt_sums'])
        for _, row in unique_pairs_df.iterrows()
        if row['pair_key'] not in results_cache
    ]

    if not pairs_to_process:
        print("\n[INFO] All unique pairs are already in the cache. No new processing needed.")
    else:
        total_new = len(pairs_to_process)
        print(f"\n[INFO] Found {total_new} new unique pairs to process.")
        
        for i, (key, summa_ip, links_cell, sums_cell) in enumerate(pairs_to_process):
            print(f"\n--- Processing Pair {i + 1}/{total_new}: {key} ---")
            
            # 1. Parse all the data sources
            links_map = parse_key_value_lines(links_cell)
            sums_map = parse_key_value_lines(sums_cell)
            
            # 2. Combine into a unified data structure
            decision_data = []
            for date, link in links_map.items():
                sum_str = sums_map.get(date)
                decision_sum = parse_sum_value(sum_str)
                decision_data.append({'date': date, 'link': link, 'sum': decision_sum})

            # 3. Apply the logic
            if pd.isna(summa_ip):
                final_decision = "Summa IP is missing."
            else:
                final_decision = select_best_decision(float(summa_ip), decision_data)
            
            print(f"  [RESULT] Selected: {final_decision}")
            
            # 4. Cache and save
            results_cache[key] = final_decision
            save_cache(cache_path, results_cache)
            print("  [CACHE SAVED] Progress saved to disk.")

    # Map results back to the entire DataFrame
    print("\n[INFO] Mapping results back to the DataFrame...")
    df['final_decision_link'] = df['pair_key'].map(results_cache)
    df.drop(columns=['pair_key'], inplace=True)

    df.to_csv(output_path, index=False)
    print(f"\n--- All Done! ---\nFinal results saved to: {output_path}")


if __name__ == "__main__":
    this_folder = Path(__file__).parent
    # This script takes the output of the previous one as its input
    input_file = this_folder / "rdsng_1.1_sum.csv"
    output_file = this_folder / "rdsng_1_decisions.csv"
    cache_file = this_folder / "rdsng_1_decisions_cache.json"

    cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    main(input_path=input_file, output_path=output_file, cache_path=cache_file)