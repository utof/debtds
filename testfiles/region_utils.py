import pandas as pd
from collections import defaultdict

def load_region_definitions(all_regions_path, excluded_regions_path):
    """
    Loads region definitions from CSV files.

    Creates a mapping from every possible region code (e.g., '23', '93', '123') 
    to its region name, a set of all codes that should be excluded, and a map
    from region names back to all their associated codes.

    Returns:
        tuple: A tuple containing:
            - code_to_name_map (dict): Maps region code to region name.
            - excluded_codes (set): A set of excluded region codes.
            - name_to_codes_map (dict): Maps region name to a list of its codes.
            - all_regions_df (pd.DataFrame): DataFrame from all_regions.csv.
    """
    try:
        all_regions_df = pd.read_csv(all_regions_path)
        excluded_regions_df = pd.read_csv(excluded_regions_path)
    except FileNotFoundError as e:
        print(f"Error: Required region definition file not found. {e}")
        return None, None, None, None

    code_to_name_map = {}
    name_to_codes_map = defaultdict(list)
    # Process all regions to build the primary code-to-name mapping
    for _, row in all_regions_df.iterrows():
        region_name = row['region']
        # Handle potentially non-string 'inns' column and split
        codes = [code.strip() for code in str(row['inns']).split(',') if code.strip()]
        for code in codes:
            code_to_name_map[code] = region_name
            name_to_codes_map[region_name].append(code)

    # Process excluded regions to build the set of excluded codes
    excluded_codes = set()
    for _, row in excluded_regions_df.iterrows():
        codes = [code.strip() for code in str(row['inns']).split(',') if code.strip()]
        for code in codes:
            excluded_codes.add(code)
    
    return code_to_name_map, excluded_codes, name_to_codes_map, all_regions_df

def get_region_info(inn_raw, code_to_name_map):
    """
    Determines region code and name from an INN based on its prefix.

    It handles 9-digit INNs by padding with a leading zero and prioritizes 
    longer matching prefixes (3-digit codes over 2-digit codes).

    Args:
        inn_raw (str or int): The INN to process.
        code_to_name_map (dict): A dictionary mapping region codes to region names.

    Returns:
        tuple: A tuple of (matched_code, region_name). Returns ('Invalid', 'Invalid INN')
               for non-compliant INNs.
    """
    inn = str(inn_raw)
    # Handle 9-digit INNs by prepending a '0'
    if len(inn) == 9 and inn.isdigit():
        inn = '0' + inn
    
    # INN must be exactly 10 digits
    if not inn.isdigit() or len(inn) != 10:
        return "Invalid", "Invalid INN"
    
    # Check for a 3-digit prefix first, as it's more specific
    prefix_3 = inn[:3]
    if prefix_3 in code_to_name_map:
        return prefix_3, code_to_name_map[prefix_3]
            
    # If no 3-digit match, check for a 2-digit prefix
    prefix_2 = inn[:2]
    if prefix_2 in code_to_name_map:
        return prefix_2, code_to_name_map[prefix_2]

    # If no match is found at all
    return "Unknown", "Unknown Code"