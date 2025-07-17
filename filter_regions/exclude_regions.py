import pandas as pd
from pathlib import Path
from region_utils import load_region_definitions, get_region_info

def main():
    """
    Main function to filter a CSV file by excluding specified regions.
    It reads a data file, identifies the region for each row based on the
    'debtor_inn', and writes a new CSV containing only the rows from
    included regions.
    """
    base_dir = Path(__file__).parent
    
    # --- File Paths ---
    input_data_file = base_dir / 'res250714_400_filtered.csv'
    all_regions_file = base_dir / 'all_regions.csv'
    excluded_regions_file = base_dir / 'excluded_regions.csv'
    output_filtered_file = base_dir / 'filtered_regions.csv'
    
    # --- Load Region Definitions ---
    # We only need the code-to-name map and the set of excluded codes for this script.
    code_to_name, excluded_codes, _, _ = load_region_definitions(
        all_regions_file, excluded_regions_file
    )
    if code_to_name is None:
        print("Failed to load region definitions. Exiting.")
        return

    # --- Load and Process Data ---
    try:
        df = pd.read_csv(input_data_file, dtype={'debtor_inn': str})
        if 'debtor_inn' not in df.columns:
            print(f"Error: Input file {input_data_file} must contain a 'debtor_inn' column.")
            return
    except FileNotFoundError:
        print(f"Error: Input data file not found at {input_data_file}")
        return
    except Exception as e:
        print(f"An error occurred while reading the data file: {e}")
        return

    # --- Identify Region for Each Row ---
    # Apply the centralized get_region_info function
    region_info = df['debtor_inn'].apply(lambda inn: get_region_info(inn, code_to_name))
    df['matched_code'] = region_info.str[0]
    df['region_name'] = region_info.str[1]

    # --- Filter Out Excluded Regions ---
    # A row is kept if its matched code is NOT in the set of excluded codes
    initial_rows = len(df)
    filtered_df = df[~df['matched_code'].isin(excluded_codes)].copy()
    final_rows = len(filtered_df)

    # --- Save Filtered Data ---
    # Drop the temporary helper columns before saving
    filtered_df.drop(columns=['matched_code'], inplace=True)
    filtered_df.to_csv(output_filtered_file, index=False)

    print(f"Processing complete.")
    print(f"Initial rows: {initial_rows}")
    print(f"Excluded rows: {initial_rows - final_rows}")
    print(f"Final rows: {final_rows}")
    print(f"Filtered data saved to: {output_filtered_file}")


if __name__ == '__main__':
    main()