from pathlib import Path
import pandas as pd

def filter_and_sort_by_summa(input_file, output_file):
    df = pd.read_csv(input_file, dtype=str).fillna('')
    # Keep only rows where 'group_summa' is empty or missing
    filtered_df = df[(df['group_summa'] == '') | (df['group_summa'].isna())]
    # Convert 'summa' to numeric for sorting
    filtered_df['summa'] = pd.to_numeric(filtered_df['summa'], errors='coerce')
    sorted_df = filtered_df.sort_values(by='summa', ascending=False)
    sorted_df.to_csv(output_file, index=False)
    print(f"Filtered and sorted data saved to {output_file}")

if __name__ == "__main__":
    this_folder = Path(__file__).parent
    input_file = this_folder / "filtered_regions.csv"
    output_file = this_folder / "filtered_regions_descend_summa_no_groups.csv"
    filter_and_sort_by_summa(input_file, output_file)
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
