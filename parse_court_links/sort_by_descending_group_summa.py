from pathlib import Path
import pandas as pd

def sort_by_descending_group_summa(input_file, output_file):
    df = pd.read_csv(input_file, dtype=str).fillna('')
    df['group_summa'] = pd.to_numeric(df['group_summa'], errors='coerce')
    sorted_df = df.sort_values(by='group_summa', ascending=False)
    sorted_df.to_csv(output_file, index=False)
    print(f"Sorted data saved to {output_file}")

if __name__ == "__main__":
    this_folder = Path(__file__).parent
    input = filtered_regions = this_folder / "filtered_regions.csv"
    output = this_folder / "filtered_regions_descending_groupsum.csv"
    sort_by_descending_group_summa(input, output)
    print(f"Input file: {input}")
    print(f"Output file: {output}")

