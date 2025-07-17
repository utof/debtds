import pandas as pd
from pathlib import Path
from region_utils import load_region_definitions, get_region_info

def generate_report(data_path, all_regions_path, excluded_regions_path, report_output_path):
    """
    Analyzes a data file and generates a comprehensive report on included,
    excluded, and missing regions, using the centralized region utility.
    """
    # --- Load Region Definitions using the utility function ---
    code_to_name, excluded_codes, name_to_codes, all_regions_df = load_region_definitions(
        all_regions_path, excluded_regions_path
    )
    if code_to_name is None:
        print("Failed to load region definitions. Exiting report generation.")
        return

    # --- Load Data ---
    try:
        df = pd.read_csv(data_path, dtype={'debtor_inn': str})
        if 'debtor_inn' not in df.columns:
            print(f"Error: Input file {data_path} must contain a 'debtor_inn' column.")
            return
    except FileNotFoundError:
        print(f"Error: Input data file not found at {data_path}")
        return
    except Exception as e:
        print(f"An error occurred while reading the data file: {e}")
        return

    # --- Process Data using the utility function ---
    region_info = df['debtor_inn'].apply(lambda inn: get_region_info(inn, code_to_name))
    df['matched_code'] = region_info.str[0]
    df['region_name'] = region_info.str[1]
    df['status'] = df['matched_code'].apply(lambda x: 'Excluded' if x in excluded_codes else 'Included')

    # --- Aggregate for Reporting ---
    report_df = df.groupby(['region_name', 'matched_code', 'status']).agg(
        matches=('debtor_inn', 'size'),
        sample_debtor_inn=('debtor_inn', 'first')
    ).reset_index()

    # Add the column with all possible codes for each region
    report_df['all_region_codes'] = report_df['region_name'].map(name_to_codes).apply(
        lambda x: ', '.join(sorted(x)) if x else ''
    )
    
    # Reorder columns for the final report
    report_df = report_df[[
        'region_name', 'matched_code', 'all_region_codes', 'status', 'matches', 'sample_debtor_inn'
    ]]

    # --- Identify Missing Regions ---
    found_regions = set(report_df['region_name'])
    all_possible_regions = set(all_regions_df['region'])
    missing_regions = sorted(list(all_possible_regions - found_regions))

    # --- Format and Save Report ---
    report_df.sort_values(by=['status', 'region_name', 'matched_code'], inplace=True)
    
    with open(report_output_path, 'w', encoding='utf-8') as f:
        f.write("--- Comprehensive Region Report ---\n\n")
        
        f.write("--- Regions Found in Data File ---\n")
        f.write("This section shows all regions present in the input file, marked as 'Included' or 'Excluded'.\n\n")
        report_df.to_string(f, index=False)
        f.write("\n\n" + "="*80 + "\n\n")

        f.write("--- Regions NOT Found in Data File ---\n")
        f.write("This section lists regions from your master 'all_regions.csv' that had zero entries in the input file.\n\n")
        if not missing_regions:
            f.write("All regions from the master list were found in the data file.\n")
        else:
            for region in missing_regions:
                codes = name_to_codes.get(region, [])
                f.write(f"- {region} (Codes: {', '.join(sorted(codes))})\n")

    print(f"Comprehensive report has been generated and saved to:\n{report_output_path}")


def main():
    """Main function to define file paths and run the report generator."""
    base_dir = Path(__file__).parent
    
    input_data_file = base_dir / 'res250714_400_filtered.csv'
    all_regions_file = base_dir / 'all_regions.csv'
    excluded_regions_file = base_dir / 'excluded_regions.csv'
    output_report_file = base_dir / 'comprehensive_region_report.txt'
    
    generate_report(input_data_file, all_regions_file, excluded_regions_file, output_report_file)


if __name__ == '__main__':
    main()