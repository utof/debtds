import pandas as pd
import plotly.express as px
from pathlib import Path
from region_utils import load_region_definitions, get_region_info

def create_visualizations(data_path, all_regions_path, excluded_regions_path, output_dir):
    """
    Generates interactive bar charts visualizing the count of entries per region,
    separated into 'Included' and 'Excluded' categories.
    """
    # --- Load Region Definitions using the utility function ---
    code_to_name, excluded_codes, _, _ = load_region_definitions(
        all_regions_path, excluded_regions_path
    )
    if code_to_name is None:
        print("Failed to load region definitions. Exiting visualization.")
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

    # --- Aggregate Data for Visualization ---
    # Group by region name and status, then sum the number of matches
    region_counts = df.groupby(['region_name', 'status'])['debtor_inn'].count().reset_index()
    region_counts.rename(columns={'debtor_inn': 'count'}, inplace=True)

    # --- Separate Included and Excluded data ---
    excluded_df = region_counts[region_counts['status'] == 'Excluded'].sort_values('count', ascending=False)
    included_df = region_counts[region_counts['status'] == 'Included'].sort_values('count', ascending=False)

    # --- Create and Save Visualizations ---
    output_dir.mkdir(exist_ok=True) # Ensure the output directory exists

    # 1. Excluded Regions Chart
    if not excluded_df.empty:
        fig_excluded = px.bar(
            excluded_df,
            x='region_name',
            y='count',
            title='Number of Rows Removed per Excluded Region',
            labels={'region_name': 'Region', 'count': 'Number of Rows Removed'},
            color='region_name'
        )
        fig_excluded.update_layout(showlegend=False)
        excluded_chart_path = output_dir / 'excluded_regions_impact.html'
        fig_excluded.write_html(excluded_chart_path)
        print(f"Excluded regions chart saved to: {excluded_chart_path}")
    else:
        print("No excluded regions found in the data to visualize.")

    # 2. Included Regions Chart
    if not included_df.empty:
        fig_included = px.bar(
            included_df,
            x='region_name',
            y='count',
            title='Number of Rows Kept per Included Region',
            labels={'region_name': 'Region', 'count': 'Number of Rows Kept'},
            color='region_name'
        )
        fig_included.update_layout(showlegend=False)
        included_chart_path = output_dir / 'included_regions_distribution.html'
        fig_included.write_html(included_chart_path)
        print(f"Included regions chart saved to: {included_chart_path}")
    else:
        print("No included regions found in the data to visualize.")


def main():
    """Main function to define file paths and run the visualization generator."""
    base_dir = Path(__file__).parent
    
    input_data_file = base_dir / 'res250714_400_filtered.csv'
    all_regions_file = base_dir / 'all_regions.csv'
    excluded_regions_file = base_dir / 'excluded_regions.csv'
    output_directory = base_dir / 'visualizations'
    
    create_visualizations(input_data_file, all_regions_file, excluded_regions_file, output_directory)


if __name__ == '__main__':
    main()