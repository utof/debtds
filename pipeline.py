import os
from fssp050_api_calls import main as run_api
from fssp060_append_w_sum_recispdoc import extract_sum_recispdoc_from_csv, add_sum_recispdoc_to_csv
from fssp070_sum_filter import process_csv

dir_name = 'pipeline_output'
def create_pipeline_dirs():
    """Create directory structure for organized output"""
    os.makedirs(dir_name, exist_ok=True)

def pipeline():
    """Execute the full data processing pipeline"""
    # Step 1: Make API calls and save raw responses
    # In simple_api_fssp.py, comment out lines 73-74 and set:
    # INPUT_CSV_PATH = "example.csv"
    # OUTPUT_CSV_PATH = f'{dir_name}/output.csv' # change this right now r: OK
    run_api()  # Requires modified simple_api_fssp.py to use pipeline paths
    
    # Step 2: Extract sum and recispdoc from responses
    # In append_w_sum_recispdoc.py, comment out lines 37-40
    info = extract_sum_recispdoc_from_csv(f'{dir_name}/output.csv') 
    
    # Step 3: Append extracted data to original CSV
    add_sum_recispdoc_to_csv('example.csv', info, f'{dir_name}/fssp_2.csv')
    
    # Step 4: Process and filter the data
    # In 3fssp_sum_filter.py, comment out lines 47-49
    process_csv(f'{dir_name}/fssp_2.csv', f'{dir_name}/3fssp.csv', 
                filter_sum=True, include_groupsum=True)

if __name__ == "__main__":
    create_pipeline_dirs()
    pipeline()