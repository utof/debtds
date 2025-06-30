import pandas as pd
import json
import re

def extract_sum_recispdoc_from_csv(csv_path):
    """Read a CSV file with 'fssp_resp' column, extract process_title, recispdoc name, and sum."""
    df = pd.read_csv(csv_path)
    result = {}
    for index, row in df.iterrows():
        fssp_resp = row['fssp_resp']
        data = json.loads(fssp_resp)
        record = data['records'][0]
        process_title = record['process_title']
        rec_isp_doc = record['recIspDoc']
        sum_value = record['sum']
        # Extract name between first number after "№ ФС" and next number
        match = re.search(r"№ ФС (?:№ )?(\d+) (.+?) (\d+)", rec_isp_doc)
        name = match.group(2) if match else "Unknown"
        result[process_title] = {"recispdoc": name, "sum": sum_value}
    return result

def add_sum_recispdoc_to_csv(input_csv_path, info_or_path, output_csv_path):
    """Add recispdoc and sum columns to a CSV based on ip from a JSON file or dict."""
    if isinstance(info_or_path, dict):
        info = info_or_path
    else:
        with open(info_or_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
    df = pd.read_csv(input_csv_path)
    if 'ip' not in df.columns:
        raise ValueError("Input CSV must have 'ip' column")
    df['recispdoc'] = df['ip'].map(lambda x: info.get(x, {}).get('recispdoc', 'Not Found'))
    df['sum'] = df['ip'].map(lambda x: info.get(x, {}).get('sum', 'Not Found'))
    df.to_csv(output_csv_path, index=False)

# Usage example:
info = extract_sum_recispdoc_from_csv('output300625_1.csv')
# with open('info_2.json', 'w', encoding='utf-8') as f:
#     json.dump(info, f, ensure_ascii=False, indent=4)
add_sum_recispdoc_to_csv('example.csv', info, 'fssp_2.csv') # or 'info.json'