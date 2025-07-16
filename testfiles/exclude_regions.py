import pandas as pd
from pathlib import Path

DEBUG = True

def load_regions(all_regions_path, excluded_regions_path):
    all_regions = pd.read_csv(all_regions_path)
    excluded_regions = pd.read_csv(excluded_regions_path)

    inn_to_region = {}
    for _, row in all_regions.iterrows():
        for inn in str(row['inns']).split(','):
            inn = inn.strip().zfill(2)
            if inn: inn_to_region[inn] = row['region']

    excluded_inns = set()
    for _, row in excluded_regions.iterrows():
        for inn in str(row['inns']).split(','):
            inn = inn.strip().zfill(2)
            if inn: excluded_inns.add(inn)

    return inn_to_region, excluded_inns, excluded_regions

def add_regions(df, inn_to_region):
    def get_region(inn_raw):
        inn = str(inn_raw)
        if len(inn) == 9:
            inn = '0' + inn
        if len(inn) != 10 or not inn.isdigit():
            return ''
        for length in range(3, 1, -1):
            prefix = inn[:length]
            if prefix in inn_to_region:
                return inn_to_region[prefix]
        return ''
    
    df['region'] = df['debtor_inn'].apply(get_region)
    return df

def filter_regions(df, excluded_inns):
    def is_excluded(inn_raw):
        inn = str(inn_raw).zfill(10)
        for length in range(3, 1, -1):
            if inn[:length] in excluded_inns:
                return True
        return False

    return df[~df['debtor_inn'].apply(is_excluded)].copy()

def filter_regions_debug(df, excluded_inns):
    def check_exclusion(inn_raw):
        inn = str(inn_raw).zfill(10)
        if not inn.isdigit() or len(inn) != 10:
            return True, "invalid INN"
        for length in range(3, 1, -1):
            prefix = inn[:length]
            if prefix in excluded_inns:
                return True, f"excluded region: {prefix}"
        return False, ""
    
    df['will_be_removed'], df['reason'] = zip(*df['debtor_inn'].apply(check_exclusion))
    return df

def report_excluded_regions(df, excluded_inns, excluded_regions_df, output_path="excluded_report.csv"):
    report_rows = []
    
    # Create a mapping from prefix to region name for efficient lookup
    prefix_to_region_name = {}
    for _, row in excluded_regions_df.iterrows():
        region_name = row['region']
        inns_list = [inn.strip() for inn in str(row['inns']).split(',')]
        for inn in inns_list:
            # Use zfill to match the format in excluded_inns
            prefix_to_region_name[inn.zfill(2)] = region_name

    for prefix in sorted(list(excluded_inns)):
        matches = df[df['reason'] == f"excluded region: {prefix}"]
        count = len(matches)
        region_name = prefix_to_region_name.get(prefix, 'UNKNOWN')
        
        if count > 0:
            sample = matches.iloc[0]
            report_rows.append({
                'excluded_region_code': prefix,
                'excluded_region_name': region_name,
                'matches': count,
                'sample_debtor_inn': sample['debtor_inn'],
                'sample_region': sample.get('region', '')
            })
        else:
            # Also report regions that had no matches in the data
            report_rows.append({
                'excluded_region_code': prefix,
                'excluded_region_name': region_name,
                'matches': 0,
                'sample_debtor_inn': '',
                'sample_region': ''
            })

    report_df = pd.DataFrame(report_rows)
    report_df.to_csv(output_path, index=False)
    return report_df


def report_included_regions(df, output_path="included_report.csv"):
    """Generates a report on the included (kept) regions."""
    included_df = df[df['region'] != ''].copy()

    if included_df.empty:
        pd.DataFrame(columns=['region_name', 'matches', 'sample_debtor_inn']).to_csv(output_path, index=False)
        return

    report = included_df.groupby('region').agg(
        matches=('debtor_inn', 'size'),
        sample_debtor_inn=('debtor_inn', 'first')
    ).reset_index()

    report.rename(columns={'region': 'region_name'}, inplace=True)
    report.sort_values(by='region_name', inplace=True)
    report.to_csv(output_path, index=False)
    return report

def main():
    base = Path(__file__).parent
    input_path = base / 'res250714_400_filtered.csv'
    all_regions_path = base / 'all_regions.csv'
    excluded_regions_path = base / 'excluded_regions.csv'
    output_path = base / 'filtered_regions.csv'

    df = pd.read_csv(input_path)
    inn_to_region, excluded_inns, excluded_regions_df = load_regions(all_regions_path, excluded_regions_path)
    df = add_regions(df, inn_to_region)

    if DEBUG:
        df = filter_regions_debug(df, excluded_inns)
        df.sort_values(by=['will_be_removed', 'region'], ascending=[False, True], inplace=True)
        # Move debug cols left
        cols = ['region', 'will_be_removed', 'reason'] + [c for c in df.columns if c not in ['region', 'will_be_removed', 'reason']]
        df_debug = df[cols]
        df_debug.to_csv(base / 'debug_output.csv', index=False)
        report_excluded_regions(df, excluded_inns, excluded_regions_df, output_path=base / 'excluded_report.csv')
        # Clean output
        df = df[~df['will_be_removed']].drop(columns=['will_be_removed', 'reason'])
    else:
        # Normal (non-debug) flow
        df = filter_regions(df, excluded_inns)

    report_included_regions(df, output_path=base / 'included_report.csv')
    df.to_csv(output_path, index=False)

if __name__ == '__main__':
    main()
