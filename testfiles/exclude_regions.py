import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent

def load_regions(all_regions_path, excluded_regions_path):
    all_regions = pd.read_csv(all_regions_path)
    excluded_regions = pd.read_csv(excluded_regions_path)

    # Normalize to dicts: INN prefix (as string) → Region
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

    return inn_to_region, excluded_inns

def add_regions(df, inn_to_region):
    def get_region(inn_raw):
        inn = str(inn_raw)
        if len(inn) == 9:
            inn = '0' + inn
        if len(inn) != 10 or not inn.isdigit():
            return ''
        # Try longest prefix match
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

def main():
    input_path = BASE_DIR / 'res250714_400_filtered.csv'
    all_regions_path = BASE_DIR / 'all_regions.csv'
    excluded_regions_path = BASE_DIR / 'excluded_regions.csv'
    output_path = BASE_DIR / 'filtered_regions.csv'

    df = pd.read_csv(input_path)
    inn_to_region, excluded_inns = load_regions(all_regions_path, excluded_regions_path)

    df = add_regions(df, inn_to_region)
    # df = filter_regions(df, excluded_inns)
    df = filter_regions_debug(df, excluded_inns)
    df.sort_values(by=['will_be_removed', 'region'], ascending=[False, True], inplace=True)
    cols = ['region', 'will_be_removed', 'reason'] + [col for col in df.columns if col not in ['region', 'will_be_removed', 'reason']]
    df = df[cols]
    df.to_csv("debug_output.csv", index=False)
    df = df[~df['will_be_removed']].drop(columns=['will_be_removed', 'reason'])
    df.to_csv(output_path, index=False)

if __name__ == "__main__":
    main()
