import pandas as pd

def process_csv(input_csv_path, output_csv_path, filter_sum=True, include_groupsum=True):
    """
    Process a CSV file to group rows by inn_debtor and inn_creditor, calculate groupsum,
    and produce a new CSV with specified columns, sorting, and optional filtering.

    Parameters:
    - input_csv_path (str): Path to the input CSV file.
    - output_csv_path (str): Path to save the output CSV file.
    - filter_sum (bool): If True, remove rows where sum < 2.5 million in the final output.
    - include_groupsum (bool): If True, calculate and include the groupsum column in the output.
    """
    # Step 1: Read the CSV into a pandas DataFrame
    df = pd.read_csv(input_csv_path)

    # Step 2: Calculate groupsum (if required)
    if include_groupsum:
        # Group by inn_debtor and inn_creditor, sum the 'sum' column
        groupsum_df = df.groupby(['inn_debtor', 'inn_creditor'])['sum'].sum().reset_index()
        groupsum_df.rename(columns={'sum': 'groupsum'}, inplace=True)

        # Merge groupsum back into the original DataFrame
        df = df.merge(groupsum_df, on=['inn_debtor', 'inn_creditor'], how='left')
    else:
        # If groupsum is not included, add a dummy column for sorting (will be dropped later)
        df['groupsum'] = 0

    # Step 3: Filter rows where sum < 2.5 million (if filter_sum is True)
    if filter_sum:
        df = df[df['sum'] >= 2500000]

    # Step 4: Sort the DataFrame
    # Sort by groupsum (descending) and then by sum (descending)
    df = df.sort_values(by=['groupsum', 'sum'], ascending=[False, False])

    # Step 5: Select the required columns
    columns = ['ip', 'inn_creditor', 'inn_debtor', 'sum', 'recispdoc']
    if include_groupsum:
        columns.append('groupsum')
    df = df[columns]

    # Step 6: Save the processed DataFrame to a new CSV file
    df.to_csv(output_csv_path, index=False)

# Example usage
if __name__ == "__main__":
    # Replace 'input.csv' and 'output.csv' with your actual file paths
    process_csv('fssp_2.csv', '3fssp.csv', filter_sum=True, include_groupsum=True)
