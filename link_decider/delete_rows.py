import pandas as pd

input_path = "link_decider\\data_.csv"      # Input CSV file path
output_path = "link_decider\\deleted_columns.csv"    # Output CSV file path

# Read the CSV file
df = pd.read_csv(input_path)

# Keep only 'summa' and 'links' columns
df = df[['summa', 'links']]

# Save the result to a new CSV file
df.to_csv(output_path, index=False)