import pandas as pd
from pathlib import Path

this_folder = Path(__file__).parent
input_file = this_folder / "rdsng_1_decisions.csv"
df = pd.read_csv(input_file, dtype=str).fillna('')
output_file = this_folder / "rdsng_1_decisions_1.csv"
df = df.drop(columns=['links_texts', 'group_summa'])
df.to_csv(output_file, index=False)