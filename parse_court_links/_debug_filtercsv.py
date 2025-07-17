import pandas as pd
from pathlib import Path

thisfolder = Path(__file__).resolve().parent
input = thisfolder / "testdata17.04.25.csv"
df = pd.read_csv(input, dtype=str).fillna('')
df = df[['number', 'debtor_inn', 'creditor_inn']]
print(df.head())

df.to_csv(thisfolder / "filtered_testdata.csv", index=False)