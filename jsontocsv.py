import json
import pandas as pd

with open("dummy_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)["data"]

df = pd.DataFrame(data)
df.to_csv("dummy_data.csv", index=False)