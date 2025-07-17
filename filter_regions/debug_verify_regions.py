from pathlib import Path
import csv

base = Path(__file__).parent
txt_path = base / "all_regions.txt"
csv_path = base / "all_regions.csv"
output_path = base / "matched_regions.csv"

def load_txt_codes(path):
    with open(path, encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    return [(lines[i].zfill(2), lines[i+1]) for i in range(0, len(lines), 2)]

def load_csv_codes(path):
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            raw_codes = row["inns"]
            codes = [c.strip().zfill(2) for c in raw_codes.split(",")]
            rows.append({
                "region": row["region"],
                "codes": codes
            })
        return rows

def match_codes(txt_data, csv_data):
    results = []
    for txt_code, txt_name in txt_data:
        matched = False
        matched_row = None
        for row in csv_data:
            if txt_code in row["codes"]:
                matched = True
                matched_row = row
                break
        results.append({
            "txt_code": txt_code,
            "txt_name": txt_name,
            "matched": matched,
            "csv_region": matched_row["region"] if matched_row else "",
            "csv_codes": ", ".join(matched_row["codes"]) if matched_row else ""
        })
    return results

def write_output(rows, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["TXT Code", "TXT Name", "Match Found", "CSV Region", "CSV Codes"])
        for r in rows:
            writer.writerow([
                r["txt_code"],
                r["txt_name"],
                "✓" if r["matched"] else "✗",
                r["csv_region"],
                r["csv_codes"]
            ])

def main():
    txt_data = load_txt_codes(txt_path)
    csv_data = load_csv_codes(csv_path)
    matched = match_codes(txt_data, csv_data)
    write_output(matched, output_path)

if __name__ == "__main__":
    main()
