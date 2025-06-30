import pandas as pd
import sys

def validate_csv(file_path: str) -> None:
    # Check if file can be opened and is not empty
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            print("err: Can't open csv (csv is empty)")
            return
    except FileNotFoundError:
        print("err: Can't open csv (file not found)")
        return
    except pd.errors.EmptyDataError:
        print("err: Can't open csv (csv is empty)")
        return
    except pd.errors.ParserError:
        print("err: Can't open csv (invalid format)")
        return

    # Required columns
    required_columns = ["inn_creditor", "inn_debtor", "ip"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"err: Please upload valid csv format. X column is missing. X = {', '.join(missing_columns)}")
        return
    else:
        print("Columns existence: All required columns (creditor_inn, debtor_inn, ip) are present")

    # Check each row
    for index, row in df.iterrows():
        # Check for empty columns
        empty_cols = [col for col in required_columns if pd.isna(row[col]) or str(row[col]).strip() == ""]
        if empty_cols:
            print(f"err: {col} is empty for row {index + 1}, fix and reupload")
            continue

        # Type check: inn_creditor and inn_debtor should be numbers
        for col in ["inn_creditor", "inn_debtor"]:
            try:
                float(row[col])  # Try to convert to number
            except (ValueError, TypeError):
                print(f"err: Invalid format for row {index + 1}, cuz {col} isn't a number")

    print("Validation complete. No further issues found in rows.") if all(not empty_cols for empty_cols in [col for col in required_columns if pd.isna(row[col]) or str(row[col]).strip() == ""] for index, row in df.iterrows()) else None

if __name__ == "__main__":
    validate_csv('dummy_data.csv')
    # if len(sys.argv) != 2:
    #     print("Usage: python csv_validator.py <csv_file_path>")
    # else:
    #     # validate_csv(sys.argv[1])