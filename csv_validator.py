import pandas as pd
import sys

def load_csv(file_path: str) -> pd.DataFrame:
    """Load a CSV file and handle basic errors."""
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("CSV is empty")
        return df
    except FileNotFoundError:
        print("err: Can't open csv (file not found)")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print("err: Can't open csv (csv is empty)")
        sys.exit(1)
    except pd.errors.ParserError:
        print("err: Can't open csv (invalid format)")
        sys.exit(1)
    except ValueError as e:
        print(f"err: {e}")
        sys.exit(1)

def check_columns(df: pd.DataFrame, required_columns: list) -> None:
    """Check if all required columns exist in the DataFrame."""
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"err: Please upload valid csv format. X column is missing. X = {', '.join(missing_columns)}")
        sys.exit(1)
    else:
        print("Columns existence: All required columns (inn_creditor, inn_debtor, ip) are present")

def validate_row(row: pd.Series, required_columns: list, show_row_content: bool = False) -> None:
    """Validate a single row for empty values and correct data types."""
    # Check for empty columns
    empty_cols = [col for col in required_columns if pd.isna(row[col]) or str(row[col]).strip() == ""]
    if empty_cols:
        if show_row_content:
            print(f"err: Row {row.name + 1} has empty columns: {', '.join(empty_cols)}. Row content: {row.to_dict()}")
        else:
            print(f"err: {', '.join(empty_cols)} is empty for row {row.name + 1}, fix and reupload")
        return

    # Check if inn_creditor and inn_debtor are numbers
    for col in ["inn_creditor", "inn_debtor"]:
        try:
            float(row[col])
        except (ValueError, TypeError):
            if show_row_content:
                print(f"err: Invalid format for row {row.name + 1}, cuz {col} isn't a number. Row content: {row.to_dict()}")
            else:
                print(f"err: Invalid format for row {row.name + 1}, cuz {col} isn't a number")

def validate_csv(file_path: str, show_row_content: bool = False) -> None:
    """Run the full CSV validation pipeline."""
    df = load_csv(file_path)
    required_columns = ["inn_creditor", "inn_debtor", "ip"]
    check_columns(df, required_columns)

    for index, row in df.iterrows():
        validate_row(row, required_columns, show_row_content)

    # Check if any rows had issues
    if not any(pd.isna(row[col]) or str(row[col]).strip() == "" for col in required_columns for index, row in df.iterrows()):
        print("Validation complete. No further issues found in rows.")

if __name__ == "__main__":
    validate_csv('dummy_data.csv', show_row_content=True)