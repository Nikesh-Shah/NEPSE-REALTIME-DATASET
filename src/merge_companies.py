import pandas as pd
import os

def merge_company_csvs(input_folder="d:/projects/nepse-data/data/company-wise/", output_file="d:/projects/nepse-data/data/all_companies.csv"):
    all_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]
    dfs = []
    for f in all_files:
        file_path = os.path.join(input_folder, f)
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                print(f"[SKIP] Empty file: {f}")
                continue
            print(f"[OK] Processed file: {f} with {len(df)} rows")
            df['ticker'] = f.replace(".csv", "")
            dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"[SKIP] Empty or invalid file: {f}")
            continue
    if not dfs:
        print("No valid CSV files found to merge. No output file created.")
        return
    big_df = pd.concat(dfs, ignore_index=True)
    big_df.to_csv(output_file, index=False)
    print(f"Merged dataset saved to {output_file}")

if __name__ == "__main__":
    merge_company_csvs()
