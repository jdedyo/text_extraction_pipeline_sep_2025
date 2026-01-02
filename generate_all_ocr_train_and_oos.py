from SETTINGS import *
import pandas as pd

def get_acceptable_ack_ids(data_path: Path=LLM_TRAIN_DATA_PATH) -> set:
    train_data = pd.read_stata(data_path)
    return set(train_data["ack_id"].dropna().astype(str).str.strip())


if __name__ == "__main__":
    good_ack_ids = get_acceptable_ack_ids()

    train_dfs = []
    oos_dfs   = []
    for path in ALL_OCR.glob("*[0-9][0-9][0-9][0-9].csv"):
        print(f"Loading {path.name}...")
        df = pd.read_csv(path, usecols=["ack_id", "year", OCR_TEXT_COL])
        df["ack_id"] = df["ack_id"].str.strip().astype(str)

        df_train = df[df["ack_id"].isin(good_ack_ids)]
        train_dfs.append(df_train)

        df_oos = df #df[~df["ack_id"].isin(good_ack_ids)]
        oos_dfs.append(df_oos)

    if oos_dfs:
        oos_combined = pd.concat(oos_dfs, ignore_index=True)
        
        # Shuffle to make sure each chunk is representative
        oos_combined = oos_combined.sample(frac=1, random_state=42).reset_index(drop=True)

        oos_output_path = ALL_OCR / "all_ocr_oos.csv"
        oos_combined.to_csv(oos_output_path, index=False)
        print(f"Saved combined OOS OCR data to {oos_output_path} ({len(oos_combined):,} rows)")
    else:
        print("No matching files or no valid ack_ids found.")

    if train_dfs:
        train_combined = pd.concat(train_dfs, ignore_index=True)
        train_output_path = ALL_OCR / "all_ocr_train.csv"
        train_combined.to_csv(train_output_path, index=False)
        print(f"Saved combined train OCR data to {train_output_path} ({len(train_combined):,} rows)")
    else:
        print("No matching files or no valid ack_ids found.")