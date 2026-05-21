import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--id", required=True, help="Path to id_card manifest CSV")
    p.add_argument("--sb", required=True, help="Path to savings_book manifest CSV")
    p.add_argument("--out", required=True, help="Output combined manifest CSV")
    args = p.parse_args()

    id_path = Path(args.id)
    sb_path = Path(args.sb)
    out_path = Path(args.out)

    id_df = pd.read_csv(id_path)
    sb_df = pd.read_csv(sb_path)
    combined = pd.concat([id_df, sb_df], ignore_index=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(out_path, index=False)

    print(f"rows_id={len(id_df)}")
    print(f"rows_sb={len(sb_df)}")
    print(f"rows_combined={len(combined)}")
    print(f"out={out_path.as_posix()}")


if __name__ == "__main__":
    main()
