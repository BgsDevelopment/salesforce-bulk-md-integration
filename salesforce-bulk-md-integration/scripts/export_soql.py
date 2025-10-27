# scripts/export_soql.py
import sys, os
from pathlib import Path

# 重要: プロジェクトルートを import パスへ追加（scripts の1つ上）
sys.path.append(str(Path(__file__).resolve().parents[1]))

import argparse
import datetime as dt
import pathlib
from api.data_integration.bulk_export import export_soql_to_csv


def main():
    parser = argparse.ArgumentParser(
        description="Run Bulk API 2.0 SOQL export and save to CSV."
    )
    parser.add_argument("--soql", required=True, help="SOQL (quote as a single string)")
    parser.add_argument("--out", default="", help="Output CSV path. (default: output/<timestamp>_export.csv)")
    parser.add_argument("--operation", default="query", choices=["query", "queryAll"],
                        help="Use queryAll to include archived/soft-deleted.")
    parser.add_argument("--page", type=int, default=100000, help="maxRecords per page (default 100000)")
    parser.add_argument("--pk-chunking", default="", help='ex) "chunkSize=100000" (optional)')
    args = parser.parse_args()

    out = pathlib.Path(args.out) if args.out else pathlib.Path(
        "output") / f"{dt.datetime.now():%Y%m%d_%H%M%S}_export.csv"
    pkc = args.pk_chunking or None

    result = export_soql_to_csv(
        soql=args.soql,
        output_csv_path=out,
        operation=args.operation,
        max_records_per_page=args.page,
        pk_chunking=pkc,
        include_header_once=True,
    )
    print("✅ Export done")
    print(f"  JobID : {result['job_id']}")
    print(f"  Pages : {result['pages']}")
    print(f"  Rows  : ~{result['rows']}")
    print(f"  File  : {result['path']}")


if __name__ == "__main__":
    main()
