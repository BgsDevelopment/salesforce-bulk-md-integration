
import argparse
import json
from pathlib import Path
from typing import Dict, Any, List, Union, Optional

import pandas as pd

try:
    import yaml  # PyYAML (optional). If not available, JSON must be used.
    _HAS_YAML = True
except Exception:
    _HAS_YAML = False
    yaml = None  # type: ignore

# ===============================================================
# 汎用マスタ変換ユーティリティ
#  - MD（基幹）→ Salesforce Bulk API 2.0 用 CSV へ変換
#  - マッピング/MASTER_KEY/出力名などは外部設定ファイル（YAML or JSON）で管理
#  - 元 ALL ファイルのエンコーディングや区切り文字、ヘッダ有無も設定可能
# ===============================================================

# --- 設定ファイルの型ヒント ------------------------------------
MappingItem = Dict[str, Any]
ConfigType = Dict[str, Any]


# --- デフォルト設定（設定ファイルで上書き可能） -----------------
DEFAULTS: ConfigType = {
    "master_key": "DPT",
    "sf_object": None,
    "operation": None,
    "external_id_field": None,
    "input_encoding": "cp932",     # MDのALLファイルは Shift-JIS(CP932) が多い
    "output_encoding": "utf-8",    # Salesforce 取込向け
    "lineterminator": "\n",        # LF 固定（WindowsでもLF推奨）
    "delimiter": ",",              # ALL がカンマ区切り想定。変更可。
    "has_header": False,           # MDのALLは通常ヘッダ無し
    "owner_id_column": "OwnerId",  # 追加する所有者列名。不要なら null
    "owner_id_value": "",          # 所有者のデフォルト値（空ならSF側自動）
    "extra_fields": {},            # 追加の固定列 { "Field__c": "constant" }
    # マッピング: 入力→出力
    # - index: 0始まりの列番号（または "col_7" など任意文字列でも可。has_header=True の時は列名を指定可）
    # - field: Salesforce 側のAPI項目名
    "mapping": [
        # 例:
        # {"index": 1,  "field": "MdScheduledModDate__c"},
        # {"index": 2,  "field": "MdMaintenanceCreateDate__c"},
    ],
    # 出力ファイルパス（省略時は input_path と master_key から自動決定）
    "output_csv": None,
}


def _load_config(config_path: Union[str, Path]) -> ConfigType:
    """
    設定ファイル（YAML/JSON）を読み込み、DEFAULTSで埋める。
    """
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {p}")

    if p.suffix.lower() in (".yaml", ".yml"):
        if not _HAS_YAML:
            raise RuntimeError("PyYAML が未インストールです。YAMLの代わりにJSONを使用するか、PyYAMLを導入してください。")
        with p.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)  # type: ignore
    elif p.suffix.lower() == ".json":
        with p.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        raise ValueError("設定ファイルの拡張子は .yaml/.yml/.json のいずれかにしてください。")

    merged = DEFAULTS.copy()
    # ネストした dict は浅いマージ（必要十分）
    for k, v in cfg.items():
        merged[k] = v
    return merged


def _build_dataframe(
    input_path: Union[str, Path],
    mapping: List[MappingItem],
    has_header: bool,
    input_encoding: str,
    delimiter: str,
) -> pd.DataFrame:
    """
    入力（ALL）を読み込み、mapping に基づいて変換済み DataFrame を返す。
    - has_header=False の場合は header=None で読み込み、列は 0 始まりの整数インデックス。
    - has_header=True の場合は 1行目をヘッダとして読み込み、列名指定のマッピングも許容。
    """
    read_kwargs: Dict[str, Any] = {
        "encoding": input_encoding,
        "dtype": str,
        "sep": delimiter,
    }
    if has_header:
        read_kwargs["header"] = 0
    else:
        read_kwargs["header"] = None

    df_raw = pd.read_csv(input_path, **read_kwargs)

    # マッピングに基づき、必要列だけ抽出して列名をSalesforce API名にリネーム
    src_cols = []
    dst_cols = []
    for m in mapping:
        idx = m.get("index")
        dst = m.get("field")
        if dst is None:
            raise ValueError(f"mapping 要素に 'field' がありません: {m}")

        if has_header and isinstance(idx, str):
            # ヘッダありで "列名" 指定
            src_cols.append(idx)
        else:
            # 数値インデックス（0始まり）想定。列名でも良いが安全に扱う。
            src_cols.append(idx)

        dst_cols.append(dst)

    # DataFrame から列を取り出す（列存在チェックを丁寧に）
    try:
        df_selected = df_raw[src_cols].copy()
    except Exception as e:
        raise KeyError(f"mapping に指定した列が入力に存在しません。src={src_cols}\n詳細: {e}")

    # 列名を Salesforce API 項目名へ変更
    rename_map = {src: dst for src, dst in zip(src_cols, dst_cols)}
    df_selected.rename(columns=rename_map, inplace=True)

    return df_selected


def convert_md_to_salesforce(
    input_path: Union[str, Path],
    config: ConfigType,
    output_path: Optional[Union[str, Path]] = None,
) -> str:
    """
    MD連携ファイル（ALL形式など）を Salesforce Bulk API 2.0 用の CSV に変換する。
    設定は config（外部設定ファイルの読み込み結果）に基づく。

    Args:
        input_path: 変換元ファイル（.ALL 等）
        config:     設定 dict（_load_config の戻り値）
        output_path:出力CSV（省略時は config["output_csv"] を使い、未指定なら自動命名）

    Returns:
        str: 出力CSVの絶対パス
    """
    input_path = Path(input_path)
    # 出力先を決める（優先順位: 引数 > 設定ファイル > 自動）
    output_csv = (
        Path(output_path).as_posix()
        if output_path
        else (config.get("output_csv") or f"output/{config.get('master_key', 'MASTER')}_upsert_ready.csv")
    )

    df_selected = _build_dataframe(
        input_path=input_path,
        mapping=config["mapping"],
        has_header=config["has_header"],
        input_encoding=config["input_encoding"],
        delimiter=config["delimiter"],
    )

    # OwnerId などの追加列を付与（不要なら設定で無効にできる）
    owner_col = config.get("owner_id_column")
    if owner_col:
        df_selected[owner_col] = config.get("owner_id_value", "")

    # 固定値の追加列（任意）
    extra_fields: Dict[str, Any] = config.get("extra_fields", {})
    for k, v in extra_fields.items():
        df_selected[k] = v

    # 出力ディレクトリを作成
    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # CSV出力
    df_selected.to_csv(
        out_path,
        index=False,
        encoding=config["output_encoding"],
        lineterminator=config["lineterminator"],
    )

    print(f"✅ 変換完了: {out_path.as_posix()}")
    return out_path.resolve().as_posix()


def load_config(config_path: Union[str, Path]) -> ConfigType:
    """
    設定ファイルを読み込み、正規化して返すヘルパー。
    外部モジュールからの利用を想定。
    """
    cfg = _load_config(config_path)
    # 必須: mapping
    if not cfg.get("mapping"):
        raise ValueError("設定ファイルに 'mapping' がありません。少なくとも1つ以上の対応を指定してください。")
    return cfg


# ------------------------- CLI -------------------------
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MD(ALL)→Salesforce Bulk API 用 CSV 変換（汎用版）"
    )
    p.add_argument("input", help="入力ファイルパス（.ALL 等）")
    p.add_argument("--config", required=True, help="設定ファイル（YAML/JSON）へのパス")
    p.add_argument("--output", help="出力CSV。省略時は設定ファイル or 自動命名")
    return p


def main() -> None:
    ap = _build_argparser()
    args = ap.parse_args()

    cfg = load_config(args.config)
    convert_md_to_salesforce(
        input_path=args.input,
        config=cfg,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
