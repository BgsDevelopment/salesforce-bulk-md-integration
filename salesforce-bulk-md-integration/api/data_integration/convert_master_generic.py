# -*- coding: utf-8 -*-
"""
汎用マスタ変換ユーティリティ（ALL -> Salesforce Bulk CSV）

主な特徴
- 設定ファイル（YAML/JSON）で入出力仕様とマッピングを管理
- 文字コードは auto をサポート（候補順にフォールバック）
- 列マッピング（index または 列名）で DataFrame を構成
- 追加列（OwnerId / 任意の固定値）に対応
- CLI と外部モジュール呼び出しの両用

使い方（CLI）
    uv run -m api.data_integration.convert_master_generic `
      input\\TEST_DPT.ALL `
      --config configs\\dpt.yaml `
      --output output\\DPT_upsert_ready.csv

設定ファイル（YAML）例は README / configs/dpt.yaml を参照。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List, Union, Optional

import pandas as pd

try:
    import yaml  # PyYAML（YAMLを使う場合に必要）

    _HAS_YAML = True
except Exception:
    _HAS_YAML = False
    yaml = None  # type: ignore

# ---------------------------------------------------------------
# 型エイリアス
# ---------------------------------------------------------------
MappingItem = Dict[str, Any]
ConfigType = Dict[str, Any]

# ---------------------------------------------------------------
# 既定値（設定ファイルで上書き可能）
#   ※ utf-8 既定だと Windows 由来の ALL で毎回落ちやすいので cp932 を既定に。
#   ※ auto 判定を使う場合は input_encoding: "auto" を設定ファイルに書く。
# ---------------------------------------------------------------
DEFAULTS: ConfigType = {
    "master_key": "MASTER_GENERIC",

    # Bulk 側情報（ここは変換には直接使わないが、ログ/命名で便利）
    "sf_object": None,
    "operation": None,
    "external_id_field": None,

    # 入力（ALL）関連
    "input_encoding": "cp932",  # "auto" / "cp932" / "shift_jis" / "utf-8-sig" / "utf-8"
    "input_encoding_candidates": ["cp932", "shift_jis", "utf-8-sig", "utf-8"],
    "delimiter": ",",  # CSV: ",", TSV: "\t"
    "has_header": False,  # 先頭行にヘッダがあるなら True

    # 出力（CSV）関連
    "output_encoding": "utf-8",
    "lineterminator": "\n",
    "output_csv": None,  # 未指定なら "output/{master_key}_upsert_ready.csv"

    # 追加列（任意）
    "owner_id_column": "OwnerId",  # 追加列名。不要なら null or "" でも可
    "owner_id_value": "",  # 追加列の値
    "extra_fields": {},  # 例: {"IsActive__c": "true"}

    # マッピング（必須）
    #   has_header=True の時は index に列名も可。
    #   has_header=False の時は 0 始まりの列番号を使う。
    "mapping": [
        # 例:
        # {"index": 7, "field": "DptCode__c"},
        # {"index": 9, "field": "Name"},
    ],
}


# ---------------------------------------------------------------
# 設定ファイルの読み込み
# ---------------------------------------------------------------
def _load_config(config_path: Union[str, Path]) -> ConfigType:
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"設定ファイルが見つかりません: {p}")

    if p.suffix.lower() in (".yaml", ".yml"):
        if not _HAS_YAML:
            raise RuntimeError("PyYAML が未インストールです。YAML の代わりに JSON を使うか、PyYAML を導入してください。")
        with p.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)  # type: ignore
    elif p.suffix.lower() == ".json":
        with p.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        raise ValueError("設定ファイルの拡張子は .yaml/.yml/.json のいずれかにしてください。")

    merged = DEFAULTS.copy()
    for k, v in (cfg or {}).items():
        # 浅いマージで十分（深いネストは現状 extra_fields 程度）
        merged[k] = v
    return merged


def load_config(config_path: Union[str, Path]) -> ConfigType:
    """
    設定ファイルを読み込み、最低限の検証をしたうえで返すヘルパー。
    """
    cfg = _load_config(config_path)
    if not cfg.get("mapping"):
        raise ValueError("設定ファイルに 'mapping' がありません。少なくとも1つ以上の対応を指定してください。")
    # input_encoding の値チェック（pandas にそのまま渡るため）
    enc = cfg.get("input_encoding")
    if enc == "auto":
        cands = cfg.get("input_encoding_candidates") or []
        if not isinstance(cands, list) or not cands:
            raise ValueError(
                "input_encoding が 'auto' の場合、input_encoding_candidates に候補リストを指定してください。")
    return cfg


# ---------------------------------------------------------------
# 入力読み込み（エンコーディング自動判定つき）
#   - has_header=True の場合: header=0 で読み、列名指定マッピングが可
#   - has_header=False の場合: header=None で 0..N-1 の整数列とする
#   - dtype=str を強制して前ゼロ欠落を防止
# ---------------------------------------------------------------
def _read_csv_with_auto_encoding(
        input_path: Union[str, Path],
        config: ConfigType,
) -> pd.DataFrame:
    read_kwargs: Dict[str, Any] = {
        "sep": config.get("delimiter", ","),
        "dtype": str,  # 重要：前ゼロを守る
        "header": 0 if config.get("has_header") else None,
    }

    enc = config.get("input_encoding", "cp932")
    if enc != "auto":
        return pd.read_csv(input_path, encoding=enc, **read_kwargs)

    last_err: Optional[Exception] = None
    for cand in config.get("input_encoding_candidates", ["cp932", "shift_jis", "utf-8-sig", "utf-8"]):
        try:
            df = pd.read_csv(input_path, encoding=cand, **read_kwargs)
            print(f"[info] input encoding detected: {cand}")
            return df
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # ここまで全滅
    if last_err:
        raise last_err
    raise LookupError("input_encoding 'auto' だが候補が空です。input_encoding_candidates を指定してください。")


# ---------------------------------------------------------------
# マッピング適用（index または 列名）
# ---------------------------------------------------------------
def _apply_mapping(
        df_raw: pd.DataFrame,
        mapping: List[MappingItem],
        has_header: bool,
) -> pd.DataFrame:
    src_cols: List[Any] = []
    dst_cols: List[str] = []

    for m in mapping:
        dst = m.get("field")
        if not dst:
            raise ValueError(f"mapping 要素に 'field' がありません: {m}")

        idx = m.get("index")
        if has_header and isinstance(idx, str):
            # 列名指定（ヘッダあり）
            if idx not in df_raw.columns:
                raise KeyError(
                    f"mapping で指定した列名が入力に存在しません: '{idx}' | columns={list(df_raw.columns)[:10]} ...")
            src_cols.append(idx)
        else:
            # 数値インデックス（0始まり）
            if not isinstance(idx, int):
                raise TypeError(f"mapping.index は int か、ヘッダありのときは str（列名）にしてください。値={idx!r}")
            max_col = df_raw.shape[1] - 1
            if idx < 0 or idx > max_col:
                raise IndexError(
                    f"mapping.index={idx} が範囲外です（0..{max_col}）。入力の列数と mapping を見直してください。")
            src_cols.append(idx)

        dst_cols.append(dst)

    try:
        df_selected = df_raw[src_cols].copy()
    except Exception as e:
        raise KeyError(f"mapping に指定した列が入力に存在しません。src={src_cols}\n詳細: {e}")

    # 列名 → Salesforce API名へ
    rename_map = {src: dst for src, dst in zip(src_cols, dst_cols)}
    df_selected.rename(columns=rename_map, inplace=True)
    return df_selected


# ---------------------------------------------------------------
# 変換本体
# ---------------------------------------------------------------
def convert_md_to_salesforce(
        input_path: Union[str, Path],
        config: ConfigType,
        output_path: Optional[Union[str, Path]] = None,
) -> str:
    """
    MD（ALL 等）を Salesforce Bulk API 2.0 用 CSV に変換する。

    Args:
        input_path:   入力ファイル（.ALL 等）
        config:       設定 dict（load_config の戻り値）
        output_path:  出力CSV（省略時は config["output_csv"] -> 自動命名の順）

    Returns:
        出力CSVの絶対パス（str）
    """
    input_path = Path(input_path)

    # 出力ファイル名の決定（引数 > 設定 > 自動）
    output_csv = (
        Path(output_path).as_posix()
        if output_path
        else (config.get("output_csv") or f"output/{config.get('master_key', 'MASTER')}_upsert_ready.csv")
    )
    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) 入力読込（エンコーディング自動判定対応）
    df_raw = _read_csv_with_auto_encoding(input_path, config)

    # 2) マッピング適用（必要な列だけ抽出＆列名を SF API 名へ）
    df_selected = _apply_mapping(
        df_raw=df_raw,
        mapping=config["mapping"],
        has_header=config["has_header"],
    )

    # 3) 追加列（OwnerId / extra_fields）
    owner_col = config.get("owner_id_column")
    if owner_col:
        df_selected[owner_col] = config.get("owner_id_value", "")

    extra_fields: Dict[str, Any] = config.get("extra_fields", {}) or {}
    for k, v in extra_fields.items():
        df_selected[k] = v

    # 4) CSV 出力（改行コード・出力エンコードを尊重）
    df_selected.to_csv(
        out_path,
        index=False,
        encoding=config.get("output_encoding", "utf-8"),
        lineterminator=config.get("lineterminator", "\n"),
    )

    print(f"✅ 変換完了: {out_path.as_posix()}")
    return out_path.resolve().as_posix()


# ---------------------------------------------------------------
# CLI
# ---------------------------------------------------------------
def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MD(ALL) → Salesforce Bulk API 用 CSV 変換（汎用・エンコーディング自動判定対応）"
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
