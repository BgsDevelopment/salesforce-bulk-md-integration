# scripts/export_soql.py
# Bulk API 2.0 (CSV) 単一オブジェクト用エクスポータ（完成版）
# - YAML (--config) で対象と項目を定義
# - soql が空なら mappings から自動SOQL生成
# - describe で存在確認 → 未存在は WARN で自動除外
# - 別オブジェクトの項目が混入しても WARN で自動無視
# - 項目重複は順序保持で自動除去
# - Bulk V2（CSV）前提：リレーション/サブクエリは使用しない

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))  # プロジェクトルートを import パスへ

import os
import argparse
import datetime as dt
import pathlib
import requests
import yaml

from api.data_integration.bulk_export import export_soql_to_csv
from api.auth.token_client_credentials import get_access_token

API_VERSION = os.getenv("SF_API_VERSION", "v62.0")


# ===== ユーティリティ =====

def normalize_token(token):
    """get_access_token() の返り値を (access_token, instance_url) に正規化。"""
    if isinstance(token, dict):
        at = token.get("access_token") or token.get("token") or token.get("accessToken")
        iu = token.get("instance_url") or token.get("instanceUrl") or os.getenv("SF_INSTANCE_URL")
        if not at or not iu:
            raise RuntimeError("get_access_token() の返り値(dict)に access_token / instance_url が不足しています。")
        return at, iu
    if isinstance(token, (tuple, list)):
        if len(token) != 2:
            raise RuntimeError("get_access_token() の返り値(tuple/list)は (access_token, instance_url) を期待します。")
        at, iu = token
        iu = iu or os.getenv("SF_INSTANCE_URL")
        if not at or not iu:
            raise RuntimeError("get_access_token() の返り値から access_token / instance_url を確定できません。")
        return at, iu
    if isinstance(token, str):
        at = token
        iu = os.getenv("SF_INSTANCE_URL")
        if not iu:
            raise RuntimeError("instance_url が不明です。環境変数 SF_INSTANCE_URL を設定してください。")
        return at, iu
    raise RuntimeError(f"get_access_token() の返り値型に未対応: {type(token)}")


def describe_fields(instance_url: str, access_token: str, object_api: str) -> set:
    """SObject のフィールド API 名集合（Id 含む）を返す。"""
    url = f"{instance_url}/services/data/{API_VERSION}/sobjects/{object_api}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    r.raise_for_status()
    data = r.json()
    names = {f["name"] for f in data.get("fields", [])}
    names.add("Id")
    return names


def filter_existing(object_api: str, requested_fields: list, instance_url: str, access_token: str):
    """存在するフィールドだけ残し、除外したフィールドも返す。"""
    existing = describe_fields(instance_url, access_token, object_api)
    ok = [f for f in requested_fields if f in existing]
    ng = [f for f in requested_fields if f not in existing]
    return ok, ng


def dedupe_keep_order(seq):
    """順序を保った重複除去。None/空はスキップ。"""
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def build_soql(object_api: str, fields: list, where: str = "", order_by: str = "", limit: str = "") -> str:
    cols = ["Id"] + (fields or [])
    soql = f"SELECT {', '.join(cols)} FROM {object_api}"
    if where:
        soql += f" WHERE {where}"
    if order_by:
        soql += f" ORDER BY {order_by}"
    if limit:
        soql += f" LIMIT {limit}"
    return soql


# ===== メイン =====

def main():
    parser = argparse.ArgumentParser(description="Run Bulk API 2.0 SOQL export (single object, CSV).")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # 対象オブジェクト決定（object_api 推奨 / 互換: object_info[0].api）
    object_api = cfg.get("object_api")
    if not object_api:
        oi = cfg.get("object_info") or []
        if oi and isinstance(oi, list) and "api" in oi[0]:
            object_api = oi[0]["api"]
        else:
            raise RuntimeError("config に object_api がありません。")

    # トークン
    access_token, instance_url = normalize_token(get_access_token())

    # mappings から取得対象フィールド抽出
    mappings = cfg.get("mappings") or []

    # 単一オブジェクト運用：object が指定されている場合は object_api と一致するものだけ採用
    wrong_object_fields = [m.get("api") for m in mappings
                           if m.get("object") and m.get("object") != object_api]

    requested = [m.get("api") for m in mappings
                 if m.get("api") and (m.get("object") in (None, object_api))]

    # 順序保持で重複除去
    requested = dedupe_keep_order(requested)

    if wrong_object_fields:
        bad = ", ".join(sorted(set([x for x in wrong_object_fields if x])))
        if bad:
            print(f"[WARN] config に別オブジェクトの項目が混入しています（無視します）: {bad}")

    # 存在確認
    fields_ok, fields_ng = filter_existing(object_api, requested, instance_url, access_token)
    if fields_ng:
        print(f"[WARN] {object_api}: 未存在フィールドを除外: {', '.join(fields_ng)}")

    # SOQL 決定
    soql_cfg = (cfg.get("soql") or "").strip()
    if soql_cfg:
        soql = soql_cfg
    else:
        qopt = cfg.get("query_options") or {}
        soql = build_soql(
            object_api=object_api,
            fields=fields_ok,
            where=qopt.get("where") or "",
            order_by=qopt.get("order_by") or "",
            limit=qopt.get("limit") or "",
        )

    # 出力パス
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = pathlib.Path(cfg.get("out", f"output/{object_api}_{ts}.csv"))
    out.parent.mkdir(parents=True, exist_ok=True)

    # 実行
    result = export_soql_to_csv(
        soql=soql,
        output_csv_path=out,
        operation=cfg.get("operation", "query"),
        max_records_per_page=cfg.get("page", 100000),
        pk_chunking=cfg.get("pk_chunking") or None,
        include_header_once=True,
    )

    # レポート
    print("✅ Export done (CSV / Bulk V2)")
    print(f"  Obj  : {object_api}")
    print(f"  JobID: {result['job_id']}")
    print(f"  Pages: {result['pages']}  Rows: ~{result['rows']}")
    print(f"  File : {result['path']}")
    print(f"  SOQL : {soql}")


if __name__ == "__main__":
    main()
