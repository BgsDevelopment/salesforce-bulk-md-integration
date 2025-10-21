# -*- coding: utf-8 -*-
"""
Salesforce Bulk API（Ingest）で MD → Salesforce 連携を行う単発実行スクリプト。

設計の肝：
- 入力が CSV なら変換を一切呼ばず、そのまま Bulk Ingest（最短経路。落ちにくい）
- 入力が ALL なら「変換器の流派差」をアダプトしてから実行
  - 旧流派: convert_md_to_salesforce(input_all, output_csv)
  - 新流派: convert_md_to_salesforce(input_all, config_dict, output_csv)
    （例: api.data_integration.convert_master_generic の最新版）

処理フロー（正常系）：
  [1] 変換器レジストリ自動構築（convert_*.py を探索）
  [2] 入力が CSV なら変換スキップ / ALL なら変換実行
  [3] OAuth2(Client Credentials) でトークン取得
  [4] ジョブ作成 → CSV アップロード → クローズ
  [5] ポーリング（最大10分）→ 成功/失敗CSV保存

障害解析を早くする工夫：
- 変換スキップ分岐の明示
- 変換関数の“流派差”を吸収（第一回目で落ちない）
- 成功/失敗CSVは「生」（UTF-8/LF）と「Excel用」（UTF-8(BOM)/CRLF）の2系統で保存
"""

import time
import pathlib
import importlib
import pkgutil
import inspect
from typing import Callable, Optional, Dict, Any, Tuple

import requests

from api.auth.token_client_credentials import get_access_token
from api.config.settings import (
    SFOBJ as DEFAULT_SFOBJ,
    OPERATION as DEFAULT_OPERATION,
    EXTERNAL_ID_FIELD as DEFAULT_EXTERNAL_ID_FIELD,
    API_VER,
)

# converter の厳密なシグネチャは流派があるため、Callable[..., str] にして柔軟対応
ConverterFn = Callable[..., str]

JSON = {"Content-Type": "application/json"}
CSV = {"Content-Type": "text/csv"}


# ---------------------------------------------------------------------------
# 変換器の探索ロジック
# ---------------------------------------------------------------------------

def _current_pkg_name() -> str:
    """このファイルの“親パッケージ名”を推定して返す（直実行の保険付き）。"""
    if __package__:
        parts = __package__.split(".")
        if parts[-1] == "bulk_upsert":
            return ".".join(parts[:-1])
        return __package__
    here = pathlib.Path(__file__).resolve()
    parts = list(here.parts)
    if "api" in parts:
        i = parts.index("api")
        if i + 1 < len(parts):
            return f"api.{parts[i + 1]}"
    return "api.md_integration"


def _choose_converter_pkgs() -> list[str]:
    """convert_*.py を探索する候補パッケージ（優先順）。"""
    me = _current_pkg_name()
    # data_integration を優先候補に含める（現行の convert_master_generic はここに居る想定）
    candidates = [me, "api.data_integration", "api.integration", "api.md_integration"]
    uniq: list[str] = []
    for c in candidates:
        if c not in uniq:
            uniq.append(c)
    return uniq


def load_converters() -> Dict[str, Dict[str, Any]]:
    """
    convert_*.py を自動 import してレジストリ辞書を返す。

    想定仕様（モジュール側。未定義はデフォルトを適用）:
      - MASTER_KEY: 実行キー（例: 'DPT'）
      - OUTPUT_CSV: 既定出力パス
      - SF_OBJECT / OPERATION / EXTERNAL_ID_FIELD: メタ
      - convert_md_to_salesforce: 変換関数（必須）
        * 旧: (input_all, output_csv) -> output_csv
        * 新: (input_all, config_dict, output_csv) -> output_csv
      - load_config(path) -> dict があれば利用（新流派向け）

    戻り値:
      { MASTER_KEY: { converter, output_csv, sf_object, operation, external_id_field, module } }
    """
    registry: Dict[str, Dict[str, Any]] = {}
    tried: list[str] = []

    for pkg_name in _choose_converter_pkgs():
        tried.append(pkg_name)
        try:
            pkg = importlib.import_module(pkg_name)
        except ModuleNotFoundError:
            continue

        found_any = False
        for _, fullname, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
            mod_short = fullname.rsplit(".", 1)[-1]
            if not mod_short.startswith("convert_"):
                continue

            m = importlib.import_module(fullname)
            converter = getattr(m, "convert_md_to_salesforce", None) or getattr(m, "CONVERTER", None)
            if not callable(converter):
                continue

            registry[getattr(m, "MASTER_KEY", mod_short.replace("convert_", "").upper())] = {
                "converter": converter,
                "output_csv": getattr(m, "OUTPUT_CSV", None),
                "sf_object": getattr(m, "SF_OBJECT", None),
                "operation": getattr(m, "OPERATION", None),
                "external_id_field": getattr(m, "EXTERNAL_ID_FIELD", None),
                "module": m,  # 後で load_config を呼ぶため保持
            }
            found_any = True

        if found_any:
            return registry

    raise RuntimeError("convert_*.py が見つかりません。探索候補: " + ", ".join(tried))


# ---------------------------------------------------------------------------
# Salesforce RESTユーティリティ（失敗時は中身を全部見せる）
# ---------------------------------------------------------------------------

def _check_json(resp: requests.Response, where: str):
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"{where}: 非JSON応答 status={resp.status_code} body={resp.text[:1000]}")
    if not resp.ok:
        raise RuntimeError(f"{where}: エラー status={resp.status_code} body={data}")
    return data


def _get_job(instance_url: str, access_token: str, job_id: str):
    url = f"{instance_url}/services/data/v{API_VER}/jobs/ingest/{job_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return _check_json(r, "ジョブ取得")


def _download_result(instance_url: str, access_token: str, job_id: str, kind: str) -> bytes:
    """
    成功/失敗結果の CSV を bytes で取得する（※ r.text は使わない）。
    文字化けの原因になる requests 側の誤エンコード推定を避けるため。
    """
    url = f"{instance_url}/services/data/v{API_VER}/jobs/ingest/{job_id}/{kind}Results"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return r.content if r.ok else b""


def _resolve_job_settings(master_key: str, registry: Dict[str, Dict[str, Any]]) -> Tuple[str, str, Optional[str]]:
    reg = registry.get(master_key)
    if not reg:
        available = ", ".join(sorted(registry.keys()))
        raise KeyError(f"未対応マスタ: {master_key}. 利用可能: {available}")

    sf_object = reg.get("sf_object") or DEFAULT_SFOBJ
    operation = (reg.get("operation") or DEFAULT_OPERATION or "").lower()
    external_id_field = reg.get("external_id_field") or DEFAULT_EXTERNAL_ID_FIELD

    if operation == "upsert" and not external_id_field:
        raise ValueError(f"{master_key}: upsert には external_id_field が必須です")
    return sf_object, operation, external_id_field


# ---------------------------------------------------------------------------
# 変換アダプタ（流派差の吸収）
# ---------------------------------------------------------------------------

def _run_converter_adaptively(
        master_key: str,
        input_all_path: str,
        reg: Dict[str, Any],
) -> str:
    """
    変換関数の“流派差”を吸収して実行し、出力CSVパスを返す。

    優先順：
      (A) 旧流派: converter(input_all, output_csv)
      (B) 新流派: converter(input_all, config_dict, output_csv)
          - config の特定は以下の順で試す：
            1) モジュールに load_config があれば configs/{master_key.lower()}.yaml を読む
            2) PyYAML があれば同パスを直接読む
          - 見つからなければ、実行を止めて丁寧に案内
    """
    converter: ConverterFn = reg["converter"]
    module = reg["module"]
    out_default = reg.get("output_csv") or f"output/{master_key}_upsert_ready.csv"

    # まずは (A) 旧流派でトライ
    try:
        sig = inspect.signature(converter)
        if len(sig.parameters) >= 2:
            # 旧流派（第2引数=output_csv）として試行
            return converter(input_all_path, out_default)  # type: ignore[misc]
    except Exception:
        # 旧流派で失敗しても（TypeErrorなど）次の流派へ
        pass

    # (B) 新流派：config が必要
    config_path = pathlib.Path("configs") / f"{master_key.lower()}.yaml"
    if getattr(module, "load_config", None) and callable(module.load_config) and config_path.exists():
        cfg = module.load_config(str(config_path))  # type: ignore[attr-defined]
        return converter(input_all_path, cfg, out_default)  # type: ignore[misc]

    # PyYAML 直読み（load_config が無い流派でも回す）
    if config_path.exists():
        try:
            import yaml  # type: ignore
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            return converter(input_all_path, cfg, out_default)  # type: ignore[misc]
        except Exception as e:
            raise RuntimeError(
                f"{master_key}: config の読み込みに失敗しました: {config_path} | {e}"
            ) from e

    # ここまで来たら config を用意していない（＝ALLを直接 upsert しようとしている）ケース
    raise RuntimeError(
        f"{master_key}: 入力が .ALL ですが変換の設定ファイルが見つかりません。\n"
        f" - 想定パス: {config_path}\n"
        f" - 先に変換を実行: uv run -m api.data_integration.convert_master_generic "
        f"input\\<YOUR>.ALL --config configs\\{master_key.lower()}.yaml --output output\\{master_key}_upsert_ready.csv\n"
        f" - その後: uv run -m api.md_integration.bulk_upsert {master_key} output\\{master_key}_upsert_ready.csv"
    )


# ---------------------------------------------------------------------------
# メイン：変換 → Job作成 → アップロード → クローズ → ポーリング → 結果保存
# ---------------------------------------------------------------------------

def run_bulk_upsert(master_key: str, input_path: str) -> str:
    """
    指定マスタのファイル（ALL または CSV）を取り込み、Bulk Ingest を実行して Job ID を返す。

    CSVが来たら変換スキップ、ALLが来たら変換器をアダプトして実行する。
    """
    print(f"start: bulk upsert for master={master_key}, path={input_path}")

    # 1) レジストリ取得（変換器のメタ＆関数）
    registry = load_converters()
    if master_key not in registry:
        available = ", ".join(sorted(registry.keys()))
        raise KeyError(f"未対応マスタ: {master_key}. 利用可能: {available}")
    reg = registry[master_key]

    # 2) 変換 or スキップ
    p = pathlib.Path(input_path)
    if p.suffix.lower() == ".csv":
        # ★ 最重要：CSVなら変換は呼ばない
        converted_path = str(p)
    else:
        # .ALL 等なら流派差を吸収して変換
        converted_path = _run_converter_adaptively(master_key, str(p), reg)

    # 3) 認証（Client Credentials）
    access_token, instance_url = get_access_token()

    # 4) Job 設定解決（SObject / Operation / ExternalID）
    sf_object, operation, external_id_field = _resolve_job_settings(master_key, registry)

    # 5) Job 作成
    job_url = f"{instance_url}/services/data/v{API_VER}/jobs/ingest"
    job_body = {
        "object": sf_object,
        "operation": operation,
        "lineEnding": "LF",
        "columnDelimiter": "COMMA",
    }
    if operation == "upsert":
        job_body["externalIdFieldName"] = external_id_field

    headers = {"Authorization": f"Bearer {access_token}", **JSON}
    resp = requests.post(job_url, json=job_body, headers=headers)
    job = _check_json(resp, "ジョブ作成")
    job_id = job.get("id")
    if not job_id:
        raise RuntimeError(f"ジョブID取得失敗: {job}")
    print(f"[{master_key}] ジョブ作成: {job_id}")

    # 6) CSV アップロード
    upload_url = f"{job_url}/{job_id}/batches"
    with open(converted_path, "rb") as f:
        up_headers = {"Authorization": f"Bearer {access_token}", **CSV}
        resp = requests.put(upload_url, headers=up_headers, data=f)
    if not resp.ok:
        raise RuntimeError(f"CSVアップロード失敗 status={resp.status_code} body={resp.text}")
    print(f"[{master_key}] CSVアップロード完了: {converted_path}")

    # 7) Job クローズ
    close_url = f"{job_url}/{job_id}"
    resp = requests.patch(close_url, headers=headers, json={"state": "UploadComplete"})
    _check_json(resp, "ジョブクローズ")
    print(f"[{master_key}] ジョブクローズ完了")

    # 8) ポーリング（最大10分 / 5秒間隔）
    deadline = time.time() + 600
    state = None
    while time.time() < deadline:
        info = _get_job(instance_url, access_token, job_id)
        state = info.get("state")
        if state in ("JobComplete", "Failed", "Aborted"):
            print(f"[{master_key}] 完了状態: {state}")
            break
        time.sleep(5)
    else:
        raise TimeoutError(f"ジョブタイムアウト: {job_id}")

    # 9) 成功/失敗 CSV 保存（bytes→明示的にUTF-8へ→Excel用はBOM+CRLF）
    out = pathlib.Path("output")
    out.mkdir(parents=True, exist_ok=True)

    def _to_windows_csv(text: str) -> str:
        # Salesforce応答はLFのことが多い。Windowsツールで扱いやすいよう CRLF に正規化
        return text.replace("\r\n", "\n").replace("\n", "\r\n")

    ok_bytes = _download_result(instance_url, access_token, job_id, "successful")
    ng_bytes = _download_result(instance_url, access_token, job_id, "failed")

    # ★ requestsの誤判定を避けるため、自分でUTF-8としてデコード
    ok_text = ok_bytes.decode("utf-8", errors="strict")
    ng_text = ng_bytes.decode("utf-8", errors="strict")

    # 解析用：UTF-8/LF の“生”ファイルも残す
    (out / f"{job_id}_{master_key}_success_raw.csv").write_text(ok_text, encoding="utf-8")
    (out / f"{job_id}_{master_key}_error_raw.csv").write_text(ng_text, encoding="utf-8")

    # 配布・閲覧用：UTF-8(BOM) + CRLF（Excel/多くのエディタで確実に読める）
    (out / f"{job_id}_{master_key}_success.csv").write_text(_to_windows_csv(ok_text), encoding="utf-8-sig")
    (out / f"{job_id}_{master_key}_error.csv").write_text(_to_windows_csv(ng_text), encoding="utf-8-sig")

    # 10) 異常終了は例外
    if state != "JobComplete":
        raise RuntimeError(f"[{master_key}] ジョブ異常終了: state={state}")

    return job_id


# ---------------------------------------------------------------------------
# スクリプト直実行
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import traceback

    try:
        # 例: python -m api.md_integration.bulk_upsert DPT input/xxx.ALL
        master = sys.argv[1] if len(sys.argv) > 1 else "DPT"
        path = sys.argv[2] if len(sys.argv) > 2 else "input/TEST_DPT.ALL"
        job_id = run_bulk_upsert(master, path)
        print(f"Done. JobID={job_id}")
        print("  - 成功/失敗CSVは output/<jobid>_<master>_success.csv / _error.csv を確認（_raw.csv はUTF-8/LFの素）")
    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        raise
