"""\
api/md_integration/bulk_upsert.py

Salesforce Bulk API（Ingest）で MD → Salesforce 連携を行う単発実行スクリプト。
以下の処理を最小限に保ちつつ、各処理の意図・前提・失敗時の挙動を明記。

- 変換器（convert_*.py）の自動検出＆呼び出し
- OAuth2（Client Credentials）でアクセストークン取得
- Ingest Job の作成 → CSVアップロード → クローズ → ポーリング
- 成功/失敗CSVのダウンロード＆保存

備考：
- 既定値（SFOBJ/OPERATION/EXTERNAL_ID_FIELD/API_VER）は api.config.settings から読み込み
- 変換器側のメタ（MASTER_KEY/SF_OBJECT/OPERATION 等）があれば優先
- upsert は external_id_field が必須
- 入力ALLの読み込みやエンコードは各 convert_any.py に委譲（例：DPTはCP932）

将来のモジュール化方針（仮）：
- converters/registry.py : load_converters() を切り出し
- bulk/sf_ingest.py      : create/upload/close/get/download を関数化
- cli/main.py            : convert / job-* のサブコマンド分割
"""

import time
import pathlib
import requests
import importlib
import pkgutil
from typing import Callable, Optional, Dict, Any, Tuple

from api.auth.token_client_credentials import get_access_token
from api.config.settings import (
    SFOBJ as DEFAULT_SFOBJ,
    OPERATION as DEFAULT_OPERATION,
    EXTERNAL_ID_FIELD as DEFAULT_EXTERNAL_ID_FIELD,
    API_VER,
)

# 変換器の関数シグネチャ： (input_all_path, output_csv_path) -> output_csv_path
ConverterFn = Callable[[str, str], str]

# Salesforce REST への Content-Type ヘッダ定数
JSON = {"Content-Type": "application/json"}
CSV = {"Content-Type": "text/csv"}


# ---------------------------------------------------------------------------
# コンバータ自動ロード
# ---------------------------------------------------------------------------

def _current_pkg_name() -> str:
    """この bulk_upsert.py が属する“親パッケージ名”を推定して返す。

    例: 'api.md_integration.bulk_upsert' から 'api.md_integration' を返す。
    __package__ が無い・不定な環境（直接実行など）の保険として、
    ファイルパスから 'api/<sub>' を推定するフォールバックも備える。
    """
    if __package__:
        parts = __package__.split(".")
        # 末尾が bulk_upsert のときは 1 つ上のパッケージ名を返す
        if parts[-1] == "bulk_upsert":
            return ".".join(parts[:-1])
        return __package__

    # __package__ が空のとき：ファイルパスから推定
    here = pathlib.Path(__file__).resolve()
    parts = list(here.parts)
    if "api" in parts:
        i = parts.index("api")
        if i + 1 < len(parts):
            return f"api.{parts[i + 1]}"
    # 最後の保険：最も一般的な既定値
    return "api.md_integration"


def _choose_converter_pkgs() -> list[str]:
    """convert_any.py を探索する候補パッケージを、優先順で返す。

    優先順位：
      1) 自分（このファイルの親パッケージ）
      2) api.data_integration
      3) api.integration
      4) api.md_integration

    ※最初に見つかったパッケージのみ採用（複数混在時の不整合を防ぐ）。
    """
    me = _current_pkg_name()
    candidates = [me, "api.data_integration", "api.integration", "api.md_integration"]
    # 重複除去（順序保持）
    uniq: list[str] = []
    for c in candidates:
        if c not in uniq:
            uniq.append(c)
    return uniq


def load_converters() -> Dict[str, Dict[str, Any]]:
    """convert_any.py を自動 import してレジストリ辞書を返す。

    各 convert モジュールの想定仕様（任意。未定義はデフォルト適用）:
      - MASTER_KEY:         実行キー（例: 'DPT'）
      - OUTPUT_CSV:         出力CSVパス（例: 'output/Department_upsert_ready.csv'）
      - SF_OBJECT:          対象 SObject（例: 'Department__c'）
      - OPERATION:          'upsert' / 'insert' / 'update' / 'delete'
      - EXTERNAL_ID_FIELD:  upsert時の外部ID項目
      - convert_md_to_salesforce(input_all, output_csv) -> output_csv  （必須）

    返り値：
      { MASTER_KEY: { converter, output_csv, sf_object, operation, external_id_field } }

    例外：
      - 1つも見つからなかった場合は RuntimeError
    """
    registry: Dict[str, Dict[str, Any]] = {}
    tried_pkgs: list[str] = []

    for pkg_name in _choose_converter_pkgs():
        tried_pkgs.append(pkg_name)
        try:
            pkg = importlib.import_module(pkg_name)
        except ModuleNotFoundError:
            # 候補に無い環境（ディレクトリ無しなど）はスキップ
            continue

        found_any = False
        # pkg.__path__ を起点に convert_*.py を探索
        for _, fullname, ispkg in pkgutil.iter_modules(pkg.__path__, pkg.__name__ + "."):
            mod_short = fullname.rsplit(".", 1)[-1]
            if not mod_short.startswith("convert_"):
                continue  # convert_*.py のみ対象

            m = importlib.import_module(fullname)
            # 必須：変換関数。CONVERTER（呼び出し可能）を許容する互換性も確保
            converter = getattr(m, "convert_md_to_salesforce", None) or getattr(m, "CONVERTER", None)
            if not callable(converter):
                # 変換関数が無ければ対象外
                continue

            # メタ情報（未定義はデフォルト）
            master_key = getattr(m, "MASTER_KEY", mod_short.replace("convert_", "").upper())
            output_csv = getattr(m, "OUTPUT_CSV", f"output/{master_key}_upsert_ready.csv")
            sf_object = getattr(m, "SF_OBJECT", None)
            operation = getattr(m, "OPERATION", None)
            external_id_field = getattr(m, "EXTERNAL_ID_FIELD", None)

            registry[master_key] = {
                "converter": converter,
                "output_csv": output_csv,
                "sf_object": sf_object,
                "operation": operation,
                "external_id_field": external_id_field,
            }
            found_any = True

        if found_any:
            # 最初に見つかったパッケージで確定（他候補は見に行かない）
            return registry

    # どの候補でも見つからなかった
    raise RuntimeError("convert_*.py が見つかりません。探索候補: " + ", ".join(tried_pkgs))


# ---------------------------------------------------------------------------
# Salesforce REST ユーティリティ
# ---------------------------------------------------------------------------

def _check_json(resp: requests.Response, where: str):
    """Salesforce REST 応答を JSON として検証し、エラー時は詳細を含めて例外化。

    Args:
        resp: requests.Response
        where: エラーメッセージに含める識別（どの処理か）
    Returns:
        dict: パースした JSON
    Raises:
        RuntimeError: 非JSON or ステータスエラー
    """
    try:
        data = resp.json()
    except Exception:
        # API からの非JSON応答（HTML/テキスト）を検知
        raise RuntimeError(f"{where}: 非JSON応答 status={resp.status_code} body={resp.text[:1000]}")
    if not resp.ok:
        # Salesforce が返すエラー JSON をそのまま body に含めて可視化
        raise RuntimeError(f"{where}: エラー status={resp.status_code} body={data}")
    return data


def _get_job(instance_url: str, access_token: str, job_id: str):
    """Ingest Job 情報の取得。

    - state（JobComplete/Failed/Aborted など）を監視で利用
    """
    url = f"{instance_url}/services/data/v{API_VER}/jobs/ingest/{job_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return _check_json(r, "ジョブ取得")


def _download_result(instance_url: str, access_token: str, job_id: str, kind: str) -> str:
    """成功/失敗結果の CSV テキストを取得。

    Args:
        kind: 'successful' or 'failed'
    Returns:
        str: CSV テキスト。応答NG時は空文字（存在しない場合など）
    """
    url = f"{instance_url}/services/data/v{API_VER}/jobs/ingest/{job_id}/{kind}Results"
    r = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    return r.text if r.ok else ""


def _resolve_job_settings(
        master_key: str, registry: Dict[str, Dict[str, Any]]
) -> Tuple[str, str, Optional[str]]:
    """ジョブ作成に必要な設定を決定（変換器メタ > 既定値 の順）。

    Returns:
        (sf_object, operation, external_id_field)
    Raises:
        KeyError: 未登録マスタ
        ValueError: upsert に external_id_field が未指定
    """
    cfg = registry.get(master_key)
    if not cfg:
        raise KeyError(f"未登録のマスタです: {master_key}")

    sf_object = cfg.get("sf_object") or DEFAULT_SFOBJ
    operation = (cfg.get("operation") or DEFAULT_OPERATION).lower()
    external_id_field = cfg.get("external_id_field") or DEFAULT_EXTERNAL_ID_FIELD

    if operation == "upsert" and not external_id_field:
        raise ValueError(f"{master_key}: upsert には external_id_field が必須です")
    return sf_object, operation, external_id_field


# ---------------------------------------------------------------------------
# メイン：一気通し実行（変換 → Job 作成 → アップロード → クローズ → 待機 → 結果保存）
# ---------------------------------------------------------------------------

def run_bulk_upsert(master_key: str, input_all_path: str) -> str:
    """指定マスタの ALL ファイルを取り込み、Bulk Ingest を実行して Job ID を返す。

    Steps:
      1) 変換器の自動ロード → 指定 master_key のレジストリ取得
      2) 変換器を呼び出して CSV 生成
      3) アクセストークン取得（Client Credentials Flow）
      4) Job 作成（必要に応じ ExternalId 指定）
      5) CSV アップロード
      6) Job クローズ（state=UploadComplete）
      7) ポーリング（最大 600 秒 / 5 秒間隔）
      8) 成功/失敗 CSV ダウンロード＆保存（utf-8）

    Returns:
      str: 作成された Job ID

    Raises:
      - KeyError/ValueError/RuntimeError/TimeoutError : 各処理の失敗
    """
    # 1) 変換器レジストリのロード
    registry = load_converters()
    if master_key not in registry:
        available = ", ".join(sorted(registry.keys()))
        raise KeyError(f"未対応マスタ: {master_key}. 利用可能: {available}")

    reg = registry[master_key]
    converter: ConverterFn = reg["converter"]
    output_csv_path = reg["output_csv"]

    # 2) 変換（ALL → SF向けCSV）
    converted_path = converter(input_all_path, output_csv_path)

    # 3) 認証（環境変数・.env に基づきトークンとインスタンスURLを取得）
    access_token, instance_url = get_access_token()

    # 4) 設定解決（SObject/ Operation / ExternalId）
    sf_object, operation, external_id_field = _resolve_job_settings(master_key, registry)

    # 5) Job 作成（JSON）
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

    # 6) CSV アップロード（PUT /batches）
    upload_url = f"{job_url}/{job_id}/batches"
    with open(converted_path, "rb") as f:
        up_headers = {"Authorization": f"Bearer {access_token}", **CSV}
        resp = requests.put(upload_url, headers=up_headers, data=f)
    if not resp.ok:
        raise RuntimeError(f"CSVアップロード失敗 status={resp.status_code} body={resp.text}")
    print(f"[{master_key}] CSVアップロード完了: {converted_path}")

    # 7) Job クローズ（state=UploadComplete）
    close_url = f"{job_url}/{job_id}"
    resp = requests.patch(close_url, headers=headers, json={"state": "UploadComplete"})
    _check_json(resp, "ジョブクローズ")
    print(f"[{master_key}] ジョブクローズ完了")

    # 8) ポーリング（最大10分：5秒間隔）
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
        # while を抜けずに deadline 到達
        raise TimeoutError(f"ジョブタイムアウト: {job_id}")

    # 9) 成否CSVのダウンロード＆保存（存在しない場合は空文字になり得る）
    ok_csv = _download_result(instance_url, access_token, job_id, "successful")
    ng_csv = _download_result(instance_url, access_token, job_id, "failed")

    out = pathlib.Path("output")
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{job_id}_{master_key}_success.csv").write_text(ok_csv, encoding="utf-8")
    (out / f"{job_id}_{master_key}_error.csv").write_text(ng_csv, encoding="utf-8")

    # 10) Job 完了判定（Failed/Aborted なら例外とする）
    if state != "JobComplete":
        raise RuntimeError(f"[{master_key}] ジョブ異常終了: state={state}")

    return job_id


# ---------------------------------------------------------------------------
# スクリプト直実行エントリポイント
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import traceback

    try:
        # 使い方： python -m api.md_integration.bulk_upsert DPT input/xxx.ALL
        master = sys.argv[1] if len(sys.argv) > 1 else "DPT"
        path = sys.argv[2] if len(sys.argv) > 2 else "input/TEST_DIV.20250305042159.ALL"
        print(f"start: bulk upsert for master={master}, path={path}")
        job_id = run_bulk_upsert(master, path)
        print(f"Done. JobID={job_id}")
        print("  - 成功/失敗CSVは output/<jobid>_<master>_success.csv / _error.csv を確認")
    except Exception as e:
        # 失敗時はスタックトレースを出力して非0終了
        print("ERROR:", e)
        traceback.print_exc()
        raise
