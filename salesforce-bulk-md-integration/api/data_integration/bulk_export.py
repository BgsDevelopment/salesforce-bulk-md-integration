# api/data_integration/bulk_export.py
import csv
import pathlib
import time
from typing import Optional, Dict, Any, Tuple, Iterable

import requests

from api.auth.token_client_credentials import get_access_token
from api.config.settings import API_VER

JSON = {"Content-Type": "application/json", "Accept": "application/json"}


class BulkQueryError(RuntimeError):
    pass


def _api_base(instance_url: str) -> str:
    return f"{instance_url}/services/data/v{API_VER}"


def _create_query_job(base_url: str, access_token: str, soql: str, operation: str = "query",
                      column_delimiter: str = "COMMA", line_ending: str = "LF",
                      pk_chunking: Optional[str] = None) -> str:
    """
    Bulk API 2.0: Create Query Job
    operation: "query" or "queryAll"
    """
    headers = {
        **JSON,
        "Authorization": f"Bearer {access_token}"
    }
    body = {
        "operation": operation,
        "query": soql,
        # 返却CSVの体裁
        "columnDelimiter": column_delimiter,
        "lineEnding": line_ending,
    }
    if pk_chunking:
        # 例: "chunkSize=100000" など
        body["pkChunking"] = pk_chunking

    r = requests.post(f"{base_url}/jobs/query", json=body, headers=headers, timeout=60)
    if r.status_code >= 300:
        raise BulkQueryError(f"Create job failed: {r.status_code} {r.text}")
    return r.json()["id"]


def _get_job(base_url: str, access_token: str, job_id: str) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    r = requests.get(f"{base_url}/jobs/query/{job_id}", headers=headers, timeout=60)
    if r.status_code >= 300:
        raise BulkQueryError(f"Get job failed: {r.status_code} {r.text}")
    return r.json()


def _wait_until_complete(base_url: str, access_token: str, job_id: str,
                         poll_sec: float = 2.0, timeout_sec: float = 1800.0) -> Dict[str, Any]:
    """
    JobComplete になるまで待機。タイムアウトで例外。
    """
    start = time.time()
    while True:
        job = _get_job(base_url, access_token, job_id)
        state = job.get("state")
        if state == "JobComplete":
            return job
        if state in ("Aborted", "Failed"):
            raise BulkQueryError(f"Job {job_id} ended with state={state}: {job}")
        if time.time() - start > timeout_sec:
            raise BulkQueryError(f"Timeout waiting for job {job_id} to complete")
        time.sleep(poll_sec)


def _iter_results_pages(base_url: str, access_token: str, job_id: str,
                        max_records: int = 100000) -> Iterable[Tuple[bytes, Dict[str, str]]]:
    """
    結果CSVをページングで取得。
    戻り: (content_bytes, response_headers) をページごとにyield
    - 2回目以降は 'Sforce-Locator' を query param に付与
    """
    url = f"{base_url}/jobs/query/{job_id}/results"
    headers = {
        "Authorization": f"Bearer {access_token}",
        # 結果はCSVで欲しい
        "Accept": "text/csv",
    }
    locator: Optional[str] = None

    while True:
        params = {"maxRecords": str(max_records)}
        if locator:
            params["locator"] = locator

        r = requests.get(url, headers=headers, params=params, timeout=300, stream=True)
        if r.status_code >= 300:
            raise BulkQueryError(f"Get results failed: {r.status_code} {r.text}")

        # 全バイトを受信（requestsはgzip自動解凍）
        content = r.content
        yield content, r.headers

        # 次ロケータ
        locator = r.headers.get("Sforce-Locator")
        # Sforce-Locator が 'null' または欠落で終端
        if not locator or locator.lower() == "null":
            break


def export_soql_to_csv(soql: str,
                       output_csv_path: pathlib.Path,
                       operation: str = "query",
                       max_records_per_page: int = 100000,
                       pk_chunking: Optional[str] = None,
                       include_header_once: bool = True) -> Dict[str, Any]:
    """
    SOQLの結果をCSVで保存する高水準関数。
    - ページング（Sforce-Locator）を繰り返し、単一CSVに追記
    - include_header_once=True: 2ページ目以降のヘッダ行を自動でスキップ
    戻り: {"job_id": ..., "rows": n, "pages": m, "path": str(output_csv_path)}
    """
    access_token, instance_url = get_access_token()
    base_url = _api_base(instance_url)

    job_id = _create_query_job(
        base_url, access_token, soql, operation=operation,
        pk_chunking=pk_chunking
    )
    job = _wait_until_complete(base_url, access_token, job_id)

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    pages = 0
    rows = 0
    header_written = False

    with open(output_csv_path, "wb") as fw:
        for content, headers in _iter_results_pages(base_url, access_token, job_id, max_records=max_records_per_page):
            pages += 1
            if not include_header_once:
                # そのまま連結
                fw.write(content)
                # 粗カウント（改行数-1程度）※厳密カウントは後段で必要に応じて
                rows += max(content.count(b"\n") - 1, 0)
                continue

            # ヘッダ一回だけにしたい場合：2ページ目以降の最初の行を除去
            if not header_written:
                fw.write(content)
                header_written = True
                rows += max(content.count(b"\n") - 1, 0)
            else:
                # 1行目（ヘッダ）を落として追記
                first_newline = content.find(b"\n")
                if first_newline == -1:
                    # 空ページ扱い
                    continue
                fw.write(content[first_newline + 1:])
                rows += max(content.count(b"\n") - 1, 0)

    return {
        "job_id": job_id,
        "rows": rows,
        "pages": pages,
        "path": str(output_csv_path),
        "job": job,
    }
