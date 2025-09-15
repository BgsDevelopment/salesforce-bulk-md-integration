# api/auth/token_client_credentials.py
# 既存：トークン取得
import os
import requests
from dotenv import load_dotenv, find_dotenv

# settings からの読み込み（存在しない場合にも耐える）
try:
    from api.config import settings as _settings

    CLIENT_ID_DEFAULT = getattr(_settings, "CLIENT_ID", None)
    CLIENT_SECRET_DEFAULT = getattr(_settings, "CLIENT_SECRET", None)
    SF_DOMAIN_DEFAULT = getattr(_settings, "SF_DOMAIN", None)
    TOKEN_URL_DEFAULT = getattr(_settings, "TOKEN_URL", None)  # 任意
except Exception:
    CLIENT_ID_DEFAULT = CLIENT_SECRET_DEFAULT = SF_DOMAIN_DEFAULT = TOKEN_URL_DEFAULT = None


def _load_env_once() -> str | None:
    """
    .env を探索して読み込む。優先順:
      1) 環境変数 SF_ENV_FILE で明示されたパス
      2) .env, .env.local, .env.dev （カレント配下）
    見つかった最初のファイルを読み込む。すでにロード済みでも上書きしない。
    """
    candidates = [os.getenv("SF_ENV_FILE"), ".env", ".env.local", ".env.dev"]
    for name in candidates:
        if not name:
            continue
        path = name if os.path.isabs(name) else find_dotenv(filename=name, usecwd=True)
        if path and os.path.exists(path):
            load_dotenv(dotenv_path=path, override=False)
            if os.getenv("SF_ENV_VERBOSE"):  # 必要なときだけ表示
                print(f"🔧 env loaded: {path}")
            return path
    return None


def _require(name: str, value: str | None):
    if not value:
        raise Exception(
            f"設定不足: {name} が見つかりません。"
            " .env（UTF-8 / ルート直下）や api.config.settings を確認してください。"
        )


def get_access_token():
    """
    Salesforce OAuth2 (Client Credentials Flow) でアクセストークンを取得。
    優先：環境変数 > settings デフォルト
    token_url は SF_TOKEN_URL があればそれを使用、無ければ SF_DOMAIN から生成。
    戻り値: (access_token, instance_url)
    """
    _load_env_once()

    # 値の解決（env > settings）
    client_id = os.getenv("SF_CLIENT_ID") or CLIENT_ID_DEFAULT
    client_secret = os.getenv("SF_CLIENT_SECRET") or CLIENT_SECRET_DEFAULT
    token_url = (
            os.getenv("SF_TOKEN_URL")
            or TOKEN_URL_DEFAULT
            or (
                f"https://{(os.getenv('SF_DOMAIN') or SF_DOMAIN_DEFAULT)}/services/oauth2/token"
                if (os.getenv("SF_DOMAIN") or SF_DOMAIN_DEFAULT)
                else None
            )
    )
    instance_url = os.getenv("SF_INSTANCE_URL")  # 任意。無ければ後でドメインから補完

    # 必須チェック
    _require("SF_CLIENT_ID/CLIENT_ID", client_id)
    _require("SF_CLIENT_SECRET/CLIENT_SECRET", client_secret)
    _require("SF_TOKEN_URL もしくは SF_DOMAIN", token_url)

    # リクエスト
    resp = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )

    if not resp.ok:
        # ヒントを含めてデバッグしやすく
        head = (client_id or "")[:6]
        raise Exception(
            f"トークン取得失敗: {resp.text} "
            f"(token_url={token_url}, client_id_prefix={head})\n"
            "  - TOKEN_URL と org (login/test) の整合性、Connected App の Client Credentials 有効化、"
            "    Consumer Key/Secret の貼り間違い/末尾スペース を確認してください。"
        )

    data = resp.json()
    access_token = data.get("access_token")
    _require("access_token (応答)", access_token)

    # instance_url は応答 or .env → それも無ければ token_url/SF_DOMAIN から補完
    if not instance_url:
        instance_url = data.get("instance_url")
    if not instance_url:
        # token_url のホスト部 or SF_DOMAIN を使って補完
        from urllib.parse import urlparse
        host = urlparse(token_url).hostname or (os.getenv("SF_DOMAIN") or SF_DOMAIN_DEFAULT)
        if not host:
            raise Exception("instance_url が判定できません。.env の SF_INSTANCE_URL を設定してください。")
        instance_url = f"https://{host}"

    return access_token, instance_url
