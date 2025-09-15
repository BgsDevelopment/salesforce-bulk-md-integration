from dotenv import load_dotenv
import os
from pathlib import Path

# 環境名を ENV 環境変数から取得（未設定なら dev とする）
env = os.getenv("ENV", "dev")

# .env.dev / .env.stg / .env.prod などを読み込む
dotenv_path = Path(f".env.{env}")
load_dotenv(dotenv_path=dotenv_path)

# 認証情報
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SF_DOMAIN = os.getenv("SF_DOMAIN", "beisiadenki--dev01.sandbox.my.salesforce.com")

# Bulk API 2.0 用
SFOBJ = os.getenv("SFOBJ", "Department__c")           # 対象オブジェクト
OPERATION = os.getenv("OPERATION", "upsert")          # insert / update / upsert
EXTERNAL_ID_FIELD = os.getenv("EXTERNAL_ID_FIELD", "DptCode__c")  # upsert時に使う外部ID
# settings.py（末尾の設定群の後ろに追記）
API_VER = os.getenv("SF_API_VER", "60.0")

# 先頭に 'v' が付いても安全に取り除く
if API_VER.lower().startswith("v"):
    API_VER = API_VER[1:]
