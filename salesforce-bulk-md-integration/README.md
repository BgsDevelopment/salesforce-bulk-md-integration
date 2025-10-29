# 🚀 bgs-bd-sf-bulk

このプロジェクトは、**Salesforce Bulk API 2.0** を使って  
基幹システム（MD）のデータを Salesforce に取り込んだり、  
Salesforce からデータを CSV で出力（エクスポート）したりするための環境です。

👉 **「MDとSalesforceのデータをつなぐツール集」** 的な物です。

---

## 📂 リポジトリの中身

```
salesforce-bulk-md-integration/
├─ api/                           # パッケージ（← 直叙：__init__.py を置くと安定）
│  ├─ auth/                       # Salesforce認証（トークン取得）
│  │  ├─ __init__.py
│  │  └─ token_client_credentials.py
│  ├─ config/                     # 設定（既定値・環境変数読み込み）
│  │  ├─ __init__.py
│  │  └─ settings.py
│  └─ data_integration/           # データ変換・入出力ロジック
│     ├─ __init__.py
│     ├─ convert_master_generic.py   # MD(.ALL) → Salesforce CSV 変換
│     ├─ bulk_upsert.py              # Bulk Ingest: CSVアップロード～完了待ち
│     └─ bulk_export.py              # SOQL実行 → CSV出力
├─ configs/                       # オブジェクト別の設定（YAML）
│  ├─ dpt.yaml
│  ├─ purchase.yaml
│  └─ orderdetailmd.yaml
├─ input/                         # MD側の入力ファイル置き場
├─ output/                        # 変換結果CSVやエクスポート結果
├─ scripts/
│  └─ export_soql.py              # SOQLでSalesforceデータをCSV出力（CLI）
├─ .env / .env.local / .env.dev   # 認証・環境設定（.gitignore 推奨）
└─ pyproject.toml                 # 依存・ツール設定（uv対応）
```

---

## 🔑 認証の仕組み

Salesforceと通信するためには「トークン」が必要です。  
`api/auth/token_client_credentials.py` が `.env` ファイルから情報を読み取って、自動的にトークンを取得します。

### `.env` の設定例

```dotenv
SF_TOKEN_URL=https://test.salesforce.com/services/oauth2/token
SF_CLIENT_ID=＜Connected AppのConsumer Key＞
SF_CLIENT_SECRET=＜Connected AppのConsumer Secret＞
SF_INSTANCE_URL=https://xxxx.my.salesforce.com

# 以下は任意（設定がなければデフォルト値が使われます）
SFOBJ=Department__c
OPERATION=upsert
EXTERNAL_ID_FIELD=External_Id__c
API_VER=62.0
```

`.env` が見つからない場合は、`.env.local` や `.env.dev` も順に探します。

---

## 🧩 YAML設定ファイルとは？

MDデータ（ALLファイル）を Salesforce 取込用の CSV に変換するために、  
各オブジェクト（例：部署マスタ、仕入伝票など）ごとに設定ファイル（YAML）を用意します。

変換のルール（どの列をどの項目に対応させるか）は YAML に書かれています。

### YAML の例（Department__c）

```yaml
# Department__c の変換設定
master_key: "DPT"                  # ファイル名などで使用
sf_object: "Department__c"         # Salesforceのオブジェクト名
operation: "upsert"                # 実行方法（upsert/insert/updateなど）
external_id_field: "DptCode__c"    # upsert時の外部キー

# 入出力設定
input_encoding: "cp932"            # MDファイルの文字コード（Shift-JIS系）
output_encoding: "utf-8"           # Salesforce用はUTF-8推奨
has_header: false                  # MDファイルにヘッダー行が無い場合はfalse

# マッピング設定（どの列がどのSalesforce項目に対応するか）
mapping:
  - { index: 1,  field: "MdScheduledModDate__c" }
  - { index: 2,  field: "MdMaintenanceCreateDate__c" }
  - { index: 7,  field: "DptCode__c" }
  - { index: 9,  field: "Name" }
  - { index: 10, field: "DptNameKana__c" }

# 出力ファイル名
output_csv: "output/Department_upsert_ready.csv"
```

YAMLを見れば、**どの列がどのSalesforce項目に入るか** 一目で分かるようになっています。

---

## ▶️ よく使うコマンド

### 1. 変換（ALL → CSV）
```powershell
uv run -m api.data_integration.convert_master_generic `
  input\test_dpt_1021.ALL `
  --config configs\dpt.yaml `
  --output output\DPT_upsert_ready.csv
```

- `input` にあるMDファイルを読み込み
- `configs/dpt.yaml` の設定どおりに変換
- 結果を `output/` にCSVで出力（UTF-8, LF改行）

---

### 2. 取込（CSV → Salesforce）
```powershell
uv run -m api.data_integration.bulk_upsert MASTER_GENERIC output\DPT_upsert_ready.csv
```

### 3. 取込（ALL → Salesforce 一気通し）
```powershell
uv run -m api.data_integration.bulk_upsert MASTER_GENERIC input\test_dpt_1021.ALL
```

---

## 🗂 出力ファイル（Bulk結果）

- `*_success_raw.csv` / `*_error_raw.csv` … UTF-8 / LF（解析・差分用）  
- `*_success.csv` / `*_error.csv` … UTF-8(BOM) / CRLF（Excel用）

---

### ③ Salesforceからデータを出力（エクスポート）

Salesforce内のデータをSOQLで検索してCSVに出力できます。

#### 例1：Accountを10件出力
```powershell
uv run python .\scripts\export_soql.py --soql "SELECT Id, Name FROM Account LIMIT 10"
```

#### 例2：仕入伝票を出力
```powershell
uv run python scripts/export_soql.py `
  --soql "SELECT Id, SlipNumber__c, StoreId__c, SupplierId__c, Status__c FROM Purchase__c" `
  --out output/Purchase_export.csv
```

#### 例3：削除済データを含めて出力
```powershell
uv run python scripts/export_soql.py `
  --soql "SELECT Id, IsDeleted, Name FROM Account" `
  --operation queryAll
```

出力結果は `output/<日付>_export.csv` に保存されます。  
複数ページ分も自動で1つのCSVにまとめてくれます。

---

## ⚙️ トラブルが出たときは

| 症状 | 確認ポイント |
|------|---------------|
| `invalid_client` エラー | Connected App の設定（URL・スコープ）を確認 |
| CSVアップロード失敗 | 改行がLF、文字コードがUTF-8になっているか確認 |
| upsert失敗 | external_id_field の指定 or 権限不足を確認 |
| Excelで文字化け | Excelの[データ]→[テキスト/CSVのインポート]→UTF-8指定で開く |

---

## 🧠 スクリプトまとめ（ざっくり役割）

| ファイル名 | 役割 |
|-------------|------|
| `convert_master_generic.py` | MDファイルをSalesforce形式に変換 |
| `bulk_upsert.py` | SalesforceにCSVをアップロードして登録 |
| `bulk_export.py` / `export_soql.py` | SalesforceデータをCSVに出力 |
| `token_client_credentials.py` | Salesforceトークンを取得（認証） |

---

## 💡補足：`uv run`とは？

`uv` は Python の実行・環境管理ツールです。  
`uv run` の後ろにスクリプトを指定することで、  
その環境で安全にスクリプトを実行できます。

もし `uv` が入っていない場合は、  
Python標準のコマンド `python` に置き換えてもOKです。

---

このREADMEを読めば、  
SalesforceとMDのデータ連携の「入口から出口まで」が一通り理解できるようになります。  
次は、`configs/` のYAML設定を実際に作ってみると流れがつかめます。
