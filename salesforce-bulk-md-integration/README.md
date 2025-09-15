# bgs-bd-sf-bulk

本リポジトリは **Salesforce Bulk API 2.0** を用いて、基幹（MD）のマスタ/トランザクションデータを Salesforce に**アップサート／インポート**したり、SOQLで**エクスポート**するための実行環境です。

## 📦 リポジトリ構成

```
bgs-bd-sf-bulk/
├─ api/
│  ├─ auth/                   # token_client_credentials.py: Client Credentials 認証
│  ├─ config/                 # settings.py: 既定値
│  └─ data_integration/
│     ├─ convert_master_generic.py   # ★ 汎用コンバータ（YAML/JSON 駆動）
│     ├─ bulk_upsert.py              # Bulk Ingest（CSVアップロード〜完了待ち）
│     └─ bulk_export.py              # Bulk Query（SOQL→CSV）
├─ configs/                   # ★ オブジェクトごとの YAML 設定
│  ├─ dpt.yaml
│  ├─ purchase.yaml
│  └─ orderdetailmd.yaml
├─ input/                     # MDのALLファイル配置
├─ output/                    # 変換済みCSV / 成否結果
└─ scripts/
   └─ export_soql.py          # Bulk Export CLI
.env / .env.local / .env.dev  # 認証情報（UTF-8保存）
```

---

## 🔐 認証（token_client_credentials.py）

`.env` から Salesforce のトークンを取得します。探索順序：`SF_ENV_FILE` → `.env` → `.env.local` → `.env.dev`。

```dotenv
SF_TOKEN_URL=https://test.salesforce.com/services/oauth2/token
SF_CLIENT_ID=＜Connected App Consumer Key＞
SF_CLIENT_SECRET=＜Connected App Consumer Secret＞
SF_INSTANCE_URL=https://xxxx.my.salesforce.com

# 任意の既定値（settings.pyに無ければ利用）
SFOBJ=Department__c
OPERATION=upsert
EXTERNAL_ID_FIELD=External_Id__c
API_VER=62.0
```

失敗時はヒント付き例外を返します。

---

## 🧩 YAML仕様と運用ルール

各オブジェクトごとに `configs/` 以下に YAML を作成します。`convert_master_generic.py` がこの設定を読み取り、ALLファイルを Salesforce 取込用のCSVに変換します。

### YAMLの基本構造（例: Department__c）

```yaml
master_key: DPT
sf_object: Department__c
operation: upsert
external_id_field: External_Id__c

input_encoding: cp932
output_encoding: utf-8
lineterminator: "\n"
delimiter: ","
has_header: false
output_csv: output/Department_upsert_ready.csv

owner_id_column: OwnerId
owner_id_value: ""
extra_fields:
  MdSourceSystem__c: "MD"

mapping:
  - { index: 0, field: External_Id__c }
  - { index: 1, field: DptCode__c }
  - { index: 2, field: DptName__c }
```

### 他オブジェクトのテンプレート

#### `configs/purchase.yaml`（仕入伝票 Purchase__c）

```yaml
master_key: PUR
sf_object: Purchase__c
operation: upsert
external_id_field: SlipNumber__c
output_csv: output/Purchase_upsert_ready.csv
mapping:
  - { index: 0, field: SlipNumber__c }
  - { index: 1, field: StoreId__c }
  - { index: 2, field: SupplierId__c }
  - { index: 3, field: Status__c }
```

#### `configs/orderdetailmd.yaml`（発注商品 OrderDetailMD__c）

```yaml
master_key: ODM
sf_object: OrderDetailMD__c
operation: upsert
external_id_field: MdOrderDetailNumber__c
output_csv: output/OrderDetailMD_upsert_ready.csv
mapping:
  - { index: 0, field: MdOrderDetailNumber__c }
  - { index: 1, field: MdStoreCode__c }
  - { index: 2, field: ProductJan__c }
  - { index: 3, field: Qty__c }
```

> 運用ルール：**必ず configs/ 以下にオブジェクトごとの YAML を配置する**。

---

## ▶️ 実行方法

### 変換（ALL → Salesforce CSV）

```powershell
uv run -m api.data_integration.convert_master_generic `
  input\TEST_DIV.ALL `
  --config configs/purchase.yaml
```

- 出力は UTF-8 / LF / ヘッダ付きCSV
- mapping にない列は出力されません

### 取込（Ingest）

- **方法1**: 変換済みCSVを `bulk_upsert.py` または curl で投入
- **方法2**: 薄いラッパー `convert_<master>.py` を用意して、`bulk_upsert.py` の自動検出に乗せる

### エクスポート（Query）
#### エクスポートの実行例

以下のように、SOQLを指定してSalesforceのデータをCSVに出力できます。

- **基本（Accountを10件）**
```powershell
uv run python scripts/export_soql.py --soql "SELECT Id, Name FROM Account LIMIT 10"
```

- **任意のオブジェクト（仕入伝票 Purchase__c を出力）**
```powershell
uv run python scripts/export_soql.py `
  --soql "SELECT Id, SlipNumber__c, StoreId__c, SupplierId__c, Status__c FROM Purchase__c" `
  --out output/Purchase_export.csv
```

- **削除済やアーカイブを含めて出力**
```powershell
uv run python scripts/export_soql.py `
  --soql "SELECT Id, IsDeleted, Name FROM Account" `
  --operation queryAll
```

- **大量データを効率的に出力（ページサイズ＋PKチャンク指定）**
```powershell
uv run python scripts/export_soql.py `
  --soql "SELECT Id, Name FROM CustomProduct__c" `
  --page 150000 `
  --pk-chunking "chunkSize=100000"
```

> 出力は `output/<timestamp>_export.csv` に保存されます。  
> 2ページ目以降のヘッダは自動で削除され、1つのCSVにまとまります。


```powershell
uv run python scripts/export_soql.py --soql "SELECT Id, Name FROM Account LIMIT 10"
```

---

## 🚑 トラブルシュート

- 認証エラー：`invalid_client` → Connected App, Scope, URL整合を確認
- CSVアップロード失敗：改行LF, UTF-8, 項目API名, FLSを確認
- upsert失敗：external_id_field の指定漏れ/権限不足
- Excel文字化け：**データ→テキスト/CSV→UTF-8**で読込

---

## 📚 モジュール関係

- `convert_master_generic.py` … 変換（YAML駆動）
- `bulk_upsert.py` … 取込（Ingest：ジョブ作成〜完了待ち）
- `bulk_export.py` / `scripts/export_soql.py` … 出力（Query）
- `token_client_credentials.py` … 認証（.env自動検出）

---

