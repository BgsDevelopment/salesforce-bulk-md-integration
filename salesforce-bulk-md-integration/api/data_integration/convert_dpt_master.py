# # api/md_integration/convert_dpt_master.py
#
# import pandas as pd
# from pathlib import Path
#
#
# # 外部ファイルからの呼び出しでも利用できるように定数を定義
# # mappingも同上
# MASTER_KEY = "DPT"
# OUTPUT_CSV = "output/Department_upsert_ready.csv"
# SF_OBJECT = None  # 個別指定なければ共通設定を利用
# OPERATION = None
# EXTERNAL_ID_FIELD = None
#
#
# def convert_md_to_salesforce(input_path: str, output_path: str) -> str:
#     """
#     MD連携ファイル（.ALL形式）をSalesforce Bulk API 2.0で利用可能なCSVに変換する。
#
#     主な処理内容：
#     - Shift-JIS（CP932）エンコードのALLファイルを読み込む
#     - 特定の列をSalesforceのAPI項目名にマッピングして抽出
#     - UTF-8（BOMなし）でCSVファイルとして保存
#
#     Args:
#         input_path (str): 元となるMDファイル（.ALL）のパス
#         output_path (str): 出力先CSVファイルのパス
#
#     Returns:
#         str: 生成されたCSVファイルのパス
#     """
#
#     # ✅ 列番号（0始まり）→ Salesforce API項目名 の対応マッピング
#     # MD側に「（MD）」項目がある場合はそちらを優先し、無い場合はSalesforce標準項目（例: Name）へマッピング
#     mapping = {
#         1: "MdScheduledModDate__c",  # (MD)更新予定日付
#         2: "MdMaintenanceCreateDate__c",  # (MD)メンテナンスレコード作成日付
#         7: "DptCode__c",  # (MD)DPTコード（外部ID）
#         9: "Name",  # DPT名（MD項目なし → Name へ）
#         10: "DptNameKana__c",  # DPT名カナ
#         11: "InventoryUpdateTypeCode__c",  # (MD)在庫更新区分コード
#         12: "TaxTypeLabelCode__c",  # (MD)課税区分コード
#         13: "NonSalesFlagCode__c",  # (MD)売上外フラグコード
#         23: "MdRegistDate__c",  # (MD)登録日
#         24: "MdModDate__c"  # (MD)更新日
#     }
#
#     # 📥 MD連携ファイルをDataFrameとして読み込み（Shift-JIS）
#     df_raw = pd.read_csv(input_path, header=None, encoding="cp932", dtype=str)
#
#     # 🎯 必要な列のみ抽出し、Salesforce用の列名に変換
#     df_selected = df_raw[list(mapping.keys())].rename(columns=mapping)
#
#     # 👤 所有者項目は空欄で追加（Salesforceで自動割り当てされることを想定）
#     df_selected["OwnerId"] = ""
#
#     # 💾 出力先フォルダがなければ作成
#     Path(output_path).parent.mkdir(parents=True, exist_ok=True)
#
#     # 📝 UTF-8でCSVとして保存（LF改行）
#     df_selected.to_csv(output_path, index=False, encoding="utf-8", lineterminator="\n")
#
#     print(f"✅ 変換完了: {output_path}")
#     return output_path
