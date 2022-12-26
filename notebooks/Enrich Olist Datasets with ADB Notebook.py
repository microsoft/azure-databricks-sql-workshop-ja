# Databricks notebook source
# MAGIC %md
# MAGIC # 1. 環境セットアップ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-1. 定数設定
# MAGIC 🔍**以下の定数のセルのみ自身の環境に合わせて書き換えてください (1-2 以降のセルは編集不要です)**

# COMMAND ----------

# ストレージ アカウントの名前
STORAGE_ACCOUNT = "stadbwssmith20221205"
# コンテナの名前
CONTAINER = "olist-brazilian-ecommerce"
# シークレット スコープ
SECRET_SCOPE = "my-secret-scope"
# ストレージ アカウントのシークレット名
SECRET_NAME = "secret-storage-account-key"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1-2. アカウント キーによる認証

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_NAME))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. データ加工

# COMMAND ----------

# スキーマ作成 (ここまでの手順で作成済みのはずだが念のため)
spark.sql("CREATE SCHEMA IF NOT EXISTS olist_enriched;")

# COMMAND ----------

# Delta テーブルへの書き込み用関数を定義
def csv_to_delta(path_csv: str, path_delta: str, table_delta: str) -> None:
    # CSV から DataFrame に読み込み
    df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(path_csv)
    # Delta テーブルに書き込み
    (df.write
        .format("delta")
        .mode("overwrite")
        .option("delta.autoOptimize.autoCompact", True)
        .option("delta.autoOptimize.optimizeWrite", True)
        .saveAsTable(table_delta, path=path_delta))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-1. olist_customers_dataset

# COMMAND ----------

# パスなどの変数を設定
path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_customers_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/customers"
table_delta = "olist_enriched.customers"

# Delta テーブルに書き込み
csv_to_delta(path_csv, path_delta, table_delta)
# Delta テーブルのデータ確認
display(spark.table(table_delta))
# Delta テーブルのプロパティ確認
display(spark.sql(f"DESC DETAIL {table_delta}"))
# Delta テーブルのバージョン確認
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-2. olist_geolocation_dataset

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_geolocation_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/geolocation"
table_delta = "olist_enriched.geolocation"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-3. olist_order_payments_dataset

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_order_payments_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/order_payments"
table_delta = "olist_enriched.order_payments"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-4. olist_order_reviews_dataset

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_order_reviews_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/order_reviews"
table_delta = "olist_enriched.olist_order_reviews"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-5. olist_products_dataset

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_products_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/products"
table_delta = "olist_enriched.products"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-6. olist_sellers_dataset

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/olist_sellers_dataset.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/sellers"
table_delta = "olist_enriched.sellers"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2-7. product_category_name_translation

# COMMAND ----------

path_csv = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/product_category_name_translation.csv"
path_delta = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/enriched/product_category_name_translation"
table_delta = "olist_enriched.product_category_name_translation"

csv_to_delta(path_csv, path_delta, table_delta)
display(spark.table(table_delta))
display(spark.sql(f"DESC DETAIL {table_delta}"))
display(spark.sql(f"DESC HISTORY {table_delta}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 環境クリーンアップ用 (普段はコメントアウト)
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.customers;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.geolocation;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.order_payments;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.order_reviews;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.products;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.sellers;
# MAGIC -- DROP TABLE IF EXISTS olist_enriched.product_category_name_translation;
