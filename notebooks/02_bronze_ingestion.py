# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Ingestion
# MAGIC Load raw CSVs into `fmcg.bronze.*` Delta tables with ingestion metadata.
# MAGIC
# MAGIC **Milestone:** all sources land in Bronze before Silver work begins.
# MAGIC
# MAGIC Rules for Bronze:
# MAGIC - Preserve source values (no business cleaning)
# MAGIC - Add ingestion metadata only
# MAGIC - Overwrite full refresh for this first load

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

RAW_PATH = "/Volumes/fmcg/bronze/raw"
CATALOG = "fmcg"
BRONZE = f"{CATALOG}.bronze"
BATCH_ID = str(uuid.uuid4())

# source filename → bronze table name
SOURCE_MAP = {
    "transaction_data.csv": "transactions",
    "product.csv": "products",
    "causal_data.csv": "promotions",
    "coupon.csv": "coupons",
    "coupon_redempt.csv": "coupon_redemptions",
    "campaign_desc.csv": "campaigns",
    "campaign_table.csv": "campaign_households",
    "hh_demographic.csv": "households",
}

print("RAW_PATH:", RAW_PATH)
print("batch_id:", BATCH_ID)
display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ensure schemas exist

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ml")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ai")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest each CSV → Delta (with metadata)

# COMMAND ----------

def add_ingestion_metadata(df, source_file: str, batch_id: str):
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingestion_date", F.current_date())
    )


results = []

for filename, table_name in SOURCE_MAP.items():
    path = f"{RAW_PATH.rstrip('/')}/{filename}"
    table_fqn = f"{BRONZE}.{table_name}"
    print("=" * 80)
    print(f"Loading {filename} → {table_fqn}")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    df = add_ingestion_metadata(df, source_file=filename, batch_id=BATCH_ID)

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_fqn)
    )

    row_count = spark.table(table_fqn).count()
    cols = spark.table(table_fqn).columns
    meta_ok = all(
        c in cols
        for c in ["_ingestion_timestamp", "_source_file", "_batch_id", "_ingestion_date"]
    )
    print(f"Wrote {row_count:,} rows | meta_ok={meta_ok}")
    results.append(
        {
            "source_file": filename,
            "table": table_fqn,
            "rows": row_count,
            "meta_ok": meta_ok,
        }
    )

display(spark.createDataFrame(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Basic Bronze quality checks (transactions)

# COMMAND ----------

txn = spark.table(f"{BRONZE}.transactions")

checks = [
    ("row_count_gt_0", txn.count() > 0),
    ("null_product_id", txn.filter(F.col("PRODUCT_ID").isNull()).count() == 0),
    ("null_store_id", txn.filter(F.col("STORE_ID").isNull()).count() == 0),
    ("null_basket_id", txn.filter(F.col("BASKET_ID").isNull()).count() == 0),
    ("null_household_key", txn.filter(F.col("household_key").isNull()).count() == 0),
    (
        "has_metadata_cols",
        all(
            c in txn.columns
            for c in [
                "_ingestion_timestamp",
                "_source_file",
                "_batch_id",
                "_ingestion_date",
            ]
        ),
    ),
]

check_rows = [{"check": name, "passed": passed} for name, passed in checks]
display(spark.createDataFrame(check_rows))

failed = [name for name, passed in checks if not passed]
assert not failed, f"Bronze quality checks failed: {failed}"
print("Bronze transaction quality checks PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. List Bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN fmcg.bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Milestone checklist
# MAGIC - [x] All 8 CSVs loaded to `fmcg.bronze.*`
# MAGIC - [x] Ingestion metadata present
# MAGIC - [x] Basic transaction key checks passed
# MAGIC - [ ] Ready for Silver (`03_silver_transactions`)
