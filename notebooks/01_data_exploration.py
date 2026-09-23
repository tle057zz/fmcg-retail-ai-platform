# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data Exploration
# MAGIC Profile dunnhumby Complete Journey CSVs **before** Bronze ingestion.
# MAGIC
# MAGIC **Before running:** set `RAW_PATH` to your Databricks Volume or DBFS folder that contains the 8 CSV files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config — edit this path

# COMMAND ----------

# Confirmed working Volume path (from exploration run):
RAW_PATH = "/Volumes/fmcg/bronze/raw"

FILES = [
    "transaction_data.csv",
    "product.csv",
    "causal_data.csv",
    "coupon.csv",
    "coupon_redempt.csv",
    "campaign_desc.csv",
    "campaign_table.csv",
    "hh_demographic.csv",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List files at RAW_PATH

# COMMAND ----------

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Profile each CSV

# COMMAND ----------

from pyspark.sql import functions as F

profiles = []

for name in FILES:
    path = f"{RAW_PATH.rstrip('/')}/{name}"
    print("=" * 80)
    print(f"FILE: {name}")
    try:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )
        row_count = df.count()
        nulls = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
        print(f"rows={row_count:,}  cols={len(df.columns)}")
        df.printSchema()
        display(df.limit(5))
        print("null counts:", nulls)
        profiles.append({"file": name, "rows": row_count, "columns": len(df.columns), "ok": True})
    except Exception as e:
        print(f"ERROR reading {name}: {e}")
        profiles.append({"file": name, "rows": None, "columns": None, "ok": False, "error": str(e)})

display(spark.createDataFrame(profiles))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Key cardinalities — transaction_data

# COMMAND ----------

txn = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(f"{RAW_PATH.rstrip('/')}/transaction_data.csv")
)

print("unique households:", txn.select("household_key").distinct().count())
print("unique stores:", txn.select("STORE_ID").distinct().count())
print("unique products:", txn.select("PRODUCT_ID").distinct().count())
print("unique baskets:", txn.select("BASKET_ID").distinct().count())
print("week range:", txn.agg(F.min("WEEK_NO"), F.max("WEEK_NO")).collect()[0])
print("day range:", txn.agg(F.min("DAY"), F.max("DAY")).collect()[0])
print("exact duplicate rows:", txn.count() - txn.dropDuplicates().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checklist before Bronze
# MAGIC - [ ] All 8 files readable from `RAW_PATH`
# MAGIC - [ ] Schemas and null patterns reviewed
# MAGIC - [ ] Transaction key cardinalities captured
# MAGIC - [ ] Ready for `02_bronze_ingestion`
