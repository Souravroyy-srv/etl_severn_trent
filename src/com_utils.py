# Fabric notebook source

import re
from functools import reduce

from pyspark.sql import DataFrame, functions as F


def run_dq_checks(
    spark,
    source_df: DataFrame,
    source_name: str,
    primary_keys: list,
    required_columns: list,
    date_order_rules: list = None,
    foreign_keys: list = None,
    dq_catalog: str = "severn_trent",
    dq_schema: str = "dq_check"
):
    """
    date_order_rules example:
    [("scheduled_start", "scheduled_end")]

    foreign_keys example:
    [
        {
            "child_column": "people_id",
            "reference_table": "severn_trent.silver.DimPeople",
            "reference_column": "people_id"
        }
    ]
    """

    date_order_rules = date_order_rules or []
    foreign_keys = foreign_keys or []

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dq_catalog}.{dq_schema}")

    dq_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", source_name).lower()
    failed_table = f"{dq_catalog}.{dq_schema}.{dq_table_name}_failed"
    log_table = f"{dq_catalog}.{dq_schema}.dq_run_log"

    rows_checked = source_df.count()
    source_with_id = source_df.withColumn(
        "_dq_row_id",
        F.monotonically_increasing_id()
    )

    failed_dataframes = []

    # Check required columns exist in the source DataFrame.
    missing_columns = [
        column
        for column in required_columns
        if column not in source_df.columns
    ]

    if missing_columns:
        failed_dataframes.append(
            source_with_id.withColumn(
                "dq_failure_reason",
                F.lit(
                    "Missing required source columns: "
                    + ", ".join(missing_columns)
                )
            )
        )

    # Check null values in primary/business keys.
    available_keys = [
        key for key in primary_keys
        if key in source_df.columns
    ]

    if available_keys:
        null_key_condition = reduce(
            lambda left, right: left | right,
            [F.col(key).isNull() for key in available_keys]
        )

        failed_dataframes.append(
            source_with_id
            .filter(null_key_condition)
            .withColumn(
                "dq_failure_reason",
                F.lit(
                    "Null value found in business key: "
                    + ", ".join(available_keys)
                )
            )
        )

        # Check duplicate business keys.
        duplicate_keys_df = (
            source_with_id
            .groupBy(*available_keys)
            .count()
            .filter(F.col("count") > 1)
            .drop("count")
        )

        failed_dataframes.append(
            source_with_id
            .join(duplicate_keys_df, available_keys, "inner")
            .withColumn(
                "dq_failure_reason",
                F.lit(
                    "Duplicate business key: "
                    + ", ".join(available_keys)
                )
            )
        )

    # Check null values in required columns.
    for column in required_columns:
        if column in source_df.columns:
            failed_dataframes.append(
                source_with_id
                .filter(F.col(column).isNull())
                .withColumn(
                    "dq_failure_reason",
                    F.lit(f"Null value found in required column: {column}")
                )
            )

    # Check that end datetime is not earlier than start datetime.
    for start_column, end_column in date_order_rules:
        if start_column in source_df.columns and end_column in source_df.columns:
            failed_dataframes.append(
                source_with_id
                .filter(F.col(end_column) < F.col(start_column))
                .withColumn(
                    "dq_failure_reason",
                    F.lit(
                        f"Invalid date range: {end_column} "
                        f"is earlier than {start_column}"
                    )
                )
            )

    # Check foreign-key values against Silver dimension tables.
    for fk in foreign_keys:
        child_column = fk["child_column"]
        reference_table = fk["reference_table"]
        reference_column = fk["reference_column"]

        if child_column not in source_df.columns:
            continue

        reference_df = spark.table(reference_table)

        # Uses only active SCD2 dimension records, if present.
        if "CurrentFlag" in reference_df.columns:
            reference_df = reference_df.filter(F.col("CurrentFlag") == "Y")

        valid_reference_keys = (
            reference_df
            .select(F.col(reference_column).alias("_reference_key"))
            .dropDuplicates()
        )
        # Identify records failing foreign key validation and append them to the DQ failure list
        failed_dataframes.append(
            source_with_id
            .filter(F.col(child_column).isNotNull())
            .join(
                valid_reference_keys,
                source_with_id[child_column]
                == valid_reference_keys["_reference_key"],
                "left_anti"
            )
            .withColumn(
                "dq_failure_reason",
                F.lit(
                    f"Invalid foreign key: {child_column} "
                    f"not found in {reference_table}"
                )
            )
        )

    # Combine all failed rows and remove duplicate rejected records.
    if failed_dataframes:
        failed_df = reduce(
            lambda left, right: left.unionByName(
                right,
                allowMissingColumns=True
            ),
            failed_dataframes
        )

        failed_row_ids = failed_df.select("_dq_row_id").dropDuplicates()
        failed_count = failed_row_ids.count()

        if failed_count > 0:
            (
                failed_df
                .dropDuplicates(["_dq_row_id", "dq_failure_reason"])
                .withColumn("dq_source_name", F.lit(source_name))
                .withColumn("dq_check_timestamp", F.current_timestamp())
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(failed_table)
            )

        valid_df = (
            source_with_id
            .join(failed_row_ids, "_dq_row_id", "left_anti")
            .drop("_dq_row_id")
        )
    else:
        failed_count = 0
        valid_df = source_with_id.drop("_dq_row_id")

    dq_status = "PASSED" if failed_count == 0 else "FAILED"

    dq_log_df = spark.createDataFrame(
        [(
            source_name,
            rows_checked,
            failed_count,
            dq_status,
            ", ".join(missing_columns)
        )],
        [
            "source_name",
            "rows_checked",
            "failed_rows",
            "dq_status",
            "missing_columns"
        ]
    ).withColumn(
        "dq_check_timestamp",
        F.current_timestamp()
    )

    (
        dq_log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(log_table)
    )

    print(
        f"DQ {dq_status}: {source_name} | "
        f"Rows checked: {rows_checked} | Failed rows: {failed_count}"
    )

    return valid_df, dq_status

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# Notebook: nb_scd_functions
# Purpose : Reusable SCD Type 1 and SCD Type 2 functions
# Layer   : Common Utility
# Usage   : Silver layer notebooks
# ============================================================

from typing import List, Optional
import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    coalesce,
    sha2,
    concat_ws,
    current_timestamp,
    to_timestamp
)

logger = logging.getLogger("scd_functions")
logger.setLevel(logging.INFO)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Function 1: SCD Type 1
# ============================================================
#
# Use this function when:
# - You do not need history
# - Existing rows should be updated with the latest values
# - New rows should be inserted
#
# Required parameters:
# spark        : Spark session
# source_df    : Source dataframe containing latest data
# target_table : Target silver table name
#                Example: "lh_silver.customer"
# join_keys    : Business key columns used to identify a record
#                Example: ["CustomerID"]
#
# Optional parameters:
# watermark_column : Column to exclude from change comparison
# full_refresh     : If True, overwrite the full target table
#
# Example:
# apply_scd1(
#     spark=spark,
#     source_df=df_customer,
#     target_table="lh_silver.customer",
#     join_keys=["CustomerID"],
#     watermark_column="last_update_ts",
#     full_refresh=False
# )
# ============================================================

def apply_scd1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    join_keys: List[str],
    watermark_column: Optional[str] = "last_update_ts",
    full_refresh: bool = False
):
    logger.info(f"[SCD1] Started for table: {target_table}")

    if not join_keys:
        raise ValueError("[SCD1] join_keys cannot be empty")

    # --------------------------------------------------------
    # Full refresh mode
    # This simply overwrites the target table.
    # --------------------------------------------------------
    if full_refresh or not spark.catalog.tableExists(target_table):
        logger.info(f"[SCD1] Full refresh / table creation for: {target_table}")

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        logger.info(f"[SCD1] Full refresh completed for: {target_table}")
        return

    # --------------------------------------------------------
    # Deduplicate source data based on business keys
    # This avoids duplicate source rows causing merge issues.
    # Null-safe handling is added for join keys.
    # --------------------------------------------------------
    dedup_columns = [
        coalesce(col(k), lit("__NULL_KEY__")).alias(f"_dedup_{k}")
        for k in join_keys
    ]

    dedup_key_names = [f"_dedup_{k}" for k in join_keys]

    source_df = (
        source_df
        .select("*", *dedup_columns)
        .dropDuplicates(dedup_key_names)
        .drop(*dedup_key_names)
    )

    logger.info(f"[SCD1] Source deduplicated on keys: {join_keys}")

    # --------------------------------------------------------
    # Build null-safe merge condition
    # Example:
    # target.CustomerID <=> source.CustomerID
    # --------------------------------------------------------
    merge_condition = " AND ".join(
        [f"target.`{k}` <=> source.`{k}`" for k in join_keys]
    )

    # --------------------------------------------------------
    # Compare only non-key columns.
    # We usually exclude watermark/audit columns from change check.
    # --------------------------------------------------------
    exclude_columns = set(join_keys)

    if watermark_column:
        exclude_columns.add(watermark_column)

    non_key_columns = [
        c for c in source_df.columns
        if c not in exclude_columns
    ]

    if non_key_columns:
        update_condition = " OR ".join(
            [f"NOT (target.`{c}` <=> source.`{c}`)" for c in non_key_columns]
        )
    else:
        update_condition = "1=1"

    update_set = {
        c: f"source.`{c}`"
        for c in source_df.columns
        if c not in join_keys
    }

    logger.info(f"[SCD1] Merge condition: {merge_condition}")
    logger.info(f"[SCD1] Update condition: {update_condition}")

    # --------------------------------------------------------
    # Perform SCD1 merge
    # Matched rows    -> update
    # Unmatched rows  -> insert
    # --------------------------------------------------------
    delta_table = DeltaTable.forName(spark, target_table)

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"[SCD1] Merge completed for: {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Function 2: SCD Type 2
# ============================================================
#
# Use this function when:
# - You need to keep history
# - Existing current record should be expired when data changes
# - New changed version should be inserted
#
# Required parameters:
# spark        : Spark session
# source_df    : Source dataframe containing latest data
# target_table : Target silver table name
#                Example: "lh_silver.customer"
# join_keys    : Business key columns used to identify a record
#                Example: ["CustomerID"]
# hash_columns : Columns used to detect change
#                Example: ["CustomerName", "City", "PhoneNumber"]
#
# Optional parameters:
# handle_deletes : If True, records missing from source will be marked deleted.
#                  Use True only when source_df is a FULL snapshot.
#                  Keep False for incremental loads.
#
# SCD2 columns added automatically:
# HashKey
# EffectiveFromDate
# EffectiveToDate
# UpdatedDate
# CurrentFlag
# DeletedFlag
#
# Example:
# apply_scd2(
#     spark=spark,
#     source_df=df_customer,
#     target_table="lh_silver.customer",
#     join_keys=["CustomerID"],
#     hash_columns=["CustomerName", "City", "PhoneNumber"],
#     handle_deletes=False
# )
# ============================================================

def apply_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    join_keys: List[str],
    hash_columns: List[str],
    handle_deletes: bool = False
):
    logger.info(f"[SCD2] Started for table: {target_table}")

    if not join_keys:
        raise ValueError("[SCD2] join_keys cannot be empty")

    if not hash_columns:
        raise ValueError("[SCD2] hash_columns cannot be empty")

    # --------------------------------------------------------
    # Create hash expression using hash columns.
    # This helps detect whether a record has changed.
    # --------------------------------------------------------
    hash_expr = sha2(
        concat_ws(
            "~",
            *[
                coalesce(col(c).cast("string"), lit("__NULL__"))
                for c in hash_columns
            ]
        ),
        256
    )

    # --------------------------------------------------------
    # Deduplicate source based on business keys.
    # --------------------------------------------------------
    dedup_columns = [
        coalesce(col(k), lit("__NULL_KEY__")).alias(f"_dedup_{k}")
        for k in join_keys
    ]

    dedup_key_names = [f"_dedup_{k}" for k in join_keys]

    source_df = (
        source_df
        .select("*", *dedup_columns)
        .withColumn("HashKey", hash_expr)
        .withColumn("EffectiveFromDate", current_timestamp())
        .withColumn("EffectiveToDate", to_timestamp(lit("2999-12-31 00:00:00")))
        .withColumn("UpdatedDate", current_timestamp())
        .withColumn("CurrentFlag", lit("Y"))
        .withColumn("DeletedFlag", lit("N"))
        .dropDuplicates(dedup_key_names)
        .drop(*dedup_key_names)
    )

    logger.info(f"[SCD2] Source deduplicated on keys: {join_keys}")
    logger.info(f"[SCD2] Hash columns used: {hash_columns}")

    # --------------------------------------------------------
    # If target table does not exist, create it first.
    # --------------------------------------------------------
    if not spark.catalog.tableExists(target_table):
        logger.info(f"[SCD2] Target table does not exist. Creating: {target_table}")

        (
            source_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        logger.info(f"[SCD2] Target table created: {target_table}")
        return

    delta_table = DeltaTable.forName(spark, target_table)

    # --------------------------------------------------------
    # Step 1:
    # Expire current records where hash has changed.
    # --------------------------------------------------------
    merge_condition = " AND ".join(
        [f"target.`{k}` <=> source.`{k}`" for k in join_keys]
    )

    logger.info("[SCD2] Expiring changed records")

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(
            condition="""
                target.CurrentFlag = 'Y'
                AND target.DeletedFlag = 'N'
                AND source.HashKey <> target.HashKey
            """,
            set={
                "CurrentFlag": "'N'",
                "EffectiveToDate": "current_timestamp()",
                "UpdatedDate": "current_timestamp()"
            }
        )
        .execute()
    )

    logger.info("[SCD2] Expiry step completed")

    # --------------------------------------------------------
    # Step 2:
    # Insert new records and changed records.
    #
    # After expiry, changed records no longer have a current match,
    # so they will be inserted as new current records.
    # --------------------------------------------------------
    logger.info("[SCD2] Finding new or changed records to insert")

    current_df = (
        delta_table.toDF()
        .filter("CurrentFlag = 'Y' AND DeletedFlag = 'N'")
        .withColumn("_matched_record", lit(1))
    )

    join_condition = None

    for key in join_keys:
        condition = col(f"s.`{key}`").eqNullSafe(col(f"c.`{key}`"))

        if join_condition is None:
            join_condition = condition
        else:
            join_condition = join_condition & condition

    new_or_changed_df = (
        source_df.alias("s")
        .join(current_df.alias("c"), join_condition, "left")
        .filter(col("c._matched_record").isNull())
        .select("s.*")
    )

    new_or_changed_count = new_or_changed_df.count()

    if new_or_changed_count > 0:
        (
            new_or_changed_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )

        logger.info(f"[SCD2] Inserted rows: {new_or_changed_count}")
    else:
        logger.info("[SCD2] No new or changed rows to insert")

    # --------------------------------------------------------
    # Step 3:
    # Optional deletion handling.
    #
    # Important:
    # Use handle_deletes=True only when source_df is a FULL snapshot.
    # Do not use this for incremental source data, otherwise valid
    # records may be marked as deleted incorrectly.
    # --------------------------------------------------------
    if handle_deletes:
        logger.info("[SCD2] Checking deleted records")

        source_keys_df = source_df.select(*join_keys).dropDuplicates()

        delete_join_condition = None

        for key in join_keys:
            condition = col(f"target.`{key}`").eqNullSafe(col(f"source.`{key}`"))

            if delete_join_condition is None:
                delete_join_condition = condition
            else:
                delete_join_condition = delete_join_condition & condition

        deleted_df = (
            delta_table.toDF()
            .filter("CurrentFlag = 'Y' AND DeletedFlag = 'N'")
            .alias("target")
            .join(source_keys_df.alias("source"), delete_join_condition, "left_anti")
            .select("target.*")
        )

        deleted_count = deleted_df.count()

        if deleted_count > 0:
            delete_merge_condition = " AND ".join(
                [f"target.`{k}` <=> deleted.`{k}`" for k in join_keys]
            )

            (
                delta_table.alias("target")
                .merge(
                    deleted_df.alias("deleted"),
                    f"{delete_merge_condition} AND target.CurrentFlag = 'Y'"
                )
                .whenMatchedUpdate(
                    set={
                        "CurrentFlag": "'N'",
                        "DeletedFlag": "'Y'",
                        "EffectiveToDate": "current_timestamp()",
                        "UpdatedDate": "current_timestamp()"
                    }
                )
                .execute()
            )

            logger.info(f"[SCD2] Marked deleted rows: {deleted_count}")
        else:
            logger.info("[SCD2] No deleted records found")

    logger.info(f"[SCD2] Completed for table: {target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Examples ->
# 
# 
# ```
# apply_scd1(
#     spark=spark,
#     source_df=bronze_df,
#     target_table="lh_silver.sales_customer_categories",
#     join_keys=["CustomerCategoryID"],
#     watermark_column="last_update_ts",
#     full_refresh=False
# )
# ```

# MARKDOWN ********************

# ```
# apply_scd2(
#     spark=spark,
#     source_df=bronze_df,
#     target_table="lh_silver.sales_customers",
#     join_keys=["CustomerID"],
#     hash_columns=[
#         "CustomerName",
#         "CustomerCategoryID",
#         "BuyingGroupID",
#         "DeliveryCityID"
#     ],
#     handle_deletes=False
# )
# ```

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Full Explanations

# MARKDOWN ********************

# # Silver Layer SCD Utility Notebook Explanation
# 
# ## 1. What are we building?
# 
# In this project, we are creating a **common utility notebook** for the Silver layer.
# 
# The notebook will provide two reusable functions:
# 
# 1. `apply_scd1()`
# 2. `apply_scd2()`
# 
# These functions help us load data from **Bronze** to **Silver** using Slowly Changing Dimension logic.
# 
# Instead of writing SCD logic again and again in every Silver notebook, we keep the logic in one common place and reuse it for different tables.
# 
# ---
# 
# ## 2. Why do we need a common SCD utility notebook?
# 
# In a real data engineering project, many tables need similar loading logic.
# 
# For example:
# 
# - Customer table
# - Product table
# - Supplier table
# - Buying group table
# - Customer category table
# - Salesperson table
# 
# For each table, we may need to:
# 
# - Insert new records
# - Update existing records
# - Track history for changed records
# - Keep only latest records
# - Add audit columns
# - Handle deleted records
# 
# If we write this logic separately in every notebook, then:
# 
# - Code becomes duplicated
# - Maintenance becomes difficult
# - Bugs become harder to fix
# - Changes need to be done in many places
# - Beginners may find the project harder to understand
# 
# So we create one common notebook:
# 
# ```python
# nb_scd_functions
# ```
# 
# This notebook contains reusable functions that can be called from any Silver notebook.
# 
# ---
# 
# ## 3. Where does this notebook fit in the project?
# 
# In the Fabric medallion architecture, we usually have:
# 
# ```text
# Raw Layer      -> Landing data as-is from source
# Bronze Layer   -> Clean technical copy of source data
# Silver Layer   -> Cleaned, conformed, historical business-ready data
# Gold Layer     -> Reporting and analytics-ready data
# ```
# 
# The SCD utility notebook is used mainly when loading data into the **Silver layer**.
# 
# Example flow:
# 
# ```text
# lh_raw
#    ↓
# lh_bronze
#    ↓
# lh_silver
#    ↓
# lh_gold
# ```
# 
# The SCD functions help with this step:
# 
# ```text
# Bronze table/dataframe  ->  Silver table
# ```
# 
# ---
# 
# # 4. What is SCD?
# 
# SCD stands for **Slowly Changing Dimension**.
# 
# It is a common data warehousing concept used to manage changes in dimension data over time.
# 
# Dimension data usually means descriptive business data, such as:
# 
# - Customer
# - Product
# - Employee
# - Supplier
# - Cost center
# - Project
# - Department
# 
# These records do not change every second, but they may change slowly over time.
# 
# Example:
# 
# A customer changes city:
# 
# ```text
# CustomerID: 101
# CustomerName: John Smith
# City: London
# ```
# 
# Later, the city changes to Manchester:
# 
# ```text
# CustomerID: 101
# CustomerName: John Smith
# City: Manchester
# ```
# 
# Now we need to decide:
# 
# Do we overwrite London with Manchester?
# 
# Or do we keep both versions so that we know the customer was earlier in London and later in Manchester?
# 
# This is where SCD comes in.
# 
# ---
# 
# # 5. What is SCD Type 1?
# 
# SCD Type 1 means:
# 
# > Keep only the latest version of the record.
# 
# In SCD1, we do not keep history.
# 
# If a record changes, we simply update the existing row.
# 
# ## Example
# 
# Original Silver table:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | London |
# 
# New source data:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | Manchester |
# 
# After SCD1 load:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | Manchester |
# 
# The old value, `London`, is overwritten.
# 
# ## When should we use SCD1?
# 
# Use SCD1 when history is not required.
# 
# Good examples:
# 
# - Product description correction
# - Customer category name correction
# - Reference data
# - Small lookup tables
# - Data where only the latest value matters
# 
# ## What does our `apply_scd1()` function do?
# 
# The function does three main things:
# 
# 1. If the target table does not exist, it creates the table.
# 2. If the record already exists, it updates the changed columns.
# 3. If the record does not exist, it inserts the new record.
# 
# ---
# 
# # 6. What is SCD Type 2?
# 
# SCD Type 2 means:
# 
# > Keep full history of changes.
# 
# In SCD2, we do not overwrite the old record.  
# Instead, we expire the old record and insert a new current record.
# 
# ## Example
# 
# Original Silver table:
# 
# | CustomerID | CustomerName | City | CurrentFlag |
# |---|---|---|---|
# | 101 | John Smith | London | Y |
# 
# New source data:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | Manchester |
# 
# After SCD2 load:
# 
# | CustomerID | CustomerName | City | CurrentFlag |
# |---|---|---|---|
# | 101 | John Smith | London | N |
# | 101 | John Smith | Manchester | Y |
# 
# Now we can see the full history.
# 
# ## When should we use SCD2?
# 
# Use SCD2 when business users need to know how data changed over time.
# 
# Good examples:
# 
# - Customer address history
# - Product category history
# - Employee department history
# - Cost center hierarchy changes
# - Project ownership changes
# 
# ---
# 
# # 7. Why do we need business keys?
# 
# A business key is the column or group of columns that uniquely identifies a business record.
# 
# Example:
# 
# For customer:
# 
# ```python
# join_keys = ["CustomerID"]
# ```
# 
# For product:
# 
# ```python
# join_keys = ["StockItemID"]
# ```
# 
# For a table where one key is not enough:
# 
# ```python
# join_keys = ["CustomerID", "ValidFromDate"]
# ```
# 
# The SCD function uses these keys to check whether the incoming source record already exists in the target table.
# 
# ## Example
# 
# Source record:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | London |
# 
# Target table:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | Manchester |
# 
# The function compares using:
# 
# ```python
# CustomerID
# ```
# 
# Since `CustomerID = 101` exists in the target table, the function knows this is an existing customer.
# 
# ---
# 
# # 8. Why do we use null-safe joins?
# 
# In Spark, normal equality does not always behave correctly when null values are involved.
# 
# For example:
# 
# ```sql
# NULL = NULL
# ```
# 
# does not return true in normal SQL comparison.
# 
# But in data engineering, sometimes business key columns may contain nulls.  
# To make the merge safer, we use null-safe equality:
# 
# ```python
# target.`CustomerID` <=> source.`CustomerID`
# ```
# 
# The `<=>` operator means:
# 
# - If both values are equal, match them
# - If both values are null, also treat them as equal
# 
# This makes our merge logic more reliable.
# 
# ---
# 
# # 9. Why do we deduplicate the source data?
# 
# Before performing a merge, we deduplicate the source data using the business keys.
# 
# This is important because Delta merge can fail or behave incorrectly if multiple source rows match the same target row.
# 
# ## Example
# 
# Source data has duplicate records:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | London |
# | 101 | John Smith | London |
# 
# If we try to merge both records into the same target row, Spark may not know which source row to use.
# 
# So before merging, we do:
# 
# ```python
# .dropDuplicates(dedup_key_names)
# ```
# 
# This keeps one record per business key.
# 
# ---
# 
# # 10. What is a hash column in SCD2?
# 
# In SCD2, we need to know whether a record has changed.
# 
# Instead of comparing every column manually, we create a hash value using the important columns.
# 
# Example columns:
# 
# ```python
# hash_columns = [
#     "CustomerName",
#     "City",
#     "PhoneNumber"
# ]
# ```
# 
# The function combines these columns and creates one hash value:
# 
# ```python
# HashKey
# ```
# 
# If any value changes, the hash value changes.
# 
# ## Example
# 
# Old record:
# 
# ```text
# CustomerName = John Smith
# City = London
# PhoneNumber = 12345
# ```
# 
# New record:
# 
# ```text
# CustomerName = John Smith
# City = Manchester
# PhoneNumber = 12345
# ```
# 
# Because `City` changed, the `HashKey` will also change.
# 
# This tells the function that a new SCD2 version needs to be inserted.
# 
# ---
# 
# # 11. What are SCD2 metadata columns?
# 
# Our SCD2 function adds these columns automatically:
# 
# | Column | Meaning |
# |---|---|
# | HashKey | Used to detect whether the record changed |
# | EffectiveFromDate | Date/time from when this version became active |
# | EffectiveToDate | Date/time until this version was active |
# | UpdatedDate | Date/time when this row was updated |
# | CurrentFlag | Shows whether this is the current active record |
# | DeletedFlag | Shows whether the record was deleted from source |
# 
# ---
# 
# ## 11.1 HashKey
# 
# `HashKey` is used to compare source and target records.
# 
# If the source hash is different from the target hash, it means the business data has changed.
# 
# ---
# 
# ## 11.2 EffectiveFromDate
# 
# This shows when the record version became active.
# 
# Example:
# 
# ```text
# 2026-07-10 10:30:00
# ```
# 
# ---
# 
# ## 11.3 EffectiveToDate
# 
# This shows when the record version stopped being active.
# 
# For current active records, we use a future date:
# 
# ```text
# 2999-12-31 00:00:00
# ```
# 
# This means the record is still active.
# 
# When the record changes, the old row gets expired by setting:
# 
# ```text
# EffectiveToDate = current timestamp
# ```
# 
# ---
# 
# ## 11.4 CurrentFlag
# 
# This tells us whether the record is the latest version.
# 
# | CurrentFlag | Meaning |
# |---|---|
# | Y | Current active record |
# | N | Old historical record |
# 
# ---
# 
# ## 11.5 DeletedFlag
# 
# This tells us whether the record was deleted from the source system.
# 
# | DeletedFlag | Meaning |
# |---|---|
# | N | Not deleted |
# | Y | Deleted from source |
# 
# ---
# 
# # 12. How does SCD1 work step by step?
# 
# The `apply_scd1()` function follows this logic:
# 
# ## Step 1: Validate input
# 
# The function checks that `join_keys` are provided.
# 
# ```python
# if not join_keys:
#     raise ValueError("[SCD1] join_keys cannot be empty")
# ```
# 
# Without join keys, the function cannot identify existing records.
# 
# ---
# 
# ## Step 2: Create table if needed
# 
# If the target table does not exist, the function creates it.
# 
# ```python
# if full_refresh or not spark.catalog.tableExists(target_table):
# ```
# 
# This is useful for the first run.
# 
# ---
# 
# ## Step 3: Deduplicate source data
# 
# The function removes duplicate records based on the business keys.
# 
# ```python
# .dropDuplicates(dedup_key_names)
# ```
# 
# This prevents merge conflicts.
# 
# ---
# 
# ## Step 4: Build merge condition
# 
# The function creates a merge condition using the business keys.
# 
# Example:
# 
# ```sql
# target.CustomerID <=> source.CustomerID
# ```
# 
# This tells Spark how to match source rows with target rows.
# 
# ---
# 
# ## Step 5: Build update condition
# 
# The function checks whether any non-key column has changed.
# 
# Example:
# 
# ```sql
# NOT (target.CustomerName <=> source.CustomerName)
# OR NOT (target.City <=> source.City)
# ```
# 
# This means the row is updated only if something has actually changed.
# 
# ---
# 
# ## Step 6: Merge into target table
# 
# The function performs the Delta merge:
# 
# ```python
# .whenMatchedUpdate(...)
# .whenNotMatchedInsertAll()
# ```
# 
# This means:
# 
# | Scenario | Action |
# |---|---|
# | Matching record found and values changed | Update existing row |
# | No matching record found | Insert new row |
# 
# ---
# 
# # 13. How does SCD2 work step by step?
# 
# The `apply_scd2()` function follows this logic:
# 
# ## Step 1: Validate input
# 
# The function checks that both `join_keys` and `hash_columns` are provided.
# 
# ```python
# if not join_keys:
#     raise ValueError("[SCD2] join_keys cannot be empty")
# 
# if not hash_columns:
#     raise ValueError("[SCD2] hash_columns cannot be empty")
# ```
# 
# ---
# 
# ## Step 2: Create hash value
# 
# The function creates a hash using selected business columns.
# 
# ```python
# HashKey
# ```
# 
# This helps detect whether the incoming record has changed.
# 
# ---
# 
# ## Step 3: Add metadata columns
# 
# The function adds SCD2 columns:
# 
# ```python
# HashKey
# EffectiveFromDate
# EffectiveToDate
# UpdatedDate
# CurrentFlag
# DeletedFlag
# ```
# 
# These columns help track history.
# 
# ---
# 
# ## Step 4: Create table if needed
# 
# If this is the first load and the target table does not exist, the function creates it.
# 
# All incoming records become current records:
# 
# ```text
# CurrentFlag = Y
# DeletedFlag = N
# ```
# 
# ---
# 
# ## Step 5: Expire changed records
# 
# The function checks the current records in the target table.
# 
# If the same business key exists but the hash is different, then the existing record is expired.
# 
# Old record changes from:
# 
# ```text
# CurrentFlag = Y
# EffectiveToDate = 2999-12-31
# ```
# 
# to:
# 
# ```text
# CurrentFlag = N
# EffectiveToDate = current timestamp
# ```
# 
# ---
# 
# ## Step 6: Insert new or changed records
# 
# After old changed records are expired, the function inserts the new version as the current record.
# 
# New version gets:
# 
# ```text
# CurrentFlag = Y
# DeletedFlag = N
# EffectiveToDate = 2999-12-31
# ```
# 
# ---
# 
# ## Step 7: Optional delete handling
# 
# If `handle_deletes=True`, the function checks whether any current target records are missing from the source.
# 
# If a target record is missing from source, it marks that record as deleted.
# 
# ```text
# CurrentFlag = N
# DeletedFlag = Y
# EffectiveToDate = current timestamp
# ```
# 
# Important:
# 
# Use `handle_deletes=True` only when the source dataframe contains a **full snapshot**.
# 
# Do not use delete handling for incremental loads.
# 
# ---
# 
# # 14. Why should delete handling not be used for incremental loads?
# 
# This is very important.
# 
# Suppose the source sends only changed records today.
# 
# Example source data today:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | London |
# 
# But the target table has many customers:
# 
# | CustomerID | CustomerName | City |
# |---|---|---|
# | 101 | John Smith | London |
# | 102 | Mary Jones | Oxford |
# | 103 | Alex Brown | Bristol |
# 
# If we use delete handling, the function may think customers 102 and 103 are deleted because they are missing from today's incremental file.
# 
# But they are not deleted.  
# They simply did not change today.
# 
# So:
# 
# | Load type | handle_deletes value |
# |---|---|
# | Full snapshot | `True` allowed |
# | Incremental load | `False` recommended |
# 
# ---
# 
# # 15. Parameters for SCD1
# 
# To use SCD1, provide these parameters:
# 
# ```python
# apply_scd1(
#     spark=spark,
#     source_df=source_df,
#     target_table="lh_silver.table_name",
#     join_keys=["BusinessKeyColumn"],
#     watermark_column="last_update_ts",
#     full_refresh=False
# )
# ```
# 
# ## Parameter explanation
# 
# | Parameter | Required | Meaning |
# |---|---|---|
# | spark | Yes | Spark session |
# | source_df | Yes | Incoming dataframe from Bronze |
# | target_table | Yes | Silver table name |
# | join_keys | Yes | Columns used to match source and target |
# | watermark_column | No | Audit column excluded from comparison |
# | full_refresh | No | If True, overwrite the full table |
# 
# ---
# 
# # 16. Parameters for SCD2
# 
# To use SCD2, provide these parameters:
# 
# ```python
# apply_scd2(
#     spark=spark,
#     source_df=source_df,
#     target_table="lh_silver.table_name",
#     join_keys=["BusinessKeyColumn"],
#     hash_columns=["Column1", "Column2", "Column3"],
#     handle_deletes=False
# )
# ```
# 
# ## Parameter explanation
# 
# | Parameter | Required | Meaning |
# |---|---|---|
# | spark | Yes | Spark session |
# | source_df | Yes | Incoming dataframe from Bronze |
# | target_table | Yes | Silver table name |
# | join_keys | Yes | Columns used to identify the record |
# | hash_columns | Yes | Columns used to detect changes |
# | handle_deletes | No | Marks missing records as deleted when source is full snapshot |
# 
# ---
# 
# # 17. Example: SCD1 for customer category
# 
# Customer category is usually simple reference data.
# 
# We may not need to keep full history.
# 
# So we can use SCD1.
# 
# ```python
# apply_scd1(
#     spark=spark,
#     source_df=customer_category_df,
#     target_table="lh_silver.sales_customer_categories",
#     join_keys=["CustomerCategoryID"],
#     watermark_column="last_update_ts",
#     full_refresh=False
# )
# ```
# 
# This means:
# 
# - Match records using `CustomerCategoryID`
# - Update changed records
# - Insert new records
# - Do not keep old history
# 
# ---
# 
# # 18. Example: SCD2 for customer
# 
# Customer data may change over time.
# 
# For example:
# 
# - Customer name
# - Category
# - Buying group
# - City
# - Credit limit
# 
# If business wants to track history, use SCD2.
# 
# ```python
# apply_scd2(
#     spark=spark,
#     source_df=customer_df,
#     target_table="lh_silver.sales_customers",
#     join_keys=["CustomerID"],
#     hash_columns=[
#         "CustomerName",
#         "CustomerCategoryID",
#         "BuyingGroupID",
#         "DeliveryCityID",
#         "CreditLimit"
#     ],
#     handle_deletes=False
# )
# ```
# 
# This means:
# 
# - Match records using `CustomerID`
# - Detect changes using the selected hash columns
# - Expire old records when values change
# - Insert new current versions
# - Do not mark deletes because this is likely incremental
# 
# ---
# 
# # 19. How to call the common utility notebook
# 
# In your Silver notebook, first call the common notebook:
# 
# ```python
# %run ../05_common_utilities/nb_scd_functions
# ```
# 
# Then prepare your source dataframe:
# 
# ```python
# source_df = spark.table("lh_bronze.sales_customers")
# ```
# 
# Then call SCD1 or SCD2 depending on your table requirement:
# 
# ```python
# apply_scd2(
#     spark=spark,
#     source_df=source_df,
#     target_table="lh_silver.sales_customers",
#     join_keys=["CustomerID"],
#     hash_columns=[
#         "CustomerName",
#         "CustomerCategoryID",
#         "BuyingGroupID",
#         "DeliveryCityID"
#     ],
#     handle_deletes=False
# )
# ```
# 
# ---
# 
# # 20. How to decide between SCD1 and SCD2
# 
# Use this simple rule:
# 
# | Requirement | Use |
# |---|---|
# | Only latest value is needed | SCD1 |
# | History is required | SCD2 |
# | Correction of wrong value | SCD1 |
# | Business wants to see old and new values | SCD2 |
# | Small lookup/reference table | Usually SCD1 |
# | Important business dimension | Usually SCD2 |
# 
# ---
# 
# # 21. Recommended beginner approach
# 
# For beginners, start with this approach:
# 
# ## Use SCD1 for simple lookup tables
# 
# Examples:
# 
# - Buying groups
# - Customer categories
# - Package types
# - Colors
# - Delivery methods
# 
# ## Use SCD2 for important business entities
# 
# Examples:
# 
# - Customers
# - Suppliers
# - Products
# - Employees
# - Cost centers
# - Projects
# 
# ## Keep delete handling off initially
# 
# Start with:
# 
# ```python
# handle_deletes=False
# ```
# 
# Only turn it on later when everyone understands the difference between full snapshot and incremental load.
# 
# ---
# 
# # 22. Simple mental model
# 
# Think of SCD1 like editing a row in Excel.
# 
# Old value is replaced by new value.
# 
# ```text
# London -> Manchester
# ```
# 
# Only Manchester remains.
# 
# Think of SCD2 like keeping a version history.
# 
# ```text
# Version 1: London      CurrentFlag = N
# Version 2: Manchester  CurrentFlag = Y
# ```
# 
# Both old and new values remain.
# 
# ---
# 
# # 23. Final summary
# 
# We created a common SCD utility notebook so that Silver layer notebooks can reuse standard loading logic.
# 
# The notebook provides:
# 
# | Function | Purpose |
# |---|---|
# | apply_scd1 | Updates existing records and inserts new records without history |
# | apply_scd2 | Maintains history by expiring old records and inserting new versions |
# 
# This approach makes the project:
# 
# - Easier to understand
# - Easier to maintain
# - More reusable
# - More scalable
# - Better aligned with medallion architecture
# - Better for real enterprise data engineering projects
# 
# For the training project, the recommended flow is:
# 
# ```text
# Bronze dataframe
#    ↓
# Call apply_scd1() or apply_scd2()
#    ↓
# Silver Delta table
# ```
# 
# This keeps the Silver notebooks simple and moves reusable logic into the common utility layer.


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }