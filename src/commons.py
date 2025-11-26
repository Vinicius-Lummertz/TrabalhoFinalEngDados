from datetime import datetime, timezone
from typing import Dict, List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

from vars import (
    CATALOG,
    LANDING_BASE_PATH,
    BRONZE_SCHEMA,
    META_SCHEMA,
    ORIGEM_SISTEMA,
    JDBC_URL,
    JDBC_USER,
    JDBC_PASSWORD,
    JDBC_DRIVER,
)


def schema_fq(schema: str) -> str:
    if CATALOG:
        return f"{CATALOG}.{schema}"
    return schema


def qname(schema: str, table: str) -> str:
    if table:
        return f"{schema_fq(schema)}.{table}"
    return schema_fq(schema)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def init_watermark_table() -> None:
    wm_schema = schema_fq(META_SCHEMA)
    wm_table = qname(META_SCHEMA, "watermark_incremental")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {wm_schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {wm_table} (
            tabela STRING NOT NULL,
            ultima_data_ref TIMESTAMP NOT NULL,
            ultima_execucao_ts TIMESTAMP NOT NULL
        )
        USING DELTA
        """
    )


def get_watermark(table_name: str) -> datetime:
    wm_table = qname(META_SCHEMA, "watermark_incremental")
    if not spark.catalog.tableExists(wm_table):
        return datetime(1900, 1, 1, tzinfo=timezone.utc)
    df = spark.table(wm_table).filter(F.col("tabela") == table_name)
    if df.rdd.isEmpty():
        return datetime(1900, 1, 1, tzinfo=timezone.utc)
    row = df.select("ultima_data_ref").head(1)[0]
    return row["ultima_data_ref"].replace(tzinfo=timezone.utc)


def update_watermark(table_name: str, new_data_ref: datetime) -> None:
    wm_table = qname(META_SCHEMA, "watermark_incremental")
    ts_exec = now_utc()
    if spark.catalog.tableExists(wm_table):
        df_others = spark.table(wm_table).filter(F.col("tabela") != table_name)
    else:
        df_others = spark.createDataFrame(
            [],
            "tabela STRING, ultima_data_ref TIMESTAMP, ultima_execucao_ts TIMESTAMP",
        )
    new_row = spark.createDataFrame(
        [(table_name, new_data_ref, ts_exec)],
        ["tabela", "ultima_data_ref", "ultima_execucao_ts"],
    )
    merged = df_others.unionByName(new_row, allowMissingColumns=True)
    (
        merged.write.mode("overwrite")
        .format("delta")
        .saveAsTable(wm_table)
    )


def build_incremental_query(pg_schema: str, table_name: str, last_wm: datetime) -> str:
    last_wm_str = last_wm.strftime("%Y-%m-%d %H:%M:%S")
    return f"""
        SELECT
            t.*,
            COALESCE(t.AUD_DH_ALTERACAO, t.AUD_DH_CRIACAO) AS data_ref,
            CASE
                WHEN t.AUD_DH_ALTERACAO IS NULL THEN 'I'
                ELSE 'U'
            END AS change_op
        FROM {pg_schema}.{table_name} t
        WHERE COALESCE(t.AUD_DH_ALTERACAO, t.AUD_DH_CRIACAO) > TIMESTAMP '{last_wm_str}'
        ORDER BY data_ref, id
    """


def extract_incremental_from_postgres(
    table_name: str,
    conf: Dict,
    last_wm: datetime,
) -> DataFrame:
    pg_schema = conf["schema"]
    query = build_incremental_query(pg_schema, table_name, last_wm)
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("dbtable", f"({query}) AS src")
        .load()
    )
    return df


def write_landing(df: DataFrame, table_name: str, batch_id: str) -> str:
    landing_path = f"{LANDING_BASE_PATH}/{table_name}/batch_id={batch_id}"
    (
        df.write.mode("append")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(landing_path)
    )
    return landing_path


def ensure_bronze_changelog_table(table_name: str, df_sample: DataFrame) -> None:
    bronze_schema = schema_fq(BRONZE_SCHEMA)
    bronze_table = qname(BRONZE_SCHEMA, f"{table_name}_changelog")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    if not spark.catalog.tableExists(bronze_table):
        df_create = (
            df_sample
            .withColumn("load_ts", F.lit(now_utc()))
            .withColumn("batch_id", F.lit("init"))
            .withColumn("origem_sistema", F.lit(ORIGEM_SISTEMA))
        ).limit(0)
        (
            df_create.write.mode("overwrite")
            .format("delta")
            .saveAsTable(bronze_table)
        )


def merge_into_bronze_changelog(
    df: DataFrame,
    table_name: str,
    business_key_cols: List[str],
    batch_id: str,
) -> None:
    if df.rdd.isEmpty():
        return
    bronze_table = qname(BRONZE_SCHEMA, f"{table_name}_changelog")
    ensure_bronze_changelog_table(table_name, df)
    df_enriched = (
        df.withColumn("load_ts", F.lit(now_utc()))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("origem_sistema", F.lit(ORIGEM_SISTEMA))
    )
    cond_parts = [f"tgt.{col} = src.{col}" for col in business_key_cols]
    cond_parts.append("tgt.data_ref = src.data_ref")
    merge_condition = " AND ".join(cond_parts)
    delta_tbl = DeltaTable.forName(spark, bronze_table)
    (
        delta_tbl.alias("tgt")
        .merge(df_enriched.alias("src"), merge_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
