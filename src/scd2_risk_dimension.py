"""
📊 SCD Type 2 (Slowly Changing Dimension) — Dimensão de Risco Financeiro
======================================================================

✅ O que este job faz (visão leiga e objetiva):
- Lê uma tabela "snapshot" com o estado mais recente das contas (fonte operacional).
- Para cada grupo de conta (account_group_id), pega o registro mais recente (pelo ingested_at).
- Compara esse estado com o registro "ativo" atual na dimensão SCD2 (is_current = true).
- Se mudou o risco (ou qualquer atributo que você decidir versionar), ele:
  1) Encerra a versão anterior (valid_to = data_atual - 1; is_current = false)
  2) Insere uma nova versão (valid_from = data_atual; valid_to = 9999-12-31; is_current = true)
- Se não mudou, não faz nada.
- É idempotente: se você rodar de novo no mesmo dia, não duplica versões.

📌 Por que isso é SCD2 de verdade?
- Porque ele NÃO sobrescreve o passado.
- Ele mantém cada versão com vigência (valid_from/valid_to) + flag de corrente (is_current).

⚠️ Observação:
- Aqui usamos "data" (current_date) como referência de vigência.
  Se você quiser granularidade por timestamp (mais preciso), dá para ajustar para current_timestamp.

Tabelas (fictícias, portfólio):
- Fonte:   financas.ops_finance.ar_open_items
- Destino: financas.ops_finance.dim_ar_risk_scd2
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ============================================================
# 0) Spark Session + configurações
# ============================================================
spark = SparkSession.builder.appName("scd2_financial_risk_dimension").getOrCreate()

# Desliga ANSI para evitar que algumas conversões invalidem o job (trade-off: menos rigor).
spark.conf.set("spark.sql.ansi.enabled", "false")

# ============================================================
# 1) Contexto do Databricks (catalog/schema) e tabelas
# ============================================================
# Parâmetros de execução (quando rodar como Job/Workflow no Databricks)
run_params = dbutils.notebook.entry_point.getCurrentBindings()

# Catálogo padrão do portfólio (se não vier via job param)
catalog = run_params.get("catalog") or "financas"
schema = "ops_finance"

# Tabela fonte (snapshot operacional) e tabela destino (dimensão SCD2)
source_table = f"{catalog}.{schema}.ar_open_items"
target_table = f"{catalog}.{schema}.dim_ar_risk_scd2"

# Ajusta o contexto do Spark no Unity Catalog
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

print(f"[INFO] source_table: {source_table}")
print(f"[INFO] target_table: {target_table}")

# ============================================================
# 2) Leitura da fonte e seleção do registro mais recente por chave
# ============================================================
df_src = spark.table(source_table)

# Data de vigência do run (SCD2 por dia)
run_date = F.current_date()

# Para cada account_group_id, pegamos o registro mais recente pelo ingested_at
w_latest = Window.partitionBy("account_group_id").orderBy(F.col("ingested_at").desc())

df_latest = (
    df_src
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# ============================================================
# 3) Change Detection: hash determinístico dos atributos versionados
# ============================================================
# Aqui estamos versionando apenas o delinquency_risk_level.
# Se quiser versionar mais colunas, inclua no concat_ws("||", ...)
df_latest = df_latest.withColumn(
    "scd_hash",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("delinquency_risk_level").cast("string"), F.lit(""))
        ),
        256
    )
)

# Campos padrão SCD2
infinite_date = F.lit("9999-12-31").cast("date")

df_incoming = (
    df_latest
    .withColumn("valid_from", run_date)
    .withColumn("valid_to", infinite_date)
    .withColumn("is_current", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("updated_at", F.current_timestamp())
)

# ============================================================
# 4) Checar se a tabela destino existe
# ============================================================
tables = spark.catalog.listTables(schema)
target_exists = any(t.name == "dim_ar_risk_scd2" for t in tables)

# ============================================================
# 5) Se não existir, fazemos carga inicial (primeira versão)
# ============================================================
if not target_exists:
    (
        df_incoming
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print("[OK] Tabela SCD2 criada e carga inicial concluída.")
else:
    # ============================================================
    # 6) SCD2 Incremental: fechar versão atual + inserir nova versão
    # ============================================================
    delta = DeltaTable.forName(spark, target_table)

    # Recupera somente registros atuais (ativos) da dimensão
    df_current = (
        spark.table(target_table)
        .filter(F.col("is_current") == True)
        .select("account_group_id", "scd_hash", "valid_from")
        .withColumnRenamed("scd_hash", "curr_hash")
        .withColumnRenamed("valid_from", "curr_valid_from")
    )

    # Compara o hash do "incoming" com o hash do registro atual
    # Classifica:
    # - NEW: chave ainda não existe na dimensão
    # - CHANGED: chave existe, mas hash é diferente (houve mudança)
    # - NO_CHANGE: nada mudou
    df_changes = (
        df_incoming.alias("s")
        .join(df_current.alias("t"), on="account_group_id", how="left")
        .withColumn(
            "change_type",
            F.when(F.col("t.curr_hash").isNull(), F.lit("NEW"))
             .when(F.col("s.scd_hash") != F.col("t.curr_hash"), F.lit("CHANGED"))
             .otherwise(F.lit("NO_CHANGE"))
        )
    )

    # Mantém só o que realmente precisa alterar/inserir
    df_to_upsert = df_changes.filter(F.col("change_type").isin("NEW", "CHANGED"))

    # ============================================================
    # 6A) Fechar registros atuais (somente para CHANGED)
    # ============================================================
    # Encerramos a vigência no dia anterior ao run_date
    df_to_close = (
        df_to_upsert
        .filter(F.col("change_type") == "CHANGED")
        .select("account_group_id")
        .distinct()
        .withColumn("close_to", F.date_sub(run_date, 1))
        .withColumn("closed_at", F.current_timestamp())
    )

    # Merge para fechar a versão atual (is_current=true)
    # Só fecha se encontrar o registro ativo daquela chave
    delta.alias("t").merge(
        df_to_close.alias("s"),
        "t.account_group_id = s.account_group_id AND t.is_current = true"
    ).whenMatchedUpdate(set={
        "valid_to": "s.close_to",
        "is_current": "false",
        "updated_at": "s.closed_at"
    }).execute()

    # ============================================================
    # 6B) Inserir nova versão para NEW e CHANGED (idempotente)
    # ============================================================
    # Evita duplicar se rodar de novo no mesmo dia:
    # - verifica se já existe versão atual com valid_from = run_date
    df_loaded_today = (
        spark.table(target_table)
        .filter((F.col("is_current") == True) & (F.col("valid_from") == run_date))
        .select("account_group_id", "scd_hash")
        .withColumnRenamed("scd_hash", "loaded_hash")
    )

    # Seleciona somente versões que ainda não foram inseridas hoje
    final_cols = df_incoming.columns  # garante a projeção final correta

    df_to_insert = (
        df_to_upsert
        .select([F.col(c) for c in final_cols] + ["change_type"])
        .join(df_loaded_today, on="account_group_id", how="left")
        .filter(F.col("loaded_hash").isNull() | (F.col("loaded_hash") != F.col("scd_hash")))
        .drop("loaded_hash", "change_type")
    )

    # Insere novas versões (append)
    (
        df_to_insert
        .write.format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    print("[OK] SCD2 executado com sucesso (fechamento + inserção de novas versões).")

# ============================================================
# 7) Observação sobre retenção
# ============================================================
# Em SCD2, geralmente NÃO se apaga histórico.
# Se você quiser retenção (ex.: 24 meses) por custo, implemente com cuidado,
# para não quebrar auditoria e análises temporais.

print("[DONE] Pipeline finalizado.")
