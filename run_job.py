"""
📌 run_job.py — Driver do Job SCD Type 2
=====================================

Função deste arquivo:
- Atuar como ponto de entrada (entrypoint) do pipeline
- Preparar contexto de execução (catalog, schema, datas)
- Delegar TODA a lógica de negócio para o job em src/jobs

Por que isso é bom?
- Mantém notebooks leves
- Código versionável e testável no src/
- Fácil de plugar em Databricks Jobs / Workflows
"""

# ============================================================
# 1) Imports básicos
# ============================================================
from pyspark.sql import SparkSession

# Importa o job principal
from src.jobs.scd2_risk_dimension import run_scd2_job

# ============================================================
# 2) Criação da Spark Session
# ============================================================
spark = (
    SparkSession
    .builder
    .appName("run_scd2_financial_risk")
    .getOrCreate()
)

# ============================================================
# 3) Parâmetros de execução
# ============================================================
# Quando rodar como Databricks Job, esses params podem vir do Workflow.
# Quando rodar local/notebook, usamos valores padrão.

try:
    run_params = dbutils.notebook.entry_point.getCurrentBindings()
except Exception:
    run_params = {}

catalog = run_params.get("catalog", "financas")
schema = run_params.get("schema", "ops_finance")

# ============================================================
# 4) Contexto do Unity Catalog
# ============================================================
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE {schema}")

print(f"[INFO] Catalog: {catalog}")
print(f"[INFO] Schema: {schema}")

# ============================================================
# 5) Execução do Job SCD2
# ============================================================
# Toda a lógica de SCD2 está encapsulada aqui
run_scd2_job(
    spark=spark,
    catalog=catalog,
    schema=schema
)

print("[DONE] run_job finalizado com sucesso.")
