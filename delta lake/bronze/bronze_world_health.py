# Databricks notebook source
# MAGIC %md
# MAGIC ## Prática de engenharia de dados
# MAGIC
# MAGIC Vamos utilizar a fonte:
# MAGIC https://www.kaggle.com/datasets/bushraqurban/world-health-indicators-dataset?resource=download

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Camada Bronze").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Create dataframe
    file_path = "/FileStore/world_health_data.csv"

    df_bronze = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path) \
        .cache() 

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistência dos dados

# COMMAND ----------

df_bronze.write.format("delta").mode("overwrite").save("/mnt/bronze/bronze_world_health")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/bronze/bronze_world_health`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_world_health
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/bronze_world_health';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_world_health;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação de um catálogo

# COMMAND ----------

# %sql
# CREATE CATALOG project

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.bronze.bronze_world_health
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/bronze/bronze_world_health';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo dados do Catálogo

# COMMAND ----------

df_bronze = spark.read.table("hive_metastore.bronze.bronze_world_health") \
.cache()

df_bronze.count()

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# %sql
# SELECT * FROM hive_metastore.bronze.bronze_world_health

# COMMAND ----------

# %sql
# show create  TABLE  hive_metastore.bronze.bronze_world_health

# COMMAND ----------

# MAGIC %md
# MAGIC ## Particionamento de tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC describe TABLE  hive_metastore.bronze.bronze_world_health

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# Salvar o DataFrame no formato Delta com particionamento
df_bronze.write.format("delta").mode("overwrite").partitionBy("year").option("overwriteSchema", "true").save("/mnt/bronze/bronze_world_health")

# COMMAND ----------

# DBTITLE 1,Registrar a tabela no catálogo
spark.sql("""
    CREATE TABLE IF NOT EXISTS hive_metastore.bronze.bronze_world_health
    USING DELTA
    LOCATION '/mnt/bronze/bronze_world_health'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação de tempo com a coluna filtrada

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.bronze.bronze_world_health

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.bronze.bronze_world_health WHERE year = 2022;
