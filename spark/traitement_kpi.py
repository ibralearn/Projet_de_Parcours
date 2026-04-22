from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, expr, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType

# 1. Définir le schéma attendu pour les messages JSON
schema = StructType() \
    .add("machine_id", StringType()) \
    .add("valeur", FloatType()) \
    .add("timestamp", StringType()) \
    .add("type_capteur", StringType())

# 2. Démarrer la session Spark
spark = SparkSession.builder \
    .appName("raffinerie-iot") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 3. Lire les données en streaming depuis Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-data") \
    .load()

# 4. Décoder les messages JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Convertir correctement le champ timestamp
json_df = json_df.withColumn("timestamp", to_timestamp("timestamp"))

# 6. Sauvegarde brute sur MinIO (désactivée temporairement)
# Cette sortie peut arrêter tout le job si le bucket n'existe pas.

# 7. Filtrer les données valides
filtrees = json_df.filter(
    ((col("type_capteur") == "temperature") & (col("valeur").between(30, 150))) |
    ((col("type_capteur") == "vibration") & (col("valeur").between(0, 5)))
)

# 8. Fonction batch pour enregistrer les mesures filtrées dans TimescaleDB
def save_filtrees_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "mesures_filtrees") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 9. Écriture en base TimescaleDB des mesures filtrées
filtrees.writeStream \
    .foreachBatch(save_filtrees_to_pg) \
    .option("checkpointLocation", "/tmp/checkpoint_filtrees") \
    .outputMode("append") \
    .start()

# 10. Calcul des KPI : moyenne glissante sur 1 minute
kpi = filtrees.withColumn("ts", expr("to_timestamp(timestamp)")) \
    .withWatermark("ts", "30 seconds") \
    .groupBy(window("ts", "1 minute"), "type_capteur") \
    .agg(avg("valeur").alias("valeur")) \
    .withColumn("type_kpi", col("type_capteur")) \
    .withColumn("unite", expr("CASE WHEN type_capteur = 'temperature' THEN '°C' ELSE 'mm/s' END")) \
    .selectExpr("window.start as timestamp", "type_kpi", "valeur", "unite")

# 11. Fonction batch pour enregistrer les KPI dans TimescaleDB
def save_kpi_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "kpi_indicateurs") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 12. Écriture en base TimescaleDB des KPI
kpi.writeStream \
    .foreachBatch(save_kpi_to_pg) \
    .option("checkpointLocation", "/tmp/checkpoint_kpi") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
