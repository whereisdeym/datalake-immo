from pyspark.sql import SparkSession
from pyspark.sql.functions import year, to_date

# INITIALISATION SPARK
spark = (
    SparkSession.builder.appName("CreateDeltaTableDVF")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Optimisations GCS + Delta Lake
    .config("spark.delta.logStore.class", "io.delta.storage.GCSLogStore")
    .config("spark.delta.syncWithExternalCatalog", "false")
    .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()
)

# CHEMINS GCS
CLEAN_PATH = "gs://datalake-immo-007/clean/dvf/"
DELTA_PATH = "gs://datalake-immo-007/delta/dvf/"

# LECTURE DES DONNÉES
print("Lecture des données nettoyées (Parquet)...")
df = spark.read.parquet(CLEAN_PATH)

# AJOUT DE LA COLONNE 'annee' (si absente)
if "annee" not in df.columns:
    if "date_mutation" in df.columns:
        df = df.withColumn("annee", year(to_date("date_mutation", "yyyy-MM-dd")))
    else:
        raise Exception("Impossible de créer la colonne 'annee' (champ 'date_mutation' introuvable).")

# OPTIMISATIONS AVANT ÉCRITURE
df = df.coalesce(1)  # Réduit le nombre de fichiers (évite la lenteur GCS)
df.printSchema()
print(f"Nombre de lignes : {df.count()}")

# ÉCRITURE EN DELTA LAKE
print("Écriture en Delta Lake...")
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("annee") \
    .save(DELTA_PATH)

print(f"Données écrites avec succès dans {DELTA_PATH}")

# VÉRIFICATION DE LA TABLE DELTA
from delta.tables import DeltaTable

print("\nLecture et validation de la table Delta...")

# Lecture de la table Delta directement depuis GCS
delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# Affiche quelques lignes pour vérifier
print("Exemple de données stockées :")
delta_table.toDF().show(10, truncate=False)

# Vérifie le nombre total d'enregistrements
count = delta_table.toDF().count()
print(f"Nombre total de lignes dans la table Delta : {count}")

# Vérifie les partitions disponibles
print("Partitions détectées :")
partitions = delta_table.toDF().select("annee").distinct().orderBy("annee").collect()
for row in partitions:
    print(f" - {row['annee']}")

# Interrogation SQL : valeur foncière moyenne par année
print("\nStatistiques : valeur foncière moyenne par année")
delta_table.toDF().groupBy("annee").avg("valeur_fonciere").show()

print("Validation terminée avec succès !")

spark.stop()
