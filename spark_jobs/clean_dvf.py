from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 🔹 Initialisation Spark
spark = SparkSession.builder.appName("CleanDVF").getOrCreate()

# 🔹 Lecture des données brutes DVF
RAW_PATH = "gs://datalake-immo-007/raw/dvf/dvf_2023.csv.gz"
print("Lecture des fichiers DVF...")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)

# 🔹 Vérif : affichons les colonnes lues
print("Colonnes disponibles :", df.columns)

# 🔹 Nettoyage : sélection des colonnes pertinentes
print("Nettoyage des données...")
df_clean = df.select(
    col("date_mutation"),
    col("valeur_fonciere"),
    col("code_commune"),
    col("surface_reelle_bati").alias("surface_m2"),
    col("type_local"),
    col("code_departement"),
    col("nom_commune")
)

# 🔹 Suppression des lignes vides et des doublons
df_clean = df_clean.dropna(how="any").dropDuplicates()

# 🔹 Écriture dans le dossier "clean" du bucket
OUTPUT_PATH = "gs://datalake-immo-007/clean/dvf/"
print(f"Sauvegarde des données nettoyées vers {OUTPUT_PATH}")
df_clean.write.mode("overwrite").parquet(OUTPUT_PATH)

print("Nettoyage terminé avec succès !")

spark.stop()
