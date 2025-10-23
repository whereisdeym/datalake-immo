from pyspark.sql import SparkSession

spark = (  
    SparkSession.builder.appName("read-delta-dvf")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.class", "io.delta.storage.GCSLogStore")
    .getOrCreate()
)

df = spark.read.format("delta").load("gs://datalake-immo-007/delta/dvf/")
df.printSchema()
print(f"Nombre total de lignes : {df.count()}")
df.show(5)
