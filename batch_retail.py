from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc

spark = SparkSession.builder \
    .appName("AnalisisVentasRetail") \
    .getOrCreate()

print("Cargando datos...")
# Aquí agregamos el sep=";" para que lea bien el formato de Excel en español
df = spark.read.csv("/home/milena/online_retail.csv", header=True, inferSchema=True, encoding="ISO-8859-1", sep=";")

df_clean = df.filter((col("Quantity") > 0) & (col("UnitPrice") > 0)).na.drop()
df_clean = df_clean.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

print("=== TOP 10 PRODUCTOS MÁS VENDIDOS ===")
top_productos = df_clean.groupBy("Description") \
    .agg(sum("Quantity").alias("Total_Vendido")) \
    .orderBy(desc("Total_Vendido")) \
    .limit(10)
top_productos.show(truncate=False)

print("=== VENTAS TOTALES POR PAÍS (TOP 5) ===")
ventas_pais = df_clean.groupBy("Country") \
    .agg(sum("TotalPrice").alias("Ingresos_Totales")) \
    .orderBy(desc("Ingresos_Totales")) \
    .limit(5)
ventas_pais.show()

spark.stop()
