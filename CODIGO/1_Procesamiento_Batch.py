from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, count, sum
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import os

# 1. Crear la sesion de Spark
spark = (
    SparkSession.builder
    .appName("Analisis de incautaciones en Colombia")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el archivo CSV
ruta = "/home/vboxuser/INCAUTACIONES_DE_BASE_DE_COCA_20261024.csv"

df = spark.read.csv(
    ruta,
    header=True,
    inferSchema=False
)

# Normalizar nombres de columnas
df = df.toDF(*[c.strip() for c in df.columns])

if "FECHA HECHO" in df.columns:
    df = df.withColumnRenamed("FECHA HECHO", "FECHA_HECHO")

print("\n========== DATOS ORIGINALES ==========")
print("Total de registros:", df.count())
print("Columnas:", df.columns)

# 3. Transformar los tipos de datos
df_limpio = (
    df
    .withColumn(
        "FECHA_HECHO",
        to_date(col("FECHA_HECHO"), "dd/MM/yyyy")
    )
    .withColumn(
        "CANTIDAD",
        col("CANTIDAD").cast("double")
    )
    .withColumn(
        "COD_DEPTO",
        col("COD_DEPTO").cast("integer")
    )
)

print("\n========== CALIDAD DE LOS DATOS ==========")

# 4. Identificar registros incompletos o invalidos
df_incompleto = df_limpio.filter(
    col("FECHA_HECHO").isNull()
    | col("DEPARTAMENTO").isNull()
    | col("CANTIDAD").isNull()
)

print("Registros con datos esenciales invalidos:", df_incompleto.count())

# 5. Depurar los datos para el analisis
df_limpio = df_limpio.filter(
    col("FECHA_HECHO").isNotNull()
    & col("DEPARTAMENTO").isNotNull()
    & (col("DEPARTAMENTO") != "")
    & col("CANTIDAD").isNotNull()
)

print("Registros despues de la limpieza:", df_limpio.count())

# 6. Extraer el año
df_limpio = df_limpio.withColumn(
    "ANIO",
    year(col("FECHA_HECHO"))
)

# 7. Analizar los registros por año
por_anio = (
    df_limpio
    .groupBy("ANIO")
    .agg(count("*").alias("NUMERO_REGISTROS"))
    .filter(col("ANIO").isNotNull())
    .orderBy("ANIO")
)

print("\n========== REGISTROS POR ANIO ==========")
por_anio.show(30, truncate=False)

# 8. Analizar los diez departamentos con mas registros
por_departamento = (
    df_limpio
    .groupBy("DEPARTAMENTO")
    .agg(count("*").alias("NUMERO_REGISTROS"))
    .orderBy(col("NUMERO_REGISTROS").desc())
    .limit(10)
)

print("\n========== TOP 10 DEPARTAMENTOS ==========")
por_departamento.show(truncate=False)

# 9. Crear carpeta para guardar los resultados
carpeta = os.path.expanduser("~/resultados_incautaciones")
os.makedirs(carpeta, exist_ok=True)

# 10. Grafica de barras: top 10 departamentos
datos_departamentos = por_departamento.collect()

nombres = [fila["DEPARTAMENTO"] for fila in datos_departamentos]
valores = [fila["NUMERO_REGISTROS"] for fila in datos_departamentos]

plt.figure(figsize=(12, 7))
plt.barh(nombres[::-1], valores[::-1])
plt.title("Top 10 departamentos por numero de registros de incautaciones")
plt.xlabel("Numero de registros")
plt.ylabel("Departamento")
plt.tight_layout()

archivo_barras = os.path.join(
    carpeta, "top_10_departamentos.png"
)
plt.savefig(archivo_barras, dpi=150)
plt.close()

# 11. Grafica de lineas: evolucion anual
datos_anuales = por_anio.collect()

anios = [fila["ANIO"] for fila in datos_anuales]
registros = [fila["NUMERO_REGISTROS"] for fila in datos_anuales]

plt.figure(figsize=(12, 6))
plt.plot(anios, registros, marker="o")
plt.title("Evolucion anual de los registros de incautaciones")
plt.xlabel("Anio")
plt.ylabel("Numero de registros")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()

archivo_lineas = os.path.join(
    carpeta, "registros_por_anio.png"
)
plt.savefig(archivo_lineas, dpi=150)
plt.close()

# 12. Mostrar la ubicacion de las graficas
print("\n========== GRAFICAS GENERADAS ==========")
print("Grafica de departamentos:", archivo_barras)
print("Grafica anual:", archivo_lineas)

# 12. Analizar kilogramos incautados por año
por_anio_kg = (
    df_limpio
    .filter(col("UNIDAD") == "KILOGRAMO")
    .groupBy("ANIO")
    .agg(
        sum("CANTIDAD").alias("KILOGRAMOS_INCAUTADOS")
    )
    .filter(col("ANIO").isNotNull())
    .orderBy("ANIO")
)

print("\n========== KILOGRAMOS INCAUTADOS POR ANIO ==========")
por_anio_kg.show(30, truncate=False)

# 13. Crear la tercera grafica
datos_kg = por_anio_kg.collect()

anios_kg = [fila["ANIO"] for fila in datos_kg]
kilogramos = [fila["KILOGRAMOS_INCAUTADOS"] for fila in datos_kg]

plt.figure(figsize=(12, 6))
plt.plot(anios_kg, kilogramos, marker="o")
plt.title("Evolucion anual de kilogramos de base de coca incautados")
plt.xlabel("Anio")
plt.ylabel("Kilogramos incautados")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()

archivo_kg = os.path.join(
    carpeta, "kilogramos_por_anio.png"
)
plt.savefig(archivo_kg, dpi=150)
plt.close()

print("Grafica de kilogramos:", archivo_kg)

print("\nAnalisis finalizado correctamente.")

spark.stop()