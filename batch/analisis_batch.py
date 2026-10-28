# =====================================================================
#  Tarea 3 - Procesamiento en batch con Apache Spark
#  Dataset : Colombianos en el exterior de 25 a 40 años (datos.gov.co)
#  Uso     : spark-submit batch/analisis_batch.py [ruta_datos]
#            (por defecto lee el dataset desde HDFS)
# =====================================================================

import os
import sys

import matplotlib
matplotlib.use("Agg")  # sin ventana gráfica (VM por SSH)
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =====================================================================
# 0. CONFIGURACIÓN
# =====================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_HDFS = "hdfs://localhost:9000/Tarea3/colombianos_exterior.csv.gz"
RUTA_DATOS = sys.argv[1] if len(sys.argv) > 1 else RUTA_HDFS
PARTICIONES = 4
DIR_GRAFICOS = os.path.join(BASE_DIR, "..", "resultados", "graficos")

EDAD_MIN, EDAD_MAX = 25, 40
ANIO_MIN_VALIDO = 2013  # inicio del registro consular en línea
NO_REPORTADO = "NO REPORTADO"
VALORES_FALTANTES = ["", "SIN INFORMACIÓN", "NO INDICA", "DESCONOCIDO",
                     "(NO REGISTRA)", "SIN ETNIA REGISTRADA"]

COLUMNAS = ["pais", "codigo_iso", "ciudad_residencia", "oficina_consular",
            "edad", "area_conocimiento", "nivel_academico", "estado_civil",
            "sexo", "etnia", "lugar_nacimiento", "localizacion",
            "fecha_registro", "cantidad_personas"]

CATEGORICAS = ["pais", "ciudad_residencia", "oficina_consular",
               "area_conocimiento", "nivel_academico", "estado_civil",
               "sexo", "etnia", "lugar_nacimiento"]

os.makedirs(DIR_GRAFICOS, exist_ok=True)

# Ruta local (si se pasa por argumento) con prefijo explícito file://
if "://" not in RUTA_DATOS:
    RUTA_DATOS = "file://" + os.path.abspath(RUTA_DATOS)

spark = SparkSession.builder.appName("AnalisisBatchColombianosExterior").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def titulo(texto):
    print("\n" + "=" * 70 + f"\n {texto}\n" + "=" * 70)


# =====================================================================
# 1. CARGA
# =====================================================================
titulo("1. CARGA DE DATOS")

df_raw = spark.read.option("header", True).option("encoding", "UTF-8").csv(RUTA_DATOS)

# Un .gz se lee en una sola partición: se reparte para procesar en paralelo
df_raw = df_raw.repartition(PARTICIONES)

filas_originales = df_raw.count()
print(f"Fuente           : {RUTA_DATOS}")
print(f"Filas originales : {filas_originales:,}")
print(f"Columnas         : {len(df_raw.columns)}")
df_raw.show(5, truncate=30)


# =====================================================================
# 2. LIMPIEZA
# =====================================================================
titulo("2. LIMPIEZA")

# Nombres de columna sin tildes ni espacios
df = df_raw.toDF(*COLUMNAS)

# Texto: quitar espacios y pasar a mayúsculas
for c in CATEGORICAS:
    df = df.withColumn(c, F.upper(F.trim(F.col(c))))

# Tipos numéricos
df = (df.withColumn("edad", F.col("edad").cast("int"))
        .withColumn("cantidad_personas", F.col("cantidad_personas").cast("int")))

# Unificar valores faltantes en una sola etiqueta
for c in CATEGORICAS:
    df = df.withColumn(c, F.when(F.col(c).isNull() | F.col(c).isin(VALORES_FALTANTES),
                                 NO_REPORTADO).otherwise(F.col(c)))

# Columna sin uso para el análisis
df = df.drop("localizacion")

# Edades inválidas (-1, >100) y fuera del rango 25-40
df = df.filter(F.col("edad").between(EDAD_MIN, EDAD_MAX))
filas_rango = df.count()

# Registros duplicados
df = df.dropDuplicates()
filas_limpias = df.count()

print(f"Filas originales                  : {filas_originales:,}")
print(f"Descartadas por edad (no 25-40)   : {filas_originales - filas_rango:,}")
print(f"Descartadas por duplicado         : {filas_rango - filas_limpias:,}")
print(f"Filas después de limpieza         : {filas_limpias:,}")


# =====================================================================
# 3. TRANSFORMACIÓN
# =====================================================================
titulo("3. TRANSFORMACIÓN")

partes_nac = F.split(F.col("lugar_nacimiento"), "/")
tiene_lugar = F.size(partes_nac) >= 3

df = (df
      # Fecha "AAAA-MM" -> fecha, año y mes (años < 2013 se descartan, ej. 1900)
      .withColumn("fecha_registro", F.to_date(F.concat(F.col("fecha_registro"), F.lit("-01"))))
      .withColumn("fecha_registro", F.when(F.year("fecha_registro") >= ANIO_MIN_VALIDO,
                                           F.col("fecha_registro")))
      .withColumn("anio_registro", F.year("fecha_registro"))
      .withColumn("mes_registro", F.month("fecha_registro"))

      # Lugar de nacimiento "PAIS/DEPTO/CIUDAD" -> tres columnas
      .withColumn("pais_nacimiento", F.when(tiene_lugar, partes_nac[0]).otherwise(NO_REPORTADO))
      .withColumn("depto_nacimiento", F.when(tiene_lugar, partes_nac[1]).otherwise(NO_REPORTADO))
      .withColumn("ciudad_nacimiento", F.when(tiene_lugar, partes_nac[2]).otherwise(NO_REPORTADO))

      # Unificar variantes de Bogotá (BOGOTA, BOGOTA D.C., SANTAFE DE BOGOTA...)
      .withColumn("ciudad_nacimiento",
                  F.when((F.col("depto_nacimiento") == "CUNDINAMARCA")
                         & F.col("ciudad_nacimiento").contains("BOGOTA")
                         & ~F.col("ciudad_nacimiento").startswith("PUERTO"),
                         "BOGOTA D.C.").otherwise(F.col("ciudad_nacimiento")))

      # Rango de edad
      .withColumn("rango_edad", F.when(F.col("edad") <= 29, "25-29")
                                 .when(F.col("edad") <= 34, "30-34")
                                 .otherwise("35-40"))

      .drop("lugar_nacimiento"))

df = df.cache()
df.printSchema()
df.show(5, truncate=25)


# =====================================================================
# 4. ANÁLISIS EXPLORATORIO (EDA)
# =====================================================================
titulo("4. ANÁLISIS EXPLORATORIO (EDA)")

total_personas = df.agg(F.sum("cantidad_personas")).first()[0]
print(f"Total de personas (25-40 años): {total_personas:,}")


def personas_por(*columnas, excluir_nr=True, orden="personas"):
    """Suma de personas por columna(s) con su porcentaje."""
    datos = df
    tipos = dict(df.dtypes)
    if excluir_nr:
        for c in columnas:
            datos = datos.filter(F.col(c).isNotNull())
            if tipos[c] == "string":
                datos = datos.filter(F.col(c) != NO_REPORTADO)
    resultado = (datos.groupBy(*columnas)
                 .agg(F.sum("cantidad_personas").alias("personas"))
                 .withColumn("porcentaje", F.round(F.col("personas") * 100 / total_personas, 2)))
    if orden == "personas":
        return resultado.orderBy(F.desc("personas"))
    return resultado.orderBy(*columnas)


# --- 4.1 Distribuciones ---
distribuciones = {
    "top10_paises_residencia": personas_por("pais").limit(10),
    "sexo": personas_por("sexo"),
    "rango_edad_sexo": personas_por("rango_edad", "sexo", orden="columnas"),
    "nivel_academico": personas_por("nivel_academico"),
    "top10_area_conocimiento": personas_por("area_conocimiento").limit(10),
    "estado_civil": personas_por("estado_civil"),
    "registros_por_anio": personas_por("anio_registro", orden="columnas"),
    "top10_depto_nacimiento": personas_por("depto_nacimiento")
                              .filter(F.col("depto_nacimiento") != NO_REPORTADO).limit(10),
    "top10_ciudad_nacimiento": personas_por("ciudad_nacimiento").limit(10),
}

resultados = {}
for nombre, tabla in distribuciones.items():
    print(f"\n4.1 {nombre}")
    resultados[nombre] = tabla.collect()
    tabla.show(20, truncate=False)

# --- 4.2 Mismo cálculo con RDD (map + reduceByKey + takeOrdered) ---
print("\n4.2 Top 5 países de residencia usando RDD")
top_paises_rdd = (df.rdd
                  .map(lambda fila: (fila["pais"], fila["cantidad_personas"]))
                  .reduceByKey(lambda a, b: a + b)
                  .takeOrdered(5, key=lambda x: -x[1]))
for pais, personas in top_paises_rdd:
    print(f"  {pais:<20} {personas:>10,}")


# =====================================================================
# 5. VISUALIZACIÓN
# =====================================================================
titulo("5. VISUALIZACIÓN")


def grafico_barras(nombre, filas, columna, titulo_grafico, horizontal=True):
    etiquetas = [str(f[columna]) for f in filas]
    valores = [f["personas"] for f in filas]
    plt.figure(figsize=(10, 6))
    if horizontal:
        plt.barh(etiquetas[::-1], valores[::-1], color="#2a6f97")
        plt.xlabel("Personas")
    else:
        plt.bar(etiquetas, valores, color="#2a6f97")
        plt.ylabel("Personas")
    plt.title(titulo_grafico)
    plt.tight_layout()
    plt.savefig(os.path.join(DIR_GRAFICOS, f"{nombre}.png"), dpi=120)
    plt.close()


# Gráficos de barras
grafico_barras("top10_paises_residencia", resultados["top10_paises_residencia"], "pais",
               "Top 10 países de residencia (25-40 años)")
grafico_barras("nivel_academico", resultados["nivel_academico"], "nivel_academico",
               "Nivel académico (sin NO REPORTADO)")
grafico_barras("top10_area_conocimiento", resultados["top10_area_conocimiento"], "area_conocimiento",
               "Top 10 áreas de conocimiento (sin NO REPORTADO)")
grafico_barras("top10_depto_nacimiento", resultados["top10_depto_nacimiento"], "depto_nacimiento",
               "Top 10 departamentos de nacimiento")
grafico_barras("estado_civil", resultados["estado_civil"], "estado_civil",
               "Estado civil (sin NO REPORTADO)")

# Línea: registros por año
anios = [f["anio_registro"] for f in resultados["registros_por_anio"]]
personas_anio = [f["personas"] for f in resultados["registros_por_anio"]]
plt.figure(figsize=(10, 5))
plt.plot(anios, personas_anio, marker="o", color="#2a6f97")
plt.title("Personas registradas por año")
plt.xlabel("Año de registro")
plt.ylabel("Personas")
plt.xticks(anios)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(DIR_GRAFICOS, "registros_por_anio.png"), dpi=120)
plt.close()

# Barras agrupadas: rango de edad por sexo
filas_rs = [f for f in resultados["rango_edad_sexo"] if f["sexo"] in ("FEMENINO", "MASCULINO")]
rangos = sorted({f["rango_edad"] for f in filas_rs})
ancho = 0.4
plt.figure(figsize=(8, 5))
for i, sexo in enumerate(["FEMENINO", "MASCULINO"]):
    valores = [next((f["personas"] for f in filas_rs
                     if f["rango_edad"] == r and f["sexo"] == sexo), 0) for r in rangos]
    posiciones = [x + i * ancho for x in range(len(rangos))]
    plt.bar(posiciones, valores, width=ancho, label=sexo.capitalize())
plt.xticks([x + ancho / 2 for x in range(len(rangos))], rangos)
plt.title("Personas por rango de edad y sexo")
plt.ylabel("Personas")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(DIR_GRAFICOS, "rango_edad_sexo.png"), dpi=120)
plt.close()

print(f"Gráficos guardados en : {os.path.abspath(DIR_GRAFICOS)}")


# =====================================================================
# 6. CIERRE
# =====================================================================
df.unpersist()
spark.stop()
print("\nProceso batch finalizado.")
