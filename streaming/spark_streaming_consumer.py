# =====================================================================
#  Tarea 3 - Consumidor Spark Streaming
#  Lee el topic "registros_consulares", limpia los registros y calcula
#  estadísticas en ventanas de 1 minuto.
#  Uso: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
#       streaming/spark_streaming_consumer.py
# =====================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


# =====================================================================
# 0. CONFIGURACIÓN
# =====================================================================

SERVIDOR_KAFKA = "localhost:9092"
TOPIC = "registros_consulares"
VENTANA = "1 minute"
FRECUENCIA = "10 seconds"
EDAD_MIN, EDAD_MAX = 25, 40
NO_REPORTADO = "NO REPORTADO"
VALORES_FALTANTES = ["", "SIN INFORMACIÓN", "NO INDICA", "DESCONOCIDO",
                     "(NO REGISTRA)", "SIN ETNIA REGISTRADA"]

spark = SparkSession.builder.appName("StreamingRegistrosConsulares").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema del JSON que envía el productor (mismas columnas del dataset)
CAMPOS = ["pais", "codigo_iso", "ciudad_residencia", "oficina_consular",
          "edad", "area_conocimiento", "nivel_academico", "estado_civil",
          "sexo", "etnia", "lugar_nacimiento", "localizacion",
          "fecha_registro", "cantidad_personas"]
esquema = StructType([StructField(c, StringType()) for c in CAMPOS]
                     + [StructField("timestamp", LongType())])


# =====================================================================
# 1. LECTURA DESDE KAFKA
# =====================================================================

df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", SERVIDOR_KAFKA)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .load())


# =====================================================================
# 2. PARSEO Y LIMPIEZA
# =====================================================================

registros = (df_kafka
             .select(F.from_json(F.col("value").cast("string"), esquema).alias("data"))
             .select("data.*")
             # Tipos
             .withColumn("edad", F.col("edad").cast("int"))
             .withColumn("cantidad_personas", F.col("cantidad_personas").cast("int"))
             .withColumn("fecha_evento", F.col("timestamp").cast("timestamp"))
             # Rango de edad del caso (25-40)
             .filter(F.col("edad").between(EDAD_MIN, EDAD_MAX)))

# Valores faltantes -> NO REPORTADO
for c in ["pais", "sexo"]:
    registros = registros.withColumn(
        c, F.when(F.col(c).isNull() | F.trim(F.col(c)).isin(VALORES_FALTANTES), NO_REPORTADO)
            .otherwise(F.upper(F.trim(F.col(c)))))


# =====================================================================
# 3. PROCESAMIENTO EN TIEMPO REAL
# =====================================================================

def por_ventana(columna):
    """Registros, personas y edad promedio por ventana y columna."""
    return (registros
            .groupBy(F.window("fecha_evento", VENTANA).alias("ventana"), columna)
            .agg(F.count("*").alias("registros"),
                 F.sum("cantidad_personas").alias("personas"),
                 F.round(F.avg("edad"), 1).alias("edad_promedio"))
            .select(F.date_format("ventana.start", "HH:mm").alias("inicio"),
                    F.date_format("ventana.end", "HH:mm").alias("fin"),
                    columna, "registros", "personas", "edad_promedio")
            .orderBy(F.desc("inicio"), F.desc("registros")))


por_pais = por_ventana("pais")
por_sexo = por_ventana("sexo")


# =====================================================================
# 4. VISUALIZACIÓN EN CONSOLA
# =====================================================================

def iniciar(consulta, nombre, filas):
    return (consulta.writeStream
            .queryName(nombre)
            .outputMode("complete")
            .format("console")
            .option("truncate", False)
            .option("numRows", filas)
            .trigger(processingTime=FRECUENCIA)
            .start())


iniciar(por_pais, "registros_por_pais", 15)
iniciar(por_sexo, "registros_por_sexo", 10)

print(f"Escuchando el topic '{TOPIC}'... (Ctrl+C para detener)")
spark.streams.awaitAnyTermination()
