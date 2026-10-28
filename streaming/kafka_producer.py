# =====================================================================
#  Tarea 3 - Productor Kafka
#  Recibe filas reales del dataset (leídas desde HDFS) por la entrada
#  estándar y las envía al topic "registros_consulares", simulando
#  la llegada de registros en tiempo real.
#  Uso:
#    hdfs dfs -cat /Tarea3/colombianos_exterior.csv.gz | zcat | tail -n +2 \
#      | shuf -n 5000 | python3 streaming/kafka_producer.py
# =====================================================================

import sys
import csv
import json
import time

from kafka import KafkaProducer


# =====================================================================
# 0. CONFIGURACIÓN
# =====================================================================

SERVIDOR_KAFKA = "localhost:9092"
TOPIC = "registros_consulares"
REGISTROS_POR_SEGUNDO = 5

COLUMNAS = ["pais", "codigo_iso", "ciudad_residencia", "oficina_consular",
            "edad", "area_conocimiento", "nivel_academico", "estado_civil",
            "sexo", "etnia", "lugar_nacimiento", "localizacion",
            "fecha_registro", "cantidad_personas"]

# Sin datos por la tubería no hay nada que enviar
if sys.stdin.isatty():
    sys.exit("Uso: hdfs dfs -cat /Tarea3/colombianos_exterior.csv.gz | zcat | tail -n +2 "
             "| shuf -n 5000 | python3 streaming/kafka_producer.py")

sys.stdin.reconfigure(encoding="utf-8", errors="ignore")


# =====================================================================
# 1. ENVÍO AL TOPIC
# =====================================================================

productor = KafkaProducer(bootstrap_servers=[SERVIDOR_KAFKA],
                          value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"))

print(f"Enviando registros al topic '{TOPIC}' ({REGISTROS_POR_SEGUNDO}/segundo, Ctrl+C para detener)...")

enviados = 0
try:
    for fila in csv.reader(sys.stdin):
        # Filas incompletas se omiten
        if len(fila) != len(COLUMNAS):
            continue
        registro = dict(zip(COLUMNAS, fila))
        registro["timestamp"] = int(time.time())
        productor.send(TOPIC, value=registro)
        enviados += 1
        print(f"[{enviados}] {registro['pais']} | {registro['sexo']} | "
              f"{registro['edad']} años | {registro['nivel_academico']}")
        time.sleep(1 / REGISTROS_POR_SEGUNDO)
    print(f"\nFin de los datos. Registros enviados: {enviados}")
except KeyboardInterrupt:
    print(f"\nProductor detenido. Registros enviados: {enviados}")
finally:
    productor.flush()
    productor.close()
