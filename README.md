# Procesamiento de Datos con Apache Spark

Tarea 3 del curso Big Data (UNAD, código 202016911), este proyecto analiza datos de colombianos en el exterior con Apache Spark, con procesamiento en batch sobre Hadoop HDFS y procesamiento en tiempo real con Kafka y Spark Streaming.

## Requisitos

El proyecto se ejecuta en la **máquina virtual Big Data del curso** (Ubuntu 22.04 LTS, usuario `vboxuser`), a la que se accede por SSH, Hadoop y Spark vienen configurados de las tareas anteriores y Kafka se instala siguiendo el *Anexo 3 – Instructivo Spark Streaming y Kafka*.

### En Windows (equipo anfitrión)

| Herramienta | Uso |
|---|---|
| VirtualBox | Ejecutar la máquina virtual Big Data |
| PuTTY | Conectarse por SSH a la VM (se usan dos sesiones para el streaming) |

### En la máquina virtual

| Herramienta | Versión | Uso |
|---|---|---|
| Git | cualquiera reciente | Clonar el repositorio |
| Java (OpenJDK) | 11 | Requerido por Hadoop, Spark y Kafka |
| Python | 3.10 o superior | Scripts del proyecto |
| Hadoop (HDFS) | 3.x, en `hdfs://localhost:9000` (interfaz web: puerto `9870`) | Almacenar el dataset |
| Apache Spark | **3.5.3** (Scala 2.12) | Procesamiento batch y streaming |
| Apache Kafka | **3.9.2**, compilación para Scala 2.12 (`kafka_2.12-3.9.2`), con ZooKeeper incluido | Cola de mensajes en tiempo real |
| Conector Spark–Kafka | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` | Permite a Spark leer de Kafka, lo descarga `spark-submit --packages` la primera vez, por lo que necesita internet |
| kafka-python | 2.0.x | Productor de Kafka en Python |
| matplotlib | cualquiera reciente | Gráficos del análisis batch |

**Compatibilidad:** el sufijo `2.12` debe ser el mismo en Kafka (`kafka_2.12-3.9.2`) y en el conector (`spark-sql-kafka-0-10_2.12`), ya que corresponde a la versión de Scala con la que está compilado Spark 3.5.3, y la versión final del conector (`3.5.3`) debe coincidir con la de Spark.

### Instalación de lo que falta

```bash
# Git y librerías de Python
sudo apt install -y git
pip install kafka-python matplotlib

# Apache Kafka 3.9.2 (según el Anexo 3)
wget https://downloads.apache.org/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz
sudo mkdir -p /opt/Kafka
sudo mv kafka_2.12-3.9.2 /opt/Kafka/
```

Si la versión 3.9.2 ya no está en `downloads.apache.org`, se encuentra en `https://archive.apache.org/dist/kafka/3.9.2/`.

### Verificación

```bash
git --version
java -version                  # 11.x
python3 --version
hadoop version                 # 3.x
spark-submit --version         # 3.5.3, Scala 2.12
ls /opt/Kafka/kafka_2.12-3.9.2 # bin/  config/  libs/ ...
```

## Problema

¿Cuál es el perfil de los colombianos de 25 a 40 años inscritos en el Registro Consular? El análisis responde:

- A qué países emigran.
- Qué nivel académico y área de conocimiento tienen.
- Cómo se distribuyen por sexo, rango de edad y estado civil.
- De qué departamentos de Colombia salen.
- Cómo ha evolucionado el número de registros por año.

## Conjunto de datos

| | |
|---|---|
| Fuente | [Colombianos en el exterior de 25 a 40 años – datos.gov.co](https://www.datos.gov.co/Estad-sticas-Nacionales/Colombianos-en-el-exterior-de-25-a-40-a-os/3k8j-4d42) |
| Entidad | Cancillería de Colombia |
| Archivo | `data/colombianos_exterior.csv.gz` (46 MB comprimido, ~389 MB sin comprimir) |
| Tamaño | 1.996.461 filas, 14 columnas |
| Fecha de descarga | 27 de octubre de 2026 |
| Columnas | País, Código ISO país, Ciudad de Residencia, Oficina de circunscripción consular, Edad (años), Área Conocimiento, Nivel Académico, Estado civil, Sexo, Pertenencia étnica, Ciudad de Nacimiento, Localización, Fecha de Registro, Cantidad de personas |

El dataset se incluye en el repositorio comprimido en `.gz`, para que cualquiera reproduzca el análisis con los mismos datos, y Spark lo lee directamente sin descomprimirlo.

## Estructura

```
procesamiento-de-datos-con-apache-spark/
├── data/
│   └── colombianos_exterior.csv.gz  # Dataset (fuente de datos)
├── batch/
│   └── analisis_batch.py            # Carga, limpieza, transformación, EDA y gráficos
├── streaming/
│   ├── kafka_producer.py            # Genera registros y los envía a Kafka
│   └── spark_streaming_consumer.py  # Consume el topic y calcula estadísticas por ventana
└── resultados/                      # Se genera al ejecutar el batch
    ├── graficos/                    # .png
    └── tablas/                      # .csv
```

---

## 1. Procesamiento en batch

### Pasos

**1. Clonar el repositorio en la VM**

```bash
git clone https://github.com/<usuario>/procesamiento-de-datos-con-apache-spark.git
cd procesamiento-de-datos-con-apache-spark
cp data/colombianos_exterior.csv.gz /tmp/
chmod 644 /tmp/colombianos_exterior.csv.gz
```

El archivo se copia a `/tmp` para que el usuario `hadoop` pueda leerlo.

**2. Iniciar Hadoop y subir el dataset a HDFS**

```bash
su - hadoop
start-all.sh
jps                              # deben aparecer NameNode, DataNode, ResourceManager y NodeManager
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put /tmp/colombianos_exterior.csv.gz /Tarea3/
hdfs dfs -ls /Tarea3
exit
```

También se puede verificar en el navegador en `http://<IP-de-la-VM>:9870` → *Utilities* → *Browse the file system* → `/Tarea3`.

**3. Ejecutar el análisis** (como `vboxuser`, desde la carpeta del repositorio)

```bash
spark-submit batch/analisis_batch.py
```

Por defecto el script lee `hdfs://localhost:9000/Tarea3/colombianos_exterior.csv.gz`, y mientras se ejecuta la interfaz de Spark está disponible en `http://<IP-de-la-VM>:4040`.

### Qué hace el script

| Etapa | Qué hace |
|---|---|
| 1. Carga | Lee el dataset desde HDFS y lo reparte en particiones |
| 2. Limpieza | Renombra las columnas, normaliza el texto, convierte los tipos, agrupa los faltantes (`SIN INFORMACIÓN`, `NO INDICA`, `DESCONOCIDO`, etc.) en `NO REPORTADO`, filtra las edades al rango 25–40 (descartando también valores inválidos como -1 o 140) y elimina duplicados |
| 3. Transformación | Convierte la fecha `AAAA-MM` en año y mes descartando fechas inválidas como 1900, separa el lugar de nacimiento en país, departamento y ciudad, unifica las variantes de Bogotá y crea el rango de edad |
| 4. EDA | Calcula estadísticas de edad, el porcentaje de datos no reportados y las distribuciones por país, sexo, nivel académico, área, estado civil, año y departamento, e incluye el top de países calculado con RDD (`map`, `reduceByKey`, `takeOrdered`) |
| 5. Visualización | Guarda gráficos en `resultados/graficos/` y tablas en `resultados/tablas/` |

---

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

Sigue el esquema del Anexo 3, adaptado al caso de registros consulares:

```
kafka_producer.py ──► Kafka (topic: registros_consulares) ──► spark_streaming_consumer.py ──► consola
```

- **`kafka_producer.py`**: genera un registro consular por segundo (país, sexo, edad, nivel académico, estado civil y `timestamp`) con proporciones parecidas a las del dataset real, y lo envía al topic.
- **`spark_streaming_consumer.py`**: se suscribe al topic y, cada 10 segundos, muestra estos resultados por ventanas de 1 minuto:
  - Registros y edad promedio por país.
  - Registros y edad promedio por sexo.

### Pasos

**Terminal 1 (PuTTY): iniciar ZooKeeper y Kafka, y crear el topic**

```bash
sudo su
/opt/Kafka/kafka_2.12-3.9.2/bin/zookeeper-server-start.sh /opt/Kafka/kafka_2.12-3.9.2/config/zookeeper.properties &
/opt/Kafka/kafka_2.12-3.9.2/bin/kafka-server-start.sh /opt/Kafka/kafka_2.12-3.9.2/config/server.properties &
/opt/Kafka/kafka_2.12-3.9.2/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic registros_consulares
exit
```

El topic solo se crea la primera vez.

**Terminal 1: iniciar el productor**

```bash
python3 streaming/kafka_producer.py
```

**Terminal 2 (nueva sesión de PuTTY): iniciar el consumidor**

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/spark_streaming_consumer.py
```

Primero se inicia el productor y luego el consumidor, cada proceso se detiene con `Ctrl+C`, y mientras corre la interfaz de Spark en `http://<IP-de-la-VM>:4040` muestra Jobs, Stages, Environment, Executors y la pestaña Structured Streaming.
