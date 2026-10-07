#!/usr/bin/env python3
"""
detector.py - Trabajo 3 Big Data - CASO C: Monitoreo de precios de acciones/cripto

Lee los precios que manda productor_precios.py al topic 'precios' de Kafka y
aplica 3 reglas en tiempo real con Spark Structured Streaming:

  Regla 1 - MEDIAS MÓVILES: media corta (últimos 3 precios) y media larga
            (últimos 8 precios) de cada símbolo.
  Regla 2 - VOLATILIDAD: alerta cuando el precio de un símbolo se mueve más
            de 3% (máximo vs mínimo) dentro de una ventana de 5 segundos.
  Regla 3 - CRUCE DE MEDIAS: señal de COMPRA cuando la media corta cruza por
            encima de la larga, y de VENTA cuando la cruza por debajo.

Todo lo que detecta también lo guarda en la carpeta 'resultados/' (CSV)
para después sacar las conclusiones del informe con resumen.py.

Uso (con Kafka arriba):
    python detector.py          -> corre hasta que le den Ctrl+C
    python detector.py 300      -> corre 300 segundos y para solo
Y en otra terminal:  python productor_precios.py
"""
import os, sys, csv, tempfile, warnings
import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# En Windows Spark busca el comando "python" y a veces no lo encuentra,
# así que le decimos explícitamente cuál Python usar.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
warnings.filterwarnings("ignore", category=FutureWarning)   # avisos de versión de pandas que no afectan

# ---------------- Parámetros de las reglas ----------------
MEDIA_CORTA = 3            # últimos 3 precios
MEDIA_LARGA = 8            # últimos 8 precios
UMBRAL_VOLATILIDAD = 3.0   # % de diferencia entre el máximo y el mínimo de la ventana
VENTANA = "5 seconds"      # tamaño de la ventana de tiempo para la volatilidad
# (primero probamos con 10 s pero la mayoría de las ventanas pasaban el 3% solo por el
#  movimiento normal del precio; con 5 s las alertas sí corresponden a saltos bruscos)

os.makedirs("resultados", exist_ok=True)   # aquí se guardan los CSV con lo detectado


# =====================================================================
# 1. Arrancar Spark con el conector de Kafka
# =====================================================================
# El conector tiene que ser de la misma versión de pyspark.
# Spark 4.x usa Scala 2.13 (si tienen Spark 3.5 hay que cambiar 2.13 por 2.12).
# La primera vez se demora un poco porque descarga el conector.
paquete_kafka = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

spark = (SparkSession.builder
         .appName("DetectorPrecios")
         .master("local[*]")
         .config("spark.jars.packages", paquete_kafka)
         .config("spark.sql.shuffle.partitions", "4")   # pocas particiones, como en clase
         .config("spark.ui.showConsoleProgress", "false")   # quita las barras [Stage ...] de la consola
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "listo")


# =====================================================================
# 2. Leer el topic 'precios' de Kafka y parsear el JSON
# =====================================================================
crudo = (spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "precios")
         .option("startingOffsets", "latest")   # solo los precios que lleguen desde ahora
         .load())

# Cada mensaje viene como bytes en la columna 'value', por ejemplo:
#   {"simbolo": "BTC", "precio": 60123.45}
esquema = StructType([
    StructField("simbolo", StringType()),
    StructField("precio", DoubleType()),
])

# El evento no trae la hora, entonces usamos el 'timestamp' que Kafka le pone a cada mensaje
precios = (crudo
           .select(F.from_json(F.col("value").cast("string"), esquema).alias("e"),
                   F.col("timestamp"))
           .select("e.simbolo", "e.precio", "timestamp"))


def guardar_csv(filas, archivo, columnas):
    """Agrega filas al final de un CSV de la carpeta resultados (le pone encabezado si es nuevo)."""
    ruta = os.path.join("resultados", archivo)
    es_nuevo = not os.path.exists(ruta)
    with open(ruta, "a", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        if es_nuevo:
            escritor.writerow(columnas)
        for fila in filas:
            escritor.writerow([fila[c] for c in columnas])


# =====================================================================
# REGLAS 1 y 3: medias móviles y cruce de medias (con foreachBatch)
# =====================================================================
# Las window functions tipo avg().over(...) o lag() NO se pueden usar directo sobre
# un stream, por eso usamos foreachBatch: Spark nos entrega cada micro-batch como un
# DataFrame normal y ahí sí funcionan.
#
# Problema: cada batch trae pocos precios por símbolo (unos 10) y la media larga
# necesita los 8 últimos, que pueden venir del batch anterior.
# Solución: guardamos en 'historial' los últimos 8 precios de cada símbolo y los
# pegamos al comienzo del siguiente batch.
historial = None                            # DataFrame con los últimos 8 precios de cada símbolo
conteo_senales = {"COMPRA": 0, "VENTA": 0}


def procesar_medias(batch, batch_id):
    global historial

    if batch.isEmpty():
        return

    # Juntamos el historial con los precios nuevos (marcamos cuáles son nuevos)
    nuevos = batch.withColumn("es_nuevo", F.lit(True))
    if historial is not None:
        datos = historial.withColumn("es_nuevo", F.lit(False)).unionByName(nuevos)
    else:
        datos = nuevos

    # Ventanas por símbolo, ordenadas por tiempo
    w = Window.partitionBy("simbolo").orderBy("timestamp")
    w_corta = w.rowsBetween(-(MEDIA_CORTA - 1), 0)   # el precio actual + los 2 anteriores
    w_larga = w.rowsBetween(-(MEDIA_LARGA - 1), 0)   # el precio actual + los 7 anteriores

    # ---- REGLA 1: medias móviles ----
    datos = (datos
             .withColumn("media_corta", F.avg("precio").over(w_corta))
             .withColumn("media_larga", F.avg("precio").over(w_larga))
             .withColumn("cuantos", F.count("precio").over(w_larga)))

    # Mientras un símbolo no tenga 8 precios todavía, la media larga queda vacía (null)
    datos = datos.withColumn("media_larga",
                             F.when(F.col("cuantos") == MEDIA_LARGA, F.col("media_larga")))

    # ---- REGLA 3: cruce de medias ----
    # diferencia = corta - larga. Si pasa de negativa a positiva la corta cruzó hacia
    # arriba (COMPRA); si pasa de positiva a negativa cruzó hacia abajo (VENTA).
    datos = (datos
             .withColumn("diferencia", F.col("media_corta") - F.col("media_larga"))
             .withColumn("diferencia_anterior", F.lag("diferencia").over(w))
             .withColumn("senal",
                         F.when((F.col("diferencia_anterior") <= 0) & (F.col("diferencia") > 0), "COMPRA")
                          .when((F.col("diferencia_anterior") >= 0) & (F.col("diferencia") < 0), "VENTA")))
    datos = datos.cache()   # lo vamos a usar varias veces abajo

    # Solo mostramos y guardamos los precios nuevos (los del historial ya se mostraron antes)
    resultado = (datos.filter("es_nuevo")
                 .select("simbolo", "timestamp", "precio",
                         F.round("media_corta", 2).alias("media_corta"),
                         F.round("media_larga", 2).alias("media_larga"),
                         "senal"))

    # Panel: el último precio de cada símbolo con sus dos medias
    w_reciente = Window.partitionBy("simbolo").orderBy(F.col("timestamp").desc())
    panel = (resultado
             .withColumn("n", F.row_number().over(w_reciente))
             .filter("n = 1")
             .withColumn("tendencia",
                         F.when(F.col("media_corta") > F.col("media_larga"), "alcista")
                          .when(F.col("media_corta") < F.col("media_larga"), "bajista")
                          .otherwise("calculando..."))
             .select("simbolo", "precio", "media_corta", "media_larga", "tendencia")
             .orderBy("simbolo"))
    print(f"\n========== Batch {batch_id} | MEDIAS MÓVILES (corta={MEDIA_CORTA}, larga={MEDIA_LARGA}) ==========")
    panel.show()

    # Señales de compra/venta de este batch
    senales = resultado.filter(F.col("senal").isNotNull()).orderBy("timestamp").collect()
    for s in senales:
        conteo_senales[s["senal"]] += 1
        if s["senal"] == "COMPRA":
            explicacion = "la media corta cruzó POR ENCIMA de la larga"
        else:
            explicacion = "la media corta cruzó POR DEBAJO de la larga"
        print(f"  >>> SEÑAL DE {s['senal']}: {s['simbolo']} a ${s['precio']:,.2f} "
              f"({s['timestamp']:%H:%M:%S}) - {explicacion}")
    print(f"  Señales acumuladas -> COMPRA: {conteo_senales['COMPRA']} | VENTA: {conteo_senales['VENTA']}")

    # Guardamos todo para el informe
    columnas = ["simbolo", "timestamp", "precio", "media_corta", "media_larga", "senal"]
    guardar_csv(resultado.orderBy("timestamp").collect(), "medias.csv", columnas)
    guardar_csv(senales, "senales.csv", columnas)

    # Guardamos los últimos 8 precios de cada símbolo para el siguiente batch.
    # localCheckpoint() "congela" esas filas en memoria; si no, Spark tendría que
    # recalcular todos los batches anteriores cada vez y se pone cada vez más lento.
    historial = (datos
                 .withColumn("n", F.row_number().over(w_reciente))
                 .filter(F.col("n") <= MEDIA_LARGA)
                 .select("simbolo", "precio", "timestamp")
                 .localCheckpoint())
    datos.unpersist()


# =====================================================================
# REGLA 2: volatilidad por ventana de tiempo
# =====================================================================
# Aquí sí sirve la función window() normal de streaming (la de la Clase 12):
# agrupamos por símbolo en ventanas de 5 segundos y sacamos el precio mínimo y
# máximo. Si (máximo - mínimo) / mínimo es más de 3%, hubo un movimiento brusco.
volatilidad = (precios
               .withWatermark("timestamp", "5 seconds")   # aceptamos datos atrasados hasta 5 s
               .groupBy(F.window("timestamp", VENTANA), "simbolo")
               .agg(F.min("precio").alias("minimo"),
                    F.max("precio").alias("maximo"),
                    F.count("*").alias("num_precios"))
               .withColumn("rango_pct",
                           F.round((F.col("maximo") - F.col("minimo")) / F.col("minimo") * 100, 2)))

# Con outputMode "update" la misma ventana puede llegar varias veces mientras se va
# llenando, así que guardamos cuáles ya alertamos para no repetir la alerta.
ventanas_alertadas = set()
conteo_volatilidad = {}


def procesar_volatilidad(batch, batch_id):
    alertas = batch.filter(F.col("rango_pct") > UMBRAL_VOLATILIDAD).orderBy("window").collect()

    nuevas = []
    for a in alertas:
        clave = (a["simbolo"], a["window"]["start"])
        if clave in ventanas_alertadas:
            continue
        ventanas_alertadas.add(clave)
        conteo_volatilidad[a["simbolo"]] = conteo_volatilidad.get(a["simbolo"], 0) + 1

        print(f"  !!! ALERTA DE VOLATILIDAD: {a['simbolo']} se movió {a['rango_pct']}% entre "
              f"{a['window']['start']:%H:%M:%S} y {a['window']['end']:%H:%M:%S} "
              f"(mín ${a['minimo']:,.2f} - máx ${a['maximo']:,.2f})")
        nuevas.append({"simbolo": a["simbolo"],
                       "inicio": a["window"]["start"],
                       "fin": a["window"]["end"],
                       "minimo": a["minimo"],
                       "maximo": a["maximo"],
                       "rango_pct": a["rango_pct"]})

    guardar_csv(nuevas, "volatilidad.csv", ["simbolo", "inicio", "fin", "minimo", "maximo", "rango_pct"])


# =====================================================================
# 3. Arrancar las dos consultas de streaming
# =====================================================================
# Los checkpoints (donde Spark anota por dónde va leyendo) van en una carpeta temporal
# NUEVA en cada ejecución. Antes usábamos siempre la misma carpeta y la borrábamos al
# arrancar, pero si por error se abrían dos detectores a la vez, el segundo le borraba
# los checkpoints al primero y se caían los dos (SparkConcurrentModificationException).
checkpoints = tempfile.mkdtemp(prefix="checkpoints_detector_")

consulta_medias = (precios.writeStream
                   .foreachBatch(procesar_medias)
                   .option("checkpointLocation", os.path.join(checkpoints, "medias"))
                   .trigger(processingTime="5 seconds")   # un micro-batch cada 5 segundos
                   .start())

consulta_volatilidad = (volatilidad.writeStream
                        .outputMode("update")
                        .foreachBatch(procesar_volatilidad)
                        .option("checkpointLocation", os.path.join(checkpoints, "volatilidad"))
                        .trigger(processingTime="5 seconds")
                        .start())

print("Esperando precios del topic 'precios'... (Ctrl+C para parar)")

duracion = int(sys.argv[1]) if len(sys.argv) > 1 else None
try:
    spark.streams.awaitAnyTermination(duracion)
except KeyboardInterrupt:
    pass

print("\nDeteniendo el detector...")
consulta_medias.stop()
consulta_volatilidad.stop()

print("\n=============== RESUMEN DE LA SESIÓN ===============")
print("Señales de cruce de medias:", conteo_senales)
print("Alertas de volatilidad por símbolo:", conteo_volatilidad)
print("Los detalles quedaron en la carpeta 'resultados/'. Para el análisis: python resumen.py")
spark.stop()
