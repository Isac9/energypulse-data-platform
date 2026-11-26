"""Job Spark Structured Streaming : mix de production en continu.

Kafka (energy.generation) -> agrégation par zone et par heure -> PostgreSQL

Notions clés (voir le cours) :
- event time      : on regroupe selon l'heure de la MESURE (period_utc),
                    pas selon l'heure d'arrivée du message.
- watermark       : on accepte des messages en retard jusqu'à 6 h ; au-delà,
                    la fenêtre est figée et Spark libère sa mémoire.
- dédoublonnage   : l'ingestion live renvoie les 6 dernières heures à chaque
                    passage ; sans dédoublonnage on additionnerait 6 fois la
                    même mesure.
- checkpoint      : Spark note où il en est (offsets Kafka + état). Après un
                    arrêt ou une panne, il reprend exactement là où il était.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
JDBC_URL = (f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
            f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB', 'energypulse')}")
JDBC_PROPS = {"user": os.environ.get("POSTGRES_USER", "energypulse"),
              "password": os.environ.get("POSTGRES_PASSWORD", "energypulse"),
              "driver": "org.postgresql.Driver"}
CHECKPOINT = os.environ.get("SPARK_CHECKPOINT", "/checkpoints/generation_mix")
TRIGGER = os.environ.get("SPARK_TRIGGER", "30 seconds")

RENEWABLE = ["WAT", "SUN", "SNB", "WND", "WNB", "GEO"]
FOSSIL = ["COL", "NG", "OIL"]
BA_CODES = ["CISO", "ERCO", "PJM", "NYIS"]
FUELS = ["COL", "NG", "NUC", "OIL", "WAT", "PS", "SUN", "SNB", "WND", "WNB",
         "BAT", "GEO", "OES", "UES", "OTH", "UNK"]

schema = StructType([
    StructField("event_id", StringType()),
    StructField("ba", StringType()),
    StructField("period_utc", StringType()),
    StructField("fuel", StringType()),
    StructField("mw", DoubleType()),
    StructField("source", StringType()),
])


def write_batch(batch_df, batch_id: int) -> None:
    """Appelé à chaque micro-batch : on ajoute les fenêtres mises à jour."""
    (batch_df.withColumn("batch_id", F.lit(batch_id))
             .write.jdbc(JDBC_URL, "marts.generation_mix_stream_log", mode="append",
                         properties=JDBC_PROPS))
    print(f"[batch {batch_id}] {batch_df.count()} fenêtre(s) écrite(s)", flush=True)


def main() -> None:
    spark = (SparkSession.builder.appName("energypulse-generation-mix")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", "energy.generation")
           .option("startingOffsets", "earliest")      # 1er lancement : tout l'historique du topic
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", 20000)      # limite la taille d'un micro-batch
           .load())

    # Garde-fou : Spark lit le topic brut, il ne profite donc pas de la
    # validation du consumer. Un seul message daté de 2099 ferait avancer le
    # watermark en 2099 et TOUTES les vraies données seraient ensuite jetées
    # comme « en retard » (bug réellement observé pendant les tests).
    events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select("e.*")
              .withColumn("event_time", F.to_timestamp("period_utc"))
              .where(F.col("event_id").isNotNull() & F.col("mw").isNotNull()
                     & F.col("ba").isin(BA_CODES)
                     & F.col("fuel").isin(FUELS)
                     & F.col("mw").between(-50000, 250000)
                     & (F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 2 HOURS"))))

    deduped = (events.withWatermark("event_time", "6 hours")
               .dropDuplicatesWithinWatermark(["event_id"]))

    def mw_where(fuels):
        return F.sum(F.when(F.col("fuel").isin(fuels), F.col("mw")).otherwise(0.0))

    mix = (deduped.groupBy(F.col("ba"), F.window("event_time", "1 hour"))
           .agg(F.sum("mw").alias("total_mw"),
                F.sum(F.greatest(F.col("mw"), F.lit(0.0))).alias("gross_mw"),  # sans le stockage qui se recharge
                mw_where(RENEWABLE).alias("renewable_mw"),
                mw_where(["SUN", "SNB"]).alias("solar_mw"),
                mw_where(["WND", "WNB"]).alias("wind_mw"),
                mw_where(["WAT"]).alias("hydro_mw"),
                mw_where(FOSSIL).alias("fossil_mw"),
                mw_where(["NUC"]).alias("nuclear_mw"),
                F.count("*").alias("event_count"))
           .select("ba",
                   F.col("window.start").alias("window_start"),
                   F.col("window.end").alias("window_end"),
                   "total_mw", "gross_mw", "renewable_mw", "solar_mw", "wind_mw", "hydro_mw",
                   "fossil_mw", "nuclear_mw",
                   F.round(F.col("renewable_mw") / F.col("gross_mw"), 4).alias("renewable_share"),
                   "event_count"))

    query = (mix.writeStream
             .outputMode("update")          # n'émet que les fenêtres modifiées
             .foreachBatch(write_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
