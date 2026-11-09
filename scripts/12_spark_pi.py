"""Partie 12 — Un topic pour les groupes Π (Spark Structured Streaming).

Lit en continu ferme.eoliennes, calcule pour chaque mesure
    Π1 = P / (ρ V³ D²)   et   Π2 = ω D / V
à partir du ρ (rho_kgm3) ajouté par le pont de la partie 04, et écrit le résultat dans
ferme.eoliennes.pi avec le nom de l'éolienne comme clé.
Usage (conteneur app) : python scripts/12_spark_pi.py
"""
from pyspark.sql import SparkSession, functions as F, types as T

import commun

SCHEMA = T.StructType([
    T.StructField("eolienne", T.StringType()),
    T.StructField("zone", T.StringType()),
    T.StructField("horodatage", T.StringType()),
    T.StructField("seq", T.LongType()),
    T.StructField("diametre_m", T.DoubleType()),
    T.StructField("vent_ms", T.DoubleType()),
    T.StructField("rotation_rads", T.DoubleType()),
    T.StructField("puissance_w", T.DoubleType()),
    T.StructField("rho_kgm3", T.DoubleType()),
])


def calculer_pi(mesures):
    """Ajoute pi1 et pi2 ; try_divide renvoie null (au lieu d'une erreur ANSI) si V = 0 ou champ absent."""
    v, d = F.col("vent_ms"), F.col("diametre_m")
    return (mesures
            .withColumn("pi1", F.try_divide(F.col("puissance_w"), F.col("rho_kgm3") * F.pow(v, 3) * F.pow(d, 2)))
            .withColumn("pi2", F.try_divide(F.col("rotation_rads") * d, v)))


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("12_spark_pi").master("local[2,4]")  # 4 tentatives par tâche : survit à une indisponibilité passagère de Kafka
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    brut = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", commun.KAFKA_BOOTSTRAP)
            .option("subscribe", commun.TOPIC_MESURES)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")      # tolère un topic vidé (partie 04) ou des offsets expirés
            .load())
    mesures = (brut.select(F.col("key").cast("string").alias("cle"),
                           F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
               .where(F.col("m").isNotNull())        # message JSON illisible : ignoré
               .select("cle", "m.*"))
    sortie = calculer_pi(mesures).select(
        F.coalesce("cle", "eolienne").alias("key"),
        F.to_json(F.struct("eolienne", "zone", "horodatage", "seq", "pi1", "pi2")).alias("value"))

    requete = (sortie.writeStream.format("kafka")
               .option("kafka.bootstrap.servers", commun.KAFKA_BOOTSTRAP)
               .option("topic", commun.TOPIC_PI)
               .option("checkpointLocation", str(commun.RACINE / "checkpoints" / "12_spark_pi"))
               .trigger(processingTime="5 seconds")
               .start())
    requete.awaitTermination()
