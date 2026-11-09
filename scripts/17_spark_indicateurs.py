"""Partie 17 — Indicateurs par minute avec Spark Structured Streaming.

Lit ferme.eoliennes.pi, calcule r = Π1 / Φ(Π2) pour chaque mesure (Φ lu dans phi.json), puis pour chaque
éolienne et chaque minute (heure des capteurs = champ horodatage, watermark de 2 minutes) :
nombre de mesures, médiane de r, nombre de mesures suspectes (r > 2), nombre hors abaque et état.
Publie un message par éolienne et par minute dans ferme.tableau (clé = éolienne).
États (mêmes règles que les parties 13 et 16, appliquées à la minute) :
HORS_ABAQUE > CAPTEUR > SOUS_PRODUCTION > SUR_PRODUCTION > OK.
"""
from itertools import chain

from pyspark.sql import SparkSession, functions as F, types as T

import commun

SCHEMA_PI = T.StructType([
    T.StructField("eolienne", T.StringType()),
    T.StructField("horodatage", T.StringType()),
    T.StructField("seq", T.LongType()),
    T.StructField("pi1", T.DoubleType()),
    T.StructField("pi2", T.DoubleType()),
])


def indicateurs(mesures, phi):
    """mesures : colonnes eolienne, t (timestamp capteur), pi1, pi2 -> agrégats par éolienne et minute."""
    table_phi = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in phi.items()))
    tranche = F.floor(F.col("pi2") / commun.LARGEUR_TRANCHE).cast("int")
    avec_r = (mesures.withColumn("phi", F.try_element_at(table_phi, tranche))
              .withColumn("r", F.try_divide(F.col("pi1"), F.col("phi"))))
    agr = (avec_r.withWatermark("t", "2 minutes")
           .groupBy("eolienne", F.window("t", "1 minute"))
           .agg(F.count(F.lit(1)).alias("nb_mesures"),
                F.percentile_approx("r", 0.5, 10000).alias("mediane_r"),
                F.sum(F.when(F.col("r") > commun.SEUIL_CAPTEUR, 1).otherwise(0)).alias("nb_suspects"),
                F.sum(F.when(F.col("phi").isNull(), 1).otherwise(0)).alias("nb_hors_abaque")))
    etat = (F.when(F.col("nb_hors_abaque") > F.col("nb_mesures") / 2, "HORS_ABAQUE")
            .when(F.col("nb_suspects") > 0, "CAPTEUR")
            .when(F.col("mediane_r") < 1 - commun.MARGE, "SOUS_PRODUCTION")
            .when(F.col("mediane_r") > 1 + commun.MARGE, "SUR_PRODUCTION")
            .otherwise("OK"))
    return agr.select("eolienne", F.col("window.start").alias("debut"), F.col("window.end").alias("fin"),
                      "nb_mesures", "mediane_r", "nb_suspects", "nb_hors_abaque", etat.alias("etat"))


if __name__ == "__main__":
    phi = commun.charger_phi()
    spark = (SparkSession.builder.appName("17_spark_indicateurs").master("local[2,4]")  # 4 tentatives par tâche : survit à une indisponibilité passagère de Kafka
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.port", "4041")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    brut = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", commun.KAFKA_BOOTSTRAP)
            .option("subscribe", commun.TOPIC_PI)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load())
    mesures = (brut.select(F.col("key").cast("string").alias("cle"),
                           F.from_json(F.col("value").cast("string"), SCHEMA_PI).alias("m"))
               .select(F.coalesce("cle", "m.eolienne").alias("eolienne"),
                       F.try_to_timestamp(F.col("m.horodatage"), F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")).alias("t"),
                       "m.pi1", "m.pi2")
               .where(F.col("t").isNotNull()))     # sans heure capteur, la mesure ne peut être placée dans une minute
    sortie = indicateurs(mesures, phi).select(F.col("eolienne").alias("key"),
                                               F.to_json(F.struct("*")).alias("value"))
    requete = (sortie.writeStream.format("kafka").outputMode("append")
               .option("kafka.bootstrap.servers", commun.KAFKA_BOOTSTRAP)
               .option("topic", commun.TOPIC_TABLEAU)
               .option("checkpointLocation", str(commun.RACINE / "checkpoints" / "17_spark_indicateurs"))
               .trigger(processingTime="10 seconds")
               .start())
    requete.awaitTermination()
