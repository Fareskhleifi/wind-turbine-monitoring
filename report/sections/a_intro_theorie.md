# Introduction

## Contexte

Une ferme de quinze éoliennes (E1 à E15), réparties sur quatre zones du site « grand-pré » (vallée, champs, colline, plateau), publie chaque seconde une mesure par éolienne sur un broker MQTT. Chaque mesure contient le diamètre du rotor, la vitesse du vent, la température et la pression de l'air, la vitesse de rotation du rotor et la puissance électrique produite.

Les éoliennes n'ont pas toutes la même taille (diamètres de 3 à 20 m) et ne voient pas le même vent : comparer directement leurs puissances n'a donc pas de sens. Le TD propose de construire, par analyse dimensionnelle, une représentation sans dimension du fonctionnement d'une éolienne, puis d'en déduire un **abaque de fonctionnement normal Φ** à partir de cinq éoliennes de référence (E1 à E5). Cet abaque sert ensuite de jumeau numérique : on compare en continu chaque éolienne à ce qu'elle « devrait » produire.

## Problématique et objectifs

La question centrale est la suivante : **comment détecter automatiquement, sur un flux continu de mesures, les éoliennes en panne, les défauts de capteur et les données manquantes, alors que les machines sont de tailles et d'environnements différents ?**

Les objectifs du TD, repris dans les dix-neuf parties de l'énoncé, sont :

- acheminer les mesures MQTT vers Kafka sans perte, puis les enrichir de la masse volumique de l'air (parties 01 à 04) ;
- établir par le théorème de Vaschy-Buckingham deux nombres sans dimension Π1 et Π2 (parties 05 à 07) ;
- construire et valider l'abaque Π1 = Φ(Π2) et identifier l'éolienne de référence en panne (parties 08 à 11) ;
- surveiller toute la ferme en continu avec Spark Structured Streaming, un système d'alertes et un tableau de bord (parties 12 à 17) ;
- étudier la robustesse de la chaîne face aux données manquantes et la fiabilité statistique de l'abaque (parties 18 et 19).

## Conventions du rapport

Toutes les valeurs numériques citées proviennent des mesures réelles reçues du broker du TD le 9 novembre 2026 (heure UTC des capteurs), sauf la partie 18, où les éoliennes EX1 à EX5 sont **simulées** à partir des mesures réelles, comme le demande l'énoncé. Les sorties brutes des scripts sont conservées dans `results/logs`. Les identifiants de connexion au broker ne figurent volontairement pas dans ce rapport.

# Architecture générale

## Vue d'ensemble

La chaîne réalisée est la suivante :

```
Broker MQTT (WebSocket + TLS, port 443)
  topics ferme/grand-pre/<zone>/<Ei>/mesures  -- 15 mesures/s
        |
        v   04_pont_masse_volumique.py  (paho-mqtt -> confluent-kafka, ajout de rho_kgm3)
Kafka  ferme.eoliennes      (6 partitions, clé = Ei, valeur = mesure JSON)
        |
        v   12_spark_pi.py  (Spark Structured Streaming)
Kafka  ferme.eoliennes.pi   (6 partitions, clé = Ei, valeur = {horodatage, seq, pi1, pi2})
        |                                   |
        |   13 / 16_surveiller*.py          |   17_spark_indicateurs.py (fenêtres d'1 min, watermark 2 min)
        v                                   v
Kafka  ferme.alertes  (changements d'état)   Kafka  ferme.tableau  (1 message / éolienne / minute)
        |
        +--> 15_tableau_r.py : tableau de bord Streamlit (r en direct, rafraîchi toutes les 5 s)

Scripts « batch » : 03, 08, 10, 11, 19 relisent ferme.eoliennes depuis le début -> figures et phi.json
```

L'ensemble tourne dans les conteneurs définis par le `docker-compose.yml` fourni : un broker Kafka 3.9.1 en mode KRaft (sans ZooKeeper), Kafka UI, et un conteneur `app` (Python 3.12, Java 21, PySpark 4.0.1 avec le connecteur Kafka préinstallé) dans lequel s'exécutent tous les scripts.

## Rôle de chaque brique

- **MQTT** est le protocole de publication des capteurs : léger, orienté *publish/subscribe*, adapté à des objets connectés. Il ne conserve pas l'historique : un message non lu au moment de sa publication est perdu pour l'abonné.
- **Kafka** joue le rôle de journal durable et rejouable. Le pont recopie chaque mesure dans `ferme.eoliennes` ; tous les traitements en aval peuvent alors relire les mesures depuis le début, à leur rythme, et indépendamment les uns des autres. La clé de message (nom de l'éolienne) garantit que toutes les mesures d'une éolienne vont dans la même partition, donc dans l'ordre.
- **Python** assure l'ingestion (pont MQTT → Kafka), les analyses ponctuelles (graphiques, construction de Φ), la surveillance par fenêtre glissante et le tableau de bord.
- **Spark Structured Streaming** réalise les calculs continus et parallélisables : calcul des groupes Π pour chaque mesure (partie 12) et agrégation par minute selon l'heure des capteurs, avec gestion des retards (partie 17).
- **L'analyse dimensionnelle** fournit le modèle : elle réduit cinq grandeurs physiques à deux nombres sans dimension, ce qui permet de comparer sur une même courbe des éoliennes de tailles différentes soumises à des vents et à des densités d'air différents.

## Organisation du code

Les scripts portent les noms imposés par l'énoncé (`scripts/01_mqtt.py` … `scripts/19_convergence.py`). Le code partagé est regroupé dans `scripts/commun.py` : lecture du fichier `.env`, création du client MQTT (WebSocket, TLS, chemin `/mqtt`), pont MQTT → Kafka, relecture complète d'un topic, calcul de ρ, de Π1 et Π2, construction de Φ et règles d'état. Ce regroupement garantit que les parties 10, 11, 13, 15, 16 et 19 utilisent exactement les mêmes définitions. Les tests hors réseau (`tests/test_calculs.py`) vérifient ces fonctions, et `tests/test_mqtt_connection.py` vérifie la connexion réelle au broker.
