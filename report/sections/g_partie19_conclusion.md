# Fiabilité de l'abaque (partie 19)

## Partie 19 — Une seule éolienne suffit-elle ?

### Objectif

Refaire le graphique de la partie 08 pour E1 seule, en y superposant l'abaque construit sur E1 seule et l'abaque de référence (E1 à E5) ; puis étudier la convergence de Φ en fonction du nombre N de mesures utilisées, d'abord pour E1 seule, puis pour E1 à E5.

### Méthode et implémentation

`19_convergence.py` réutilise `points_par_eolienne` (partie 08) et `construire_phi` (partie 10), avec la même règle : tranches de 0,5, médiane, au moins 20 mesures, points d'arrêt exclus. Les mesures utiles sont prises **dans l'ordre chronologique** (tri sur `seq`) : pour E1 à E5, les N premières mesures sont donc celles des N/5 premières secondes de fonctionnement des cinq éoliennes. Pour N = 30, 60, 90, … jusqu'à toutes les mesures, on construit Φ_N et on calcule :

$$écart(N) = médiane sur les tranches communes de | Φ_N(k) / Φ_réf(k) − 1 |$$

On trace aussi le nombre de tranches retenues, qui mesure la couverture du domaine. L'abaque de référence est construit sur toutes les mesures de E1 à E5 disponibles au moment de l'exécution (09:03 UTC, 7519 mesures utiles, 33 tranches). Pour E1 à E5, la dernière valeur de la courbe est donc nulle par construction : Φ_N y coïncide avec Φ_réf.

### Résultats

![E1 seule : nuage Π1(Π2) de E1, abaque construit sur E1 seule et abaque de référence E1 à E5](../results/graphiques/phi_e1.png)

![Convergence de Φ : écart médian à l'abaque de référence (en haut, échelle logarithmique) et nombre de tranches retenues (en bas) en fonction du nombre N de premières mesures utilisées](../results/graphiques/convergence.png)

| N (mesures utiles) | E1 seule : écart | E1 seule : tranches | E1 à E5 : écart | E1 à E5 : tranches |
|---|---|---|---|---|
| 270 | 1,98 % | 2 | 0,36 % | 3 |
| 510 | 0,86 % | 13 | 0,97 % | 12 |
| 990 | 0,93 % | 19 | 0,39 % | 18 |
| 1440 | 1,00 % | 22 | 0,29 % | 22 |
| 3000 | — | — | 0,23 % | 28 |
| 6000 | — | — | 0,10 % | 31 |
| 7519 (tout) | — | — | 0 (référence) | 33 |

Avec toutes ses mesures (1445), E1 seule donne un abaque de 22 tranches (Π2 de 8 à 19,5) dont l'écart médian à la référence est de 1,0 %. La référence en compte 33 (Π2 de 6 à 22,5).

À titre de vérification complémentaire, chaque éolienne de référence a aussi servi seule à construire un abaque, comparé à la référence (exécution vers 09:50 UTC sur les mesures disponibles à ce moment, `results/logs/19_phi_par_eolienne.log`) : E1, E3, E4 et E5 donnent chacune un Φ supérieur de **+1,2 %** à la référence, tandis que **E2 seule donne un Φ inférieur de 14,2 %**.

### Cela aurait-il pu marcher en ne prenant qu'une éolienne ?

**Oui, mais par chance, et avec un domaine réduit.** E1 étant saine, son abaque se superpose à la référence à 1 % près (figure 9). Il est même un peu plus juste, puisqu'il ne contient pas le biais de E2. Deux problèmes subsistent :

- **on ne peut pas savoir si l'éolienne choisie est saine.** Si l'on avait pris E2, l'abaque aurait été faux de 14 % : E2 aurait été jugée normale et toutes les éoliennes saines auraient paru en SUR_PRODUCTION (r ≈ 1,17). Avec une seule machine, rien ne permet de détecter l'erreur, car il n'existe aucun point de comparaison ;
- **la couverture est plus faible** : 22 tranches au lieu de 33. Les régimes extrêmes (Π2 < 8 et Π2 > 19,5) sont trop rares sur une seule machine pour atteindre 20 mesures par tranche. Une éolienne surveillée qui fonctionnerait dans ces régimes serait déclarée HORS_ABAQUE.

### Quel avantage y a-t-il à avoir pris plusieurs éoliennes ?

- **Robustesse aux pannes** : avec cinq machines, une machine défaillante ne représente que 20 % des points et la médiane l'élimine presque complètement (biais résiduel de 1 % au lieu de 14 %). Elle devient en outre *détectable* par comparaison avec les autres (partie 11).
- **Couverture du domaine** : à durée égale, cinq éoliennes collectent cinq fois plus de mesures et voient des régimes plus variés (deux zones, trois diamètres) : 33 tranches au lieu de 22.
- **Rapidité** : pour obtenir 22 tranches, il faut N ≈ 1440 mesures dans les deux cas. Pour E1 seule, cela représente environ 24 minutes de fonctionnement ; pour E1 à E5, environ 5 minutes, puisque les cinq machines mesurent en parallèle.
- **Validation croisée** : l'accord entre les abaques individuels des éoliennes saines (+1,2 % chacune) démontre la propriété de similitude que suppose toute la méthode.

### À partir de combien de mesures l'abaque est-il stable ?

Il faut distinguer deux aspects.

- **La valeur de Φ dans les tranches couvertes** est stable très tôt. Dès qu'une tranche atteint 20 mesures, sa médiane est à moins de 2 % de la référence. L'écart médian est inférieur à 2 % dès N = 270 pour E1 seule et N = 240 pour E1 à E5, et il ne remonte jamais au-dessus. Pour E1 à E5, il décroît ensuite régulièrement : 0,4 % vers N = 1000, 0,2 % vers N = 3000, 0,1 % vers N = 6000. Pour E1 seule, il plafonne à environ 1 % : ce plancher n'est pas un manque de données, c'est l'écart systématique entre une éolienne saine et une référence légèrement biaisée par E2.
- **La couverture du domaine** converge beaucoup plus lentement, et c'est elle qui limite. Pour E1 à E5, il faut N ≈ 1140 mesures pour 20 tranches, N ≈ 4890 pour 30 tranches et N ≈ 7110 pour les 33 tranches de la référence. Les tranches des bords de la courbe sont rares, et la couverture dépend autant des conditions de vent rencontrées que du nombre de mesures.

En résumé, avec E1 à E5, **l'abaque est stable à mieux que 0,5 % sur la partie centrale (Π2 de 9 à 19) à partir d'environ 1000 à 1500 mesures**, soit quelques minutes de fonctionnement des cinq éoliennes. Il faut environ 7000 mesures, soit près d'une demi-heure de mesures, pour couvrir tout le domaine observé. La règle des 20 mesures par tranche est ce qui garantit cette stabilité : aucune tranche n'est retenue tant que sa médiane n'est pas fiable.

# Conclusion générale

## Synthèse du travail

Les dix-neuf parties du TD ont été réalisées sur les **données réelles** du broker de la ferme, le 9 novembre 2026, de 08:13 à 09:50 UTC environ. Les seules exceptions sont les éoliennes EX1 à EX5 de la partie 18, simulées à partir de ces mesures comme le demande l'énoncé. La chaîne complète MQTT → Kafka → Spark → surveillance → tableau de bord a fonctionné en continu, y compris après une interruption réelle de 25 minutes, absorbée grâce aux checkpoints Spark et à la reconnexion automatique du pont.

## Résultats importants

- L'analyse dimensionnelle (rang 3, n − r = 2) donne **Π1 = P/(ρV³D²)** et **Π2 = ωD/V**, c'est-à-dire, à un facteur près, le coefficient de puissance et la vitesse réduite en bout de pale. Les cinq éoliennes de référence, de 3 à 8 m de diamètre, se rangent sur **une seule courbe en cloche** de maximum Π1 ≈ 0,179 (C_p ≈ 0,455) pour Π2 ≈ 14 (λ ≈ 7).
- L'abaque Φ (33 tranches de Π2 entre 6 et 22,5) révèle que **E2 est en panne** (−14 %, 90 % des mesures hors marge), sans que sa présence ait faussé l'abaque de plus de 1 %, grâce à la médiane.
- La surveillance continue met en évidence trois éoliennes en **sous-production** : **E7** (−35 %, erratique), **E10** (−24 %, constante) et **E12** (−5 à −15 %, intermittente). Elle révèle aussi des **mesures fausses** sur E7, E12 et E6, cette dernière présentant un défaut parfaitement périodique (30 s toutes les 7 minutes) que seule la vue temporelle du tableau de bord rend évident. E14 (−7 %) reste sous le seuil.
- Les états HORS_ABAQUE observés correspondent tous à des arrêts normaux par vent faible (moins de 3 m/s) ou trop fort (plus de 25 m/s).
- Les indicateurs Spark par minute confirment ces diagnostics et sont publiés environ 2 min 20 s après la fin de chaque minute, conformément au watermark de 2 minutes.
- La simulation des données manquantes montre que la chaîne est robuste (aucun plantage côté flux), mais **silencieuse** face aux données absentes. Elle a aussi révélé et permis de corriger un vrai défaut du tableau de bord.

## Apports de l'architecture

Kafka découple la collecte des traitements : les mesures sont conservées, relisibles depuis le début et partagées entre des consommateurs indépendants (scripts d'analyse, surveillance, Spark, tableau de bord). Spark Structured Streaming apporte le calcul continu, le temps d'événement, la gestion des retards et la reprise sur incident grâce aux checkpoints. Regrouper le code commun dans `commun.py` garantit que tous les scripts appliquent exactement les mêmes définitions de Π, de Φ et des états.

## Intérêt de l'analyse dimensionnelle

Elle remplace un problème à cinq variables, avec des machines incomparables, par une relation universelle entre deux nombres. Un modèle construit sur cinq éoliennes de 3 à 8 m s'est ainsi révélé valable pour des éoliennes jusqu'à 20 m, sur d'autres zones, avec un air jusqu'à 12 % moins dense. Elle fournit en outre des garde-fous physiques (limite de Betz) qui justifient objectivement le seuil r > 2 de détection des mesures fausses.

## Limites rencontrées

- Durée d'observation courte (environ 1 h 20 de mesures, dont un trou de 25 min) : une seule situation météorologique, sans effet saisonnier ni vieillissement.
- Abaque légèrement biaisé (+1 %) par E2 ; effets d'échelle (nombre de Reynolds), turbulence et relief non modélisés.
- Le seuil de ±10 % laisse passer les dégradations faibles (E14) ; la médiane sur 60 mesures laisse passer les défauts courts.
- Absence de détection explicite des données manquantes (partie 18).
- La purge du topic `ferme.eoliennes` demandée en partie 04 a été réalisée après coup, et non avant le lancement du pont enrichi (voir partie 04) ; elle est sans effet sur les résultats.

## Améliorations possibles

- Reconstruire Φ sans E2, puis le mettre à jour périodiquement avec les seules éoliennes jugées saines.
- Ajouter des états `MUETTE` et `INCOMPLET` (fraîcheur, sauts de `seq`, champs manquants) et un topic de rejets.
- Compléter la médiane par des indicateurs de tendance (médiane glissante sur 1 h, test de rupture) pour détecter les dérives lentes comme celle de E14, et par une détection de périodicité pour les défauts du type de E6.
- Utiliser une session MQTT persistante pour ne rien perdre pendant un arrêt du pont, et un *watchdog* qui relance automatiquement les jobs.

# Annexes

## Annexe A — Arborescence du projet

```
TD_Ferme_Eolienne/
├── README.md, docker-compose.yml, Dockerfile, requirements.txt, .env.exemple, .gitignore
├── td-ferme-eolienne-enonce.pdf
├── scripts/      commun.py, 01_mqtt.py … 19_convergence.py (15 scripts, noms imposés)
├── data/         topics.jsonl, mesures.jsonl (partie 01)
├── results/
│   ├── graphiques/   puissance_vent.png, pi1_pi2.png, phi.png, r_par_eolienne.png, phi_e1.png, convergence.png
│   ├── tableaux/     phi.json, phi_detail.csv, r_par_eolienne.csv, convergence.csv,
│   │                 ferme_alertes.jsonl, ferme_tableau.jsonl
│   ├── captures/     dashboard_overview.png, dashboard_wind_farm.png, dashboard_temps_reel.png,
│   │                 dashboard_alertes.png, dashboard_qualite_donnees.png (interface finale),
│   │                 tableau_de_bord_E6_E7.png, tableau_de_bord_EX_silence.png (première version),
│   │                 kafka_ui_topics.png, spark_ui_12_streaming.png
│   └── logs/         sorties réelles de chaque script et scripts d'analyse complémentaires
├── report/       Compte_Rendu_TD_Ferme_Eolienne.docx / .md, sections/, construire_docx.js
└── tests/        test_mqtt_connection.py, test_calculs.py
```

## Annexe B — Commandes d'exécution

```
docker compose up -d --build
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic ferme.eoliennes.pi --partitions 6 --replication-factor 1
docker compose exec app python tests/test_mqtt_connection.py 10
docker compose exec app python scripts/01_mqtt.py 30
docker compose exec app python scripts/04_pont_masse_volumique.py      # terminal 1
docker compose exec app python scripts/12_spark_pi.py                  # terminal 2
docker compose exec app python scripts/10_phi_construire.py            # après 30 à 60 min
docker compose exec app python scripts/16_surveiller_capteur.py        # terminal 3
docker compose exec app python scripts/17_spark_indicateurs.py         # terminal 4
docker compose exec app streamlit run scripts/15_tableau_r.py --server.port 8501 --server.address 0.0.0.0
docker compose exec app python tests/test_calculs.py
```

Le README détaille l'ensemble des commandes, partie par partie, ainsi que les commandes de vérification de Kafka (`kafka-topics.sh --describe`, `kafka-get-offsets.sh`, `kafka-console-consumer.sh`).

## Annexe C — Dépendances

| Composant | Version | Rôle |
|---|---|---|
| Python | 3.12 (image `python:3.12-slim-trixie`) | langage des scripts |
| Java | OpenJDK 21 | exécution de Spark |
| Apache Kafka | 3.9.1 (KRaft) | journal des mesures |
| Kafka UI (kafbat) | 1.5.0 | administration des topics |
| pyspark | 4.0.1 (+ spark-sql-kafka-0-10_2.13 4.0.1) | traitements en flux |
| confluent-kafka | 2.16.0 | producteurs et consommateurs Python |
| paho-mqtt | 2.1.0 | client MQTT (WebSocket + TLS) |
| numpy / pandas / pyarrow | 2.5.4 / 2.3.3 / 26.0.0 | calcul et tableaux |
| matplotlib | 3.9.2 | graphiques |
| sympy | 1.14.0 | vérification du rang et des groupes Π |
| streamlit / altair | 1.40.2 / 5.5.0 (fourni avec Streamlit) | tableau de bord et ses graphiques |
| python-dotenv / certifi | 1.0.1 / 2024.8.30 | configuration et certificats TLS |

## Annexe D — Tests et preuves d'exécution

| Test | Contenu | Résultat |
|---|---|---|
| `tests/test_mqtt_connection.py` | DNS, TCP 443, TLS, WebSocket `/mqtt`, authentification, réception | réussi : 150 messages en 10 s sur 15 topics |
| `tests/test_calculs.py` (8 tests) | ρ standard ; pont ρ et messages invalides ; matrice dimensionnelle, rang 3 et noyau (sympy) ; Π et mesures incomplètes ; construction de Φ (seuil de 20) ; règles d'état et priorités (y compris NaN) ; simulation de la partie 18 ; fidélité du tableau de bord aux calculs (état, médiane sur 60, sauts de seq, jauge de Betz) | 8/8 réussis |
| Pont 02, arrêt par SIGINT | comparaison des mesures transmises et des offsets Kafka | 2702 = 2702, 0 perte |
| Pont 04, arrêt par SIGINT en fin de session | messages en file au moment de l'arrêt | 91 904 transmises, 0 non accusée |
| Purge de la partie 04 | suppression des mesures sans `rho_kgm3` en tête de partition, puis relecture du topic | 3122 supprimées, 0 restante |
| Journaux `results/logs/` | sortie brute de chaque script exécuté | conservés |

## Annexe E — Extrait de `phi.json`

```
{"definition": "phi = mediane de Pi1 par tranche de Pi2 de largeur 0.5 ; E1-E5 ; tranches de moins de 20 mesures et points (0,0) exclus",
 "pi1": "P / (rho V^3 D^2)", "pi2": "omega D / V", "largeur_tranche": 0.5, "min_mesures": 20,
 "tranches": [{"tranche": 12, "pi2_min": 6.0, "pi2_max": 6.5, "n": 51, "phi": 0.0304…},
              …,
              {"tranche": 28, "pi2_min": 14.0, "pi2_max": 14.5, "n": 485, "phi": 0.1786…},
              …,
              {"tranche": 44, "pi2_min": 22.0, "pi2_max": 22.5, "n": 20, "phi": 0.0307…}]}
```

## Annexe F — Interface finale du tableau de bord

Captures réelles de l'interface « Wind Turbine Monitoring » (version finale de `15_tableau_r.py`), prises le 9 novembre 2026 sur les données du broker. Toutes les valeurs affichées sont lues dans Kafka ; les états sont ceux des règles des parties 13 et 16.

![Vue Overview (12:41 UTC) : indicateurs clés de la ferme, carte de E6 (état, médiane de r, compteurs, rotation mesurée), écart à l'abaque en temps réel, grandeurs physiques mesurées, anomalies actives et médiane de r par éolienne ; le trou de la courbe correspond à une mise en veille du poste](../results/captures/dashboard_overview.png)

![Vue Real-time Monitoring (11:11 UTC) : r de E6 (épisodes périodiques de mesures fausses) et de E7 (sous-production dispersée) sur 90 minutes, puis grandeurs physiques réelles de E15](../results/captures/dashboard_temps_reel.png)

![Vue Data Quality (11:15 UTC) : fraîcheur et complétude des données des 15 éoliennes réelles, puis éoliennes simulées EX1 à EX5 séparées, avec leurs défauts mesurés (mesures perdues, champs absents, silence)](../results/captures/dashboard_qualite_donnees.png)
