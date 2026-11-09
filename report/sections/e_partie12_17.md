# Surveillance de la ferme en continu (parties 12 à 17)

## Partie 12 — Un topic pour les groupes Π

### Objectif

Créer le topic `ferme.eoliennes.pi` (6 partitions) et un job Spark Structured Streaming qui lit en continu `ferme.eoliennes`, calcule Π1 et Π2 pour chaque mesure à partir du ρ ajouté en partie 04, et les écrit dans `ferme.eoliennes.pi` avec le nom de l'éolienne comme clé.

### Méthode et implémentation

Le topic a été créé avec l'outil `kafka-topics.sh` du conteneur Kafka, ce qui est équivalent au bouton « Add a Topic » de Kafka UI : `--topic ferme.eoliennes.pi --partitions 6 --replication-factor 1` (la création automatique des topics est désactivée dans le `docker-compose.yml`).

Le job `12_spark_pi.py` (PySpark 4.0.1, connecteur `spark-sql-kafka-0-10_2.13:4.0.1` préinstallé dans l'image) :

1. lit `ferme.eoliennes` en flux (`readStream.format("kafka")`, `startingOffsets=earliest`) ;
2. décode la valeur avec `from_json` et un schéma explicite (`eolienne`, `zone`, `horodatage`, `seq`, `diametre_m`, `vent_ms`, `rotation_rads`, `puissance_w`, `rho_kgm3`) ; un message illisible donne une structure nulle et est filtré ;
3. calcule `pi1 = try_divide(puissance_w, rho_kgm3 · vent_ms³ · diametre_m²)` et `pi2 = try_divide(rotation_rads · diametre_m, vent_ms)`. Spark 4 active par défaut le mode ANSI, dans lequel une division par zéro lève une erreur et arrêterait le flux ; `try_divide` renvoie null à la place, de même qu'un champ absent ;
4. écrit dans `ferme.eoliennes.pi` la clé (clé d'origine, ou à défaut le champ `eolienne`) et la valeur `to_json(struct(eolienne, zone, horodatage, seq, pi1, pi2))` ;
5. conserve sa progression dans un *checkpoint* (`checkpoints/12_spark_pi`), avec un déclenchement toutes les 5 s.

Exemple de message produit : `E5 → {"eolienne":"E5","zone":"champs","horodatage":"2026-11-09T08:31:45.000+00:00","seq":43726,"pi1":0.0,"pi2":0.0}` (éolienne à l'arrêt à ce moment-là).

### Résultats

Le job a tourné en continu à partir de 08:31 UTC : l'interface Spark indique une entrée moyenne de 15,5 mesures/s (les quinze éoliennes) et une capacité de traitement de 42,5 mesures/s. Le topic contenait 12 108 messages Π à 08:39 et plus de 52 000 en fin de session.

![Interface Spark (port 4040) : requête Structured Streaming du job 12 active](../results/captures/spark_ui_12_streaming.png)

![Kafka UI : les cinq topics du TD, dont ferme.eoliennes.pi à 6 partitions](../results/captures/kafka_ui_topics.png)

**Incident réel et correction.** À 09:08 UTC, le poste s'est mis en veille pendant 25 minutes. À la reprise, le job 12 s'était arrêté sur `TimeoutException: Timeout of 60000ms expired before the position for partition ferme.eoliennes-3 could be determined` : en mode `local`, Spark abandonne la requête au premier échec de tâche. Le job a été relancé : grâce au checkpoint, il a repris exactement au dernier offset traité, sans doublon ni trou. Pour la suite, le maître Spark est configuré en `local[2,4]` (4 tentatives par tâche), ce qui permet de survivre à une indisponibilité passagère de Kafka. Les mesures publiées sur MQTT pendant la veille n'ont pas pu être reçues : **les données réelles présentent un trou de 09:08 à 09:33 UTC**, visible sur les graphiques temporels.

### Pourquoi faire ce calcul dans un job séparé plutôt que dans le pont ?

- **Séparation des responsabilités.** Le pont a une seule mission critique : ne perdre aucune mesure. Plus il fait de calculs, plus il risque de planter ou de prendre du retard sur MQTT, qui ne conserve pas les messages. ρ est une simple conversion de la mesure brute ; Π1 et Π2 relèvent déjà de la modélisation.
- **Rejouabilité et évolutivité.** `ferme.eoliennes` conserve les mesures brutes enrichies. Si l'on change la définition des Π (par exemple λ = ωR/V au lieu de ωD/V), il suffit de relancer le job depuis le début du topic, sans rien demander aux capteurs. Un calcul fait dans le pont serait figé dans des messages déjà publiés.
- **Passage à l'échelle et tolérance aux pannes.** Spark répartit le calcul sur les 6 partitions et reprend exactement là où il s'était arrêté grâce à son checkpoint, ce qu'illustre l'incident ci-dessus ; un pont Python est un processus unique.
- **Mutualisation.** Les parties 13, 15, 16 et 17 lisent toutes `ferme.eoliennes.pi` : le calcul est fait une seule fois au lieu d'être recopié dans chaque consommateur, ce qui supprime le risque de définitions divergentes.

## Partie 13 — Surveiller E6 à E15

### Objectif

Faire tourner simultanément la chaîne MQTT → `ferme.eoliennes` → `ferme.eoliennes.pi`, lire `ferme.eoliennes.pi` en continu, garder les 60 dernières mesures de chaque éolienne de E6 à E15 et, toutes les 10 s, lui attribuer un état ; publier une alerte dans `ferme.alertes` à chaque changement d'état.

### Méthode et implémentation

Le pont 04 et le job 12 tournaient chacun dans leur processus. `13_surveiller.py` consomme `ferme.eoliennes.pi` et range les couples (Π1, Π2) dans une file de longueur fixe (`deque(maxlen=60)`) par éolienne. Toutes les 10 s, la fonction partagée `commun.etat` applique les règles de l'énoncé, dans cet ordre de priorité :

| État | Condition sur les 60 dernières mesures |
|---|---|
| HORS_ABAQUE | plus de 30 mesures (plus de la moitié) ont un Π2 absent de la table Φ (tranche non retenue, ou Π2 manquant) |
| SOUS_PRODUCTION | médiane de r < 0,9 (calculée sur les mesures dont r est défini) |
| SUR_PRODUCTION | médiane de r > 1,1 |
| OK | sinon |

Le premier état connu de chaque éolienne est aussi publié (ancien état `null`). Une alerte est un message JSON de clé égale au nom de l'éolienne, par exemple : `{"eolienne": "E12", "ancien_etat": "SOUS_PRODUCTION", "nouvel_etat": "OK", "mediane_r": 0.918, "hors_abaque": 0, "r_sup_2": 1, "taille_fenetre": 60, "derniere_mesure": "…", "emise_a": "…", "source": "13_surveiller"}`. Au démarrage, la première évaluation n'a lieu qu'une fois l'historique du topic relu, pour ne pas juger sur des fenêtres incomplètes ou anciennes.

### Résultats

**Exécution en direct** de 09:03:25 à 09:06:35 UTC (190 s, 19 évaluations). Alertes publiées dans `ferme.alertes` :

| Heure (UTC) | Éolienne | Transition | Médiane de r |
|---|---|---|---|
| 09:03:35 | E7 | — → SOUS_PRODUCTION | 0,649 |
| 09:03:36 | E10 | — → SOUS_PRODUCTION | 0,757 |
| 09:03:36 | E12 | — → SOUS_PRODUCTION | 0,882 |
| 09:03:35-36 | E6, E8, E9, E11, E13, E14, E15 | — → OK | 0,93 à 1,02 |
| 09:03:46 | E12 | SOUS_PRODUCTION → OK | 0,918 |
| 09:03:56 | E12 | OK → SOUS_PRODUCTION | 0,896 |
| 09:04:45 | E13, E14, E15 | OK → HORS_ABAQUE | 31 à 34 mesures hors abaque |
| 09:05:16 | E12 | SOUS_PRODUCTION → OK | 0,947 |
| 09:05:36 | E13, E14, E15 | HORS_ABAQUE → OK | — |

Pour juger sur une durée plus représentative que trois minutes, les mêmes règles (même fonction `commun.etat`) ont été rejouées sur tout l'historique de 08:30 à 09:08 UTC, avec une fenêtre de 60 mesures évaluée toutes les 10 mesures (`results/logs/rejeu_etats.py`, 232 évaluations par éolienne) :

| Éolienne | OK | SOUS_PRODUCTION | SUR_PRODUCTION | HORS_ABAQUE |
|---|---|---|---|---|
| E6 | 87,9 % | 0 % | 5,2 % | 6,9 % |
| E7 | 6,9 % | **87,5 %** | 0 % | 5,6 % |
| E8 | 93,5 % | 0 % | 0 % | 6,5 % |
| E9 | 92,7 % | 0 % | 0 % | 7,3 % |
| E10 | 6,5 % | **86,6 %** | 0 % | 6,9 % |
| E11 | 92,2 % | 0 % | 0 % | 7,8 % |
| E12 | 39,2 % | **53,9 %** | 0 % | 6,9 % |
| E13 | 98,3 % | 0 % | 0 % | 1,7 % |
| E14 | 96,6 % | 0 % | 0 % | 3,4 % |
| E15 | 97,0 % | 0 % | 0 % | 3,0 % |

Médiane de r par tranche de 5 minutes (extrait) :

| Éolienne | 08:30 | 08:40 | 08:50 | 09:00 | 09:35 |
|---|---|---|---|---|---|
| E7 | 0,670 | 0,636 | 0,562 | 0,663 | 0,675 (09:05) |
| E10 | 0,759 | 0,760 | 0,760 | 0,756 | 0,761 (09:05) |
| E12 | 0,852 | 0,924 | 0,867 | 0,866 | 0,947 (09:05) |
| E14 | 0,930 | 0,932 | 0,933 | 0,928 | 0,931 (09:05) |
| E6, E8, E9, E11, E13, E15 | 1,00 à 1,02 | 1,00 à 1,01 | 1,01 | 1,00 à 1,01 | 1,01 |

### Quelles éoliennes sont dans un état autre que OK ?

- **E7** et **E10** : en SOUS_PRODUCTION de façon quasi permanente (87 % du temps), avec des médianes respectives d'environ 0,65 et 0,76.
- **E12** : alternance entre SOUS_PRODUCTION et OK (54 % / 39 %), médiane entre 0,85 et 0,95, juste autour du seuil de 0,9.
- **E13, E14, E15** (plateau) : brièvement HORS_ABAQUE à 09:04:45 ; à ce moment, le vent moyen sur le plateau était de 3,7 m/s et 136 des 228 mesures de la minute étaient des arrêts.
- **E6** : SUR_PRODUCTION pendant 5 % des évaluations seulement (voir partie 15).
- Toutes les éoliennes passent quelques pour cent du temps en HORS_ABAQUE, lors des épisodes de vent faible.

### Que signifie physiquement chaque état ?

**SOUS_PRODUCTION (r < 1).** À régime aérodynamique égal (même Π2), l'éolienne convertit une plus faible part de l'énergie du vent que ses semblables. Causes possibles : dégradation des pales (érosion du bord d'attaque, encrassement, givre), mauvais calage des pales (*pitch*), désalignement de la nacelle par rapport au vent (*yaw*, la puissance chutant à peu près comme cos³ de l'angle), pertes mécaniques (roulements, multiplicateur), défaut de la génératrice ou du convertisseur, bridage volontaire de la puissance, ou encore erreur de mesure (anémomètre qui surestime le vent : 3 % d'erreur sur V donnent environ 9 % sur V³). Les profils diffèrent : **E10** perd 24 % de façon parfaitement constante, ce qui évoque un défaut permanent (bridage, calage, désalignement) ; **E7** perd environ 35 % avec une dispersion énorme (r de 0,1 à 2,5 sur le tableau de bord, figure 7), signe d'un fonctionnement erratique, probablement combiné à une mesure défaillante ; **E12** fluctue entre −5 % et −15 %, ce qui ressemble à un défaut intermittent. **E14**, à 0,93 en permanence, reste OK au sens de la règle mais produit systématiquement 7 % de moins que la référence : le seuil de ±10 % ne la signale pas.

**Une éolienne peut-elle produire plus que Φ ?** Ponctuellement oui, et c'est normal : Φ est la *médiane* des éoliennes saines, donc la moitié de leurs mesures sont au-dessus, avec une dispersion de quelques pour cent (rafales, inertie du rotor). En revanche, une médiane durablement supérieure à 1,1 n'est pas plausible pour une machine de la même famille. Les éoliennes de référence fonctionnent déjà près de leur optimum (C_p ≈ 0,455 au sommet de la courbe), et la limite de Betz (C_p ≤ 0,593, soit Π1 ≤ 0,233) interdit au sommet tout r supérieur à 0,233/0,179 ≈ 1,30. **Un r durablement supérieur à 1 révèle donc d'abord une erreur de mesure** (vent sous-estimé, puissance surestimée, ρ faux, diamètre mal renseigné) ou une machine d'une conception différente de la référence, pour laquelle l'abaque n'est pas valable. Les passages de E6 en SUR_PRODUCTION coïncident avec des mesures aberrantes (partie 15).

**HORS_ABAQUE.** Plus de la moitié des mesures ont un Π2 dans une zone où la table Φ n'a pas de valeur : rotor à l'arrêt (Π2 = 0, vent inférieur à 3 m/s ou supérieur à 25 m/s), régime de rotation jamais observé sur les éoliennes de référence, ou Π2 non calculable. **On ne peut pas conclure à une panne** : on ne sait simplement pas juger. Dans les cas observés (plateau à 09:04, vallée par vent faible), il s'agissait d'arrêts normaux par manque de vent. Une éolienne arrêtée alors que le vent est compris entre 3 et 25 m/s serait en revanche suspecte : il faudrait croiser l'état avec la vitesse du vent.

### Pourquoi juger sur la médiane de 60 mesures plutôt que sur chaque mesure ?

Une mesure isolée est bruitée : rafales, inertie du rotor (la puissance suit le vent avec retard), bruit des capteurs. Même pour les éoliennes saines de référence, 1 à 2,5 % des mesures sortent de la marge de ±10 % (partie 11). Juger chaque mesure produirait, sur dix éoliennes à 1 Hz, une dizaine de fausses alertes par minute. La médiane sur 60 mesures (une minute) filtre ce bruit et reste insensible aux valeurs aberrantes minoritaires, ce qui rend les états stables.

**Défaut que cette méthode laisse passer :** tout ce qui touche moins de la moitié de la fenêtre. D'abord les **défauts ponctuels ou intermittents** : mesures aberrantes isolées (E7, E12) ou épisodes courts (E6 : 30 s toutes les 7 minutes, soit au plus la moitié de la fenêtre, d'où seulement 5 % d'évaluations signalées). Ensuite les **dérives lentes et faibles** qui restent dans la marge (E14 à −7 %). Enfin, toute détection arrive avec un retard de l'ordre de 30 s, le temps que le défaut occupe la moitié de la fenêtre.

### L'abaque Φ, construit sur 5 éoliennes, est-il valable pour les autres ?

Les résultats montrent qu'il se transpose bien : les éoliennes saines E8, E9, E11, E13 et E15, de 5 à **20 m** de diamètre, situées dans les zones colline et plateau (ρ jusqu'à 12 % plus faible, vents jusqu'à 35 m/s), ont une médiane de r comprise entre 1,00 et 1,02. C'est la propriété de similitude : Π1 et Π2 absorbent les effets de taille, de densité et de vitesse. Ses limites sont les suivantes :

- **similitude géométrique et de pilotage** : Φ ne vaut que pour des rotors de même forme (profil et nombre de pales, vrillage) et de même loi de régulation. Une éolienne d'un autre modèle aurait sa propre courbe ;
- **effets d'échelle négligés** : l'analyse a écarté la viscosité de l'air, donc le nombre de Reynolds. À vent égal, il est proportionnel au diamètre : pour E15 (D = 20 m), il est 2,5 à 7 fois plus grand que pour les éoliennes de référence (D de 3 à 8 m), et un grand rotor a en général un C_p légèrement meilleur, ce qui biaise r vers le haut ;
- **domaine couvert** : Φ n'est défini que pour Π2 ∈ [6 ; 22,5[ et n'a été calibré qu'avec des vents de 0 à 18 m/s ;
- **biais de construction** : Φ est environ 1 % trop bas à cause de E2 (partie 11) ;
- **représentativité temporelle** : 33 minutes de mesures, un seul jour, sans givre ni vieillissement, et des mesures à 1 Hz interprétées comme des états d'équilibre, alors que le rotor a de l'inertie ;
- **conditions non modélisées** : turbulence, cisaillement du vent avec la hauteur et effets de relief propres à chaque zone n'entrent pas dans Π1 et Π2.

## Partie 14 — Décision

En croisant la partie 11 (éoliennes de référence) et les parties 13, 16 et 17 (surveillance) :

| Priorité | Éolienne | Constat | Intervention proposée |
|---|---|---|---|
| 1 | **E7** (D = 8 m) | SOUS_PRODUCTION permanente, r ≈ 0,65 (−35 %), dispersion énorme et nombreuses mesures r > 2 (état CAPTEUR) | Inspection complète : chaîne de production (calage, orientation, génératrice) **et** capteurs (anémomètre, mesure de puissance) |
| 1 | **E10** (D = 8 m) | SOUS_PRODUCTION permanente et très stable, r ≈ 0,76 (−24 %) | Inspection de la chaîne de production : bridage, calage des pales, alignement de la nacelle |
| 2 | **E12** (D = 12 m) | Alternance SOUS_PRODUCTION / OK, r de 0,85 à 0,95, mesures aberrantes isolées | Inspection : défaut intermittent probable ; l'enjeu énergétique est important vu le diamètre |
| 2 | **E2** (D = 5 m, référence) | r = 0,857 stable (−14 %), 90 % des mesures hors marge | Inspection de la chaîne de production ; la retirer du jeu de référence et reconstruire Φ |
| 3 | **E6** (D = 5 m) | Production normale (r ≈ 1,01), mais épisodes réguliers de mesures fausses (30 s toutes les 7 min) | Maintenance de l'instrumentation et de l'acquisition, sans urgence pour la production |
| Surveiller | **E14** (D = 12 m) | r = 0,93 constant, dans la marge donc OK | Pas d'intervention immédiate, contrôle lors de la prochaine maintenance |
| Aucune | E13, E14, E15 (HORS_ABAQUE) | Arrêts par vent faible ou trop fort | Aucune : fonctionnement normal |

L'ordre de priorité tient compte de l'énergie perdue, proportionnelle à D² × (1 − r) à vent égal : E7 (64 × 0,35 ≈ 22), E10 (64 × 0,24 ≈ 15), E12 (144 × ~0,10 ≈ 14), E14 (144 × 0,07 ≈ 10), E2 (25 × 0,14 ≈ 3,5). Ces valeurs sont des ordres de grandeur, car les vents diffèrent d'une zone à l'autre. Les éoliennes à intervention prioritaire sont donc **E7, E10, E12 et E2**, auxquelles s'ajoute une intervention sur l'instrumentation de **E6**.

## Partie 15 — Suivre r dans le temps

### Objectif

Fournir un tableau de bord qui affiche en direct, pour les éoliennes choisies, r de chaque mesure en fonction du temps, sa médiane glissante sur 60 mesures et la marge de ±10 %, avec une mise à jour toutes les 5 s.

### Méthode et implémentation

`15_tableau_r.py` utilise **Streamlit** (déjà présent dans `requirements.txt`). Des fils d'exécution de fond, créés une seule fois (`st.cache_resource`), consomment `ferme.eoliennes.pi` depuis le début (jusqu'à 3600 mesures par éolienne), la fin de `ferme.eoliennes` (grandeurs physiques) et `ferme.alertes`. Un fragment Streamlit (`st.fragment(run_every=5)`) se réexécute toutes les 5 s : pour chaque éolienne choisie, il calcule r, la médiane glissante sur 60 mesures (`rolling(60).median()`) et trace la série temporelle selon l'heure capteur (UTC), avec la bande de ±10 %. Les valeurs supérieures à l'échelle choisie sont écrêtées et marquées d'un triangle. L'état de toutes les éoliennes est calculé par la même fonction que les parties 13 et 16 (`commun.etat`). Le choix des éoliennes, de la période et de l'échelle se fait dans la barre latérale ou par l'URL (`?eoliennes=E6,E7&minutes=80`). Le tableau de bord est accessible sur `http://localhost:8501`.

La version finale de l'interface (« Wind Turbine Monitoring », annexe F) organise le tableau de bord en cinq vues : **Overview** (indicateurs clés, carte de l'éolienne sélectionnée, graphique r(t), grandeurs physiques mesurées), **Wind Farm** (tableau des 15 éoliennes, médianes, comparaison de plusieurs éoliennes), **Real-time Monitoring** (graphique demandé par l'énoncé et grandeurs physiques), **Alerts & Anomalies** (états, règles appliquées, historique de `ferme.alertes`) et **Data Quality** (fraîcheur et complétude des données, EX1 à EX5 séparées des éoliennes réelles). La présentation est isolée dans le paquet `scripts/tdb/` (données, thème, graphiques Altair, composants) ; aucun calcul ni seuil n'y est redéfini, ce que vérifie un test dédié. Deux choix ont été dictés par l'honnêteté des données : la puissance nominale n'étant pas publiée, la jauge circulaire porte sur le coefficient de puissance C_p = (8/π)·Π1 rapporté à la limite de Betz, et signale comme physiquement impossible toute valeur au-delà ; aucune température de composant (rotor, roulements) n'est affichée, car les capteurs ne la fournissent pas.

![Capture du tableau de bord en direct pendant le TD (09:48 UTC, première version de l'interface) : r de E6 et E7 sur 80 minutes, médiane glissante sur 60 mesures (rouge), marge ±10 % (vert) et état de toute la ferme ; le trou de 09:08 à 09:33 correspond à la mise en veille du poste](../results/captures/tableau_de_bord_E6_E7.png)

### Observez E6 : quel défaut l'évolution de r dans le temps met-elle en évidence ?

En dehors de certains moments, E6 est parfaitement normale : r reste dans la marge, avec une médiane de 1,00 à 1,02 sur chaque tranche de 5 minutes. Mais **toutes les 7 minutes exactement, pendant environ 30 secondes, r bondit au-delà de 2**, et la médiane glissante monte alors jusqu'à 1,5-1,6. L'analyse de l'historique (`results/logs/analyse_episodes.log`) donne des épisodes débutant à 08:31:00, 08:38:00, 08:45:09, 08:52:00 et 08:59:00 UTC, puis à nouveau à 09:06, 09:34 et 09:41, soit 118 mesures à r > 2 sur 1829. Il s'agit d'un **défaut intermittent et périodique**. Sa régularité parfaite et son caractère tout ou rien (r passe brutalement de 1 à plus de 2, puis revient) désignent un problème de mesure ou d'acquisition (capteur de vent ou de puissance, cycle d'un équipement) plutôt qu'un phénomène aérodynamique : un rotor ne peut pas doubler son rendement pendant 30 s puis revenir exactement à la normale.

### Le tableau de la partie 13 permettait-il de le voir ?

**Presque pas.** La partie 13 n'affiche qu'un état instantané, calculé sur la médiane des 60 dernières mesures. Un épisode de 30 s occupe au plus la moitié de la fenêtre : la médiane ne franchit le seuil de 1,1 que pendant les quelques secondes où la fenêtre contient tout l'épisode. Sur l'historique, E6 n'est en SUR_PRODUCTION que dans 5,2 % des évaluations, et le test en direct de 190 s n'a produit aucune alerte pour elle. Surtout, un état n'a pas de mémoire : même lorsqu'une alerte SUR_PRODUCTION brève apparaît, rien ne montre qu'elle se répète toutes les 7 minutes. C'est la **représentation temporelle** du tableau de bord qui révèle la périodicité.

## Partie 16 — Détecter les défauts ponctuels

### Objectif

Ajouter à la surveillance un état CAPTEUR, prioritaire sur la sous- et la sur-production, déclenché dès qu'au moins une mesure de la fenêtre a un r supérieur à 2.

### Méthode et implémentation

`16_surveiller_capteur.py` réutilise intégralement la partie 13 en activant `capteur=True` dans `commun.etat`. L'ordre de priorité devient HORS_ABAQUE > CAPTEUR > SOUS_PRODUCTION > SUR_PRODUCTION > OK. HORS_ABAQUE reste prioritaire, car l'énoncé ne place CAPTEUR qu'au-dessus de la sous- et de la sur-production. Le nombre de mesures r > 2 de la fenêtre est joint à chaque alerte (`r_sup_2`).

### Résultats

Exécution en direct de 09:03 à 09:06 UTC : **E7** → CAPTEUR et **E12** → CAPTEUR dès la première évaluation, E12 alternant ensuite entre CAPTEUR et SOUS_PRODUCTION, puis **E6 : OK → CAPTEUR à 09:06:07**, au moment précis de son épisode périodique de 09:06. Sur le rejeu de l'historique, les éoliennes en CAPTEUR sont **E7 (82 % des évaluations), E12 (42 %) et E6 (15,5 %)** ; aucune autre éolienne n'a jamais eu de r > 2 (`analyse_episodes.log` : 102 mesures r > 2 pour E7, 22 pour E12, 118 pour E6, 0 pour les autres).

### Pourquoi un r supérieur à 2 révèle-t-il une mesure fausse ?

- **Statistiquement** : la dispersion des éoliennes saines autour de Φ est de quelques pour cent ; un écart d'un facteur 2 est sans commune mesure avec ce bruit.
- **Physiquement** : à Π2 donné, Π1 est fixé par l'aérodynamique du rotor. Doubler la puissance extraite à régime égal supposerait de doubler le rendement aérodynamique. Près du sommet de la courbe (Φ ≈ 0,18), r = 2 donnerait Π1 ≈ 0,36, au-delà de la limite de Betz (0,233) : c'est impossible. On a d'ailleurs relevé dans les données un Π1 de 0,72, soit trois fois cette limite.
- **Dynamiquement** : les valeurs r > 2 apparaissent et disparaissent d'une seconde à l'autre, entourées de valeurs normales. Le rotor a une inertie : sa puissance ne peut pas doubler puis revenir en une seconde.

La cause la plus probable est une erreur sur l'une des grandeurs du calcul : un vent sous-estimé de 21 % suffit à doubler V³ (0,79³ ≈ 0,5), donc à doubler Π1. Un pic sur la mesure de puissance ou une erreur de transmission produirait le même effet.

### Pourquoi combiner la médiane et ce comptage ?

Les deux indicateurs voient des choses complémentaires. La **médiane** décrit le comportement *typique* : elle détecte les défauts de production durables (E7, E10, E12, E2) mais ignore, par construction, les valeurs extrêmes minoritaires. Le **comptage des r > 2** surveille la *queue* de la distribution : il détecte les défauts ponctuels de capteur que la médiane cache (E6, E12). Ni l'un ni l'autre ne suffit seul. CAPTEUR est prioritaire parce qu'une éolienne dont les capteurs donnent des valeurs fausses ne peut plus être jugée de façon fiable sur sa production. En contrepartie, l'état CAPTEUR masque la sous-production de E7 et E12 : c'est pourquoi chaque alerte transporte aussi la médiane de r, afin que l'opérateur dispose des deux informations.

## Partie 17 — Indicateurs par minute avec Spark

### Objectif

Écrire un job Spark Structured Streaming qui lit `ferme.eoliennes.pi`, calcule r pour chaque mesure, puis, par éolienne et par minute (heure des capteurs, watermark de 2 minutes), le nombre de mesures, la médiane de r, le nombre de mesures suspectes (r > 2) et l'état, et qui publie un message par éolienne et par minute dans `ferme.tableau`.

### Méthode et implémentation

`17_spark_indicateurs.py` :

1. lit `ferme.eoliennes.pi` et convertit `horodatage` en *timestamp* avec `try_to_timestamp(…, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")` : c'est le **temps d'événement** ;
2. intègre la table Φ dans le plan de calcul sous forme de *map* littérale (33 entrées), et calcule `tranche = floor(pi2 / 0,5)`, `phi = try_element_at(map, tranche)`, puis `r = try_divide(pi1, phi)` ;
3. regroupe par éolienne et par fenêtre fixe d'une minute, `groupBy("eolienne", window("t", "1 minute"))`, après `withWatermark("t", "2 minutes")` ;
4. agrège `count`, `percentile_approx(r, 0.5)` (médiane), le nombre de r > 2 et le nombre de mesures hors abaque, puis déduit l'état avec les mêmes règles qu'en partie 16 ;
5. écrit en mode `append` dans `ferme.tableau` (clé = éolienne), avec un déclenchement toutes les 10 s et un checkpoint.

### Résultats

840 messages avaient été publiés en fin de session, un par éolienne et par minute. Exemple : `E11 → {"eolienne": "E11", "debut": "2026-11-09T09:05:00.000Z", "fin": "2026-11-09T09:06:00.000Z", "nb_mesures": 60, "mediane_r": 1.0035, "nb_suspects": 0, "nb_hors_abaque": 2, "etat": "OK"}`. Répartition des états par minute sur les 44 premières minutes (08:20-09:08) :

| Éolienne | OK | SOUS_PRODUCTION | CAPTEUR | HORS_ABAQUE | SUR_PRODUCTION |
|---|---|---|---|---|---|
| E2 | 5 | 34 | 0 | 5 | 0 |
| E6 | 36 | 0 | 5 | 3 | 0 |
| E7 | 5 | 3 | 32 | 3 | 1 |
| E10 | 5 | 37 | 0 | 2 | 0 |
| E12 | 12 | 13 | 17 | 2 | 0 |
| E1, E3, E4, E5, E8, E9, E11, E13, E14, E15 | 38 à 42 | 0 | 0 | 2 à 6 | 0 |

Ces résultats concordent avec ceux de la surveillance Python : E2, E7, E10 et E12 sont signalées, et E6 apparaît en CAPTEUR pendant 5 minutes, ce qui correspond à ses épisodes périodiques. Les 81 messages dont la médiane est nulle concernent les minutes sans aucun r calculable (mesures de la partie 02 sans ρ, éoliennes EX à champs manquants).

**Délai de publication mesuré** (heure d'écriture du message dans Kafka moins la fin de la minute) : de **141 à 146 s** pour toutes les fenêtres terminées après 09:34.

### Pourquoi découper selon l'heure des capteurs plutôt que selon l'heure d'arrivée dans Kafka ?

Une minute doit regrouper les mesures *prises* pendant cette minute. L'heure d'arrivée dans Kafka dépend de tout ce qui se passe entre le capteur et Kafka : latence réseau, mise en mémoire tampon du producteur, arrêt et redémarrage du pont, relecture de l'historique. Lors de la reprise après la veille, ou à chaque démarrage avec `startingOffsets=earliest`, des dizaines de minutes de mesures ont été traitées en quelques secondes : découpées selon l'heure d'arrivée, elles seraient toutes tombées dans la même minute, et la médiane aurait mélangé des conditions de vent sans rapport entre elles. Le temps d'événement rend le résultat déterministe et reproductible : rejouer le topic redonne exactement les mêmes indicateurs.

### Que se passe-t-il pour une mesure qui arrive plus de 2 minutes en retard ?

Spark maintient un watermark égal à la plus grande heure capteur déjà vue, moins 2 minutes. Dès que le watermark dépasse la fin d'une fenêtre, celle-ci est considérée comme complète : son résultat est publié (mode `append`) et son état intermédiaire est supprimé. Une mesure dont l'heure est antérieure au watermark, c'est-à-dire qui arrive plus de 2 minutes après des mesures plus récentes, est **ignorée silencieusement** : elle n'est comptée dans aucune fenêtre et aucune erreur n'est signalée. Une mesure en retard de moins de 2 minutes est, elle, correctement intégrée à sa minute. Le watermark est global au flux : c'est l'éolienne la plus « en avance » qui le fait progresser.

### Pourquoi les résultats n'apparaissent-ils qu'au bout de quelques minutes ?

En mode `append`, une fenêtre n'est publiée qu'une fois définitive, donc quand le watermark a dépassé sa fin : la minute [t, t + 1 min[ ne sort qu'après réception d'une mesure d'heure supérieure ou égale à t + 3 min. Le résultat d'une minute arrive donc **au moins 2 minutes après sa fin**, soit environ 3 minutes après son début. S'y ajoutent l'intervalle de déclenchement (10 s) et le fait que Spark calcule le nouveau watermark à la fin d'un micro-lot pour l'appliquer au suivant. Le délai mesuré, de 141 à 146 s après la fin de la minute, correspond exactement à ce calcul (120 s de watermark plus environ 20 s). Au tout premier démarrage s'ajoute le temps de traitement de l'historique.
