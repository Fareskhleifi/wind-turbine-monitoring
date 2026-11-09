# Acquisition des mesures (parties 01 à 04)

## Partie 01 — Écouter le broker

### Objectif

Se connecter au broker avec les identifiants du `.env`, écouter tous les topics pendant quelques secondes, enregistrer chaque topic rencontré une seule fois avec un exemple de message (`topics.jsonl`), ainsi que toutes les mesures d'éoliennes (`mesures.jsonl`).

### Méthode et implémentation

Le broker n'est accessible qu'en **MQTT sur WebSocket sécurisé** (port 443, TLS, chemin `/mqtt`). La fonction `creer_client_mqtt()` de `commun.py` construit un client `paho-mqtt` 2.1 : `transport="websockets"`, `tls_set()` avec le magasin de certificats `certifi`, `ws_set_options(path="/mqtt")`, identifiants lus dans le `.env`, et un identifiant de session unique pour que deux scripts lancés en même temps ne s'éjectent pas mutuellement. Le script `01_mqtt.py` s'abonne au filtre `#` (tous les topics), mémorise dans un dictionnaire le premier message reçu pour chaque topic, et décode en JSON les messages des topics se terminant par `/mesures`. Un message non JSON est ignoré sans interrompre l'écoute.

Avant ce script, un diagnostic pas à pas (`tests/test_mqtt_connection.py`) a été exécuté. Il n'affiche jamais le mot de passe.

| Étape vérifiée | Résultat réel |
|---|---|
| Résolution DNS | `master2026.lemansschoolofai.fr` → 137.74.173.64 |
| Port TCP 443 | ouvert |
| TLS | TLS 1.3, certificat valide pour le nom du broker |
| WebSocket `/mqtt` et authentification | CONNACK « Success » |
| Abonnement `#` | accordé (QoS 0) |
| Réception | 150 messages en 10 s sur 15 topics |

### Résultats

Une écoute de 30 s a produit **15 topics** et **450 mesures** (30 par éolienne, soit exactement une mesure par seconde et par éolienne). Les topics suivent le schéma `ferme/grand-pre/<zone>/<éolienne>/mesures` ; aucun autre topic n'a été reçu avec l'abonnement `#`.

| Zone | Éoliennes (diamètre) |
|---|---|
| vallee | E1 (3 m), E2 (5 m), E3 (5 m), E4 (8 m) |
| champs | E5 (3 m), E6 (5 m), E7 (8 m), E8 (12 m) |
| colline | E9 (5 m), E10 (8 m), E11 (8 m), E12 (12 m) |
| plateau | E13 (8 m), E14 (12 m), E15 (20 m) |

Chaque message est un objet JSON plat, par exemple (topic `ferme/grand-pre/vallee/E1/mesures`) :

```
{"eolienne": "E1", "zone": "vallee", "horodatage": "2026-11-09T08:13:48.000+00:00", "seq": 42649,
 "diametre_m": 3.0, "vent_ms": 2.72, "vent_kmh": 9.792, "temp_c": 13.98, "pression_hpa": 994.27,
 "rotation_rads": 0.0, "puissance_w": 0.0, "puissance_kw": 0.0}
```

L'horodatage est celui du capteur (ISO 8601, UTC, à la seconde) ; `seq` est un compteur partagé par les quinze éoliennes, incrémenté à chaque seconde. Les unités sont données dans le nom des champs.

### Commentaire

Le flux est régulier (1 Hz) et parfaitement structuré, ce qui simplifie la suite. Le compteur `seq` et l'horodatage permettront plus loin de trier les mesures et de repérer les trous dans les données.

## Partie 02 — Recopier les mesures dans Kafka

### Objectif

Écrire un pont continu qui s'abonne uniquement aux mesures et les recopie dans `ferme.eoliennes`, avec le nom de l'éolienne comme clé et le message JSON intact comme valeur, sans perdre de mesure à l'arrêt.

### Méthode et implémentation

La fonction `pont()` de `commun.py` (appelée par `02_pont_kafka.py`) :

- s'abonne au filtre `ferme/+/+/+/mesures` (QoS 1), qui ne capte que les mesures ;
- extrait la clé du topic (avant-dernier segment, `E1` … `E15`), ce qui reste valable même si le contenu du message était illisible ;
- envoie les **octets reçus, sans décodage ni réencodage** (`value=msg.payload`) : le message est strictement intact ;
- utilise un producteur `confluent-kafka` configuré avec `acks=all` et `enable.idempotence=true` : un message n'est considéré comme écrit qu'après accusé de réception du broker, et un renvoi automatique ne crée pas de doublon ;
- gère la saturation de la file locale du producteur (`BufferError` : on laisse partir des messages puis on réessaie) ;
- sur Ctrl+C (ou SIGTERM), arrête d'abord la boucle MQTT (plus aucun message accepté), se déconnecte, puis appelle `producer.flush(30)`, qui bloque jusqu'à l'accusé de tous les messages encore en file.

La reconnexion MQTT est automatique (`reconnect_delay_set(1, 30)`) ; elle s'est d'ailleurs produite en conditions réelles pendant le TD (coupure réseau lors d'une mise en veille du poste) sans arrêt du pont.

### Résultats

Le pont a tourné environ 3 minutes, puis a été arrêté par un signal SIGINT (équivalent de Ctrl+C). Sa sortie indique `Arrêt : 2702 mesures transmises, 0 non accusées`. Les offsets de `ferme.eoliennes` sont passés de 420 à 3122 messages, soit **exactement 2702 messages ajoutés** : aucune mesure reçue n'a été perdue à l'arrêt. La répartition par clé sur les 6 partitions est conforme : 15 clés, donc certaines partitions reçoivent plusieurs éoliennes.

### Commentaire

La garantie porte sur les mesures **reçues** par le pont. Pendant que le pont est arrêté, les mesures publiées sur MQTT ne sont pas conservées par le broker pour nous (session non persistante) : pour couvrir aussi cette fenêtre, il faudrait une session persistante (`clean_start=False` et identifiant client fixe) que le broker accepterait de stocker. C'est précisément ce rôle de mémoire que Kafka joue pour les traitements en aval.

## Partie 03 — Relire Kafka et tracer

### Objectif

Relire depuis le début toutes les mesures de `ferme.eoliennes`, garder E1 et E2 et tracer la puissance en fonction du vent.

### Méthode et implémentation

`lire_topic()` (dans `commun.py`) crée un consommateur à identifiant de groupe unique, lit les *high watermarks* de chaque partition, se positionne au début (`OFFSET_BEGINNING`) et lit jusqu'à atteindre la fin actuelle de chaque partition. On obtient ainsi une relecture complète et reproductible, sans dépendre d'un délai d'attente arbitraire. `03_consumer.py` filtre les clés E1 et E2 et trace P en fonction de V.

### Résultats

La figure a d'abord été produite sur les mesures du pont 02 (209 mesures par éolienne), puis régénérée en fin de TD sur l'ensemble du topic (2550 mesures par éolienne, de 08:13 à 09:36 UTC). C'est cette dernière version qui est présentée.

![Puissance électrique en fonction de la vitesse du vent pour E1 (D = 3 m) et E2 (D = 5 m), mesures relues dans ferme.eoliennes](../results/graphiques/puissance_vent.png)

Rapports des puissances médianes à vent égal (fenêtres de ±0,25 m/s) :

| Vent V | P médiane E1 | P médiane E2 | P(E2) / P(E1) |
|---|---|---|---|
| 6 m/s | 361 W | 874 W | 2,42 |
| 8 m/s | 888 W | 1917 W | 2,16 |
| 10 m/s | 1643 W | 3896 W | 2,37 |

### Pourquoi les deux courbes ne se superposent-elles pas ?

La puissance d'une éolienne vaut P = C_p · ½ ρ · (πD²/4) · V³ : à vent égal, elle est **proportionnelle à la surface balayée, donc à D²**. E2 a un diamètre de 5 m contre 3 m pour E1 : sa surface est (5/3)² ≈ 2,78 fois plus grande, d'où une courbe nettement au-dessus. Le rapport mesuré (2,2 à 2,4) est cependant inférieur à 2,78 : E2 produit moins que ce que sa taille laisserait prévoir, ce qui annonce le défaut identifié en partie 11. Deux autres facteurs dispersent les points : la densité de l'air ρ (identique pour E1 et E2, situées dans la même vallée, mais pas d'une zone à l'autre) et le régime de rotation (à un même vent, le rotor ne tourne pas toujours à la même vitesse, donc C_p varie). La courbe P(V) brute est donc propre à chaque machine : elle ne permet pas de comparer des éoliennes entre elles, d'où l'intérêt de l'analyse dimensionnelle.

On lit aussi sur la figure le seuil de démarrage : en dessous de 3 m/s, la puissance est nulle (rotor arrêté).

## Partie 04 — Enrichir les mesures

### Objectif

Ajouter à chaque mesure, dans le pont, la masse volumique de l'air `rho_kgm3` = P / (R_air · T), avec R_air = 287,05 J·kg⁻¹·K⁻¹, P en Pa et T en K.

### Méthode et implémentation

`04_pont_masse_volumique.py` réutilise la fonction `pont()` en lui passant une transformation `ajouter_rho` : décodage JSON, conversion d'unités, ajout du champ, réencodage. Les conversions sont `p [Pa] = pression_hpa × 100` et `T [K] = temp_c + 273,15` :

$$ρ = (pression_hpa × 100) / (287,05 × (temp_c + 273,15))   [kg·m⁻³]$$

Homogénéité : Pa / (J·kg⁻¹·K⁻¹ · K) = (kg·m⁻¹·s⁻²) / (m²·s⁻²·kg⁻¹) = kg·m⁻³. Le test unitaire vérifie la valeur de l'atmosphère standard (1013,25 hPa, 15 °C) : ρ = 1,225 kg·m⁻³. Si la pression ou la température manque, la mesure est transmise sans `rho_kgm3` plutôt que perdue ; un message non JSON est compté comme invalide et ignoré (aucun cas observé : `0 invalides`).

### Résultats

Le pont enrichi a tourné en continu de 08:29 UTC à la fin de la session, puis a été arrêté par SIGINT sans perte (91 904 mesures transmises, 0 non accusée). Exemple de mesure enrichie relue dans Kafka : `{"eolienne": "E1", …, "temp_c": 12.47, "pression_hpa": 998.18, …, "rho_kgm3": 1.217482}`. Les plages observées sont cohérentes avec l'altitude des zones :

| Zone | ρ minimale | ρ maximale |
|---|---|---|
| vallée (E1-E4) | 1,193 | 1,233 |
| champs (E5-E8) | 1,184 | 1,232 |
| colline (E9-E12) | 1,136 | 1,181 |
| plateau (E13-E15) | 1,050 | 1,091 |

L'air du plateau est environ 12 % moins dense que celui de la vallée : à vent et diamètre égaux, une éolienne du plateau dispose de 12 % de puissance en moins.

> **Purge du topic.** L'énoncé demande de vider les partitions de `ferme.eoliennes` avant de relancer le pont. Pendant la session, le pont enrichi a été relancé sans attendre cette purge, qui a été faite après coup. Les 3122 mesures non enrichies de la partie 02 occupaient exactement le début de chaque partition (offsets 0 à 623, 207, 207, 1041, 415 et 623 pour les partitions 0 à 5, aucune mesure brute au-delà). Elles ont été supprimées avec `kafka-delete-records.sh`, l'opération qu'exécute le bouton « Purge messages » de Kafka UI, mais limitée à ces offsets pour conserver les mesures enrichies. Après la purge, `ferme.eoliennes` ne contient plus aucune mesure réelle sans `rho_kgm3` (`results/logs/04_purge_mesures_brutes.log`). Les résultats du rapport n'en dépendent pas : toutes les fonctions de calcul des Π écartaient déjà les mesures sans ρ, et dans le flux Spark elles donnaient un Π1 nul, donc aucun r.
