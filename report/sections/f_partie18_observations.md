### Simulation

`18_simuler_manques.py` lit en continu les nouvelles mesures de E1 à E5 dans `ferme.eoliennes` et les republie dans ce même topic sous les noms EX1 à EX5, chacune abîmée différemment. Pour éviter toute boucle, le programme ignore les clés EX1 à EX5. Les tirages aléatoires utilisent une graine fixe (18), ce qui rend la simulation reproductible. **Les données EX1 à EX5 sont simulées** : elles sont dérivées des mesures réelles de E1 à E5, mais leurs défauts sont artificiels.

| Éolienne simulée | Source | Défaut simulé | Bilan du simulateur (09:33-09:46) |
|---|---|---|---|
| EX1 | E1 | mesures perdues (2 sur 3 en moyenne, au hasard) | 260 republiées sur 787 reçues (33 %) |
| EX2 | E2 | `puissance_w` absent dans 2 mesures sur 3 en moyenne | 787 / 787 |
| EX3 | E3 | `vent_ms` absent dans 2 mesures sur 3 en moyenne | 786 / 786 |
| EX4 | E4 | `rho_kgm3` absent dans 2 mesures sur 3 en moyenne | 786 / 786 |
| EX5 | E5 | silence : 2 min d'émission puis 3 min sans rien, en boucle | 326 / 786 (41 %) |

EX1 à EX5 ont été ajoutées aux éoliennes surveillées (`--eoliennes E6,…,E15,EX1,…,EX5`). Les parties 13 et 16 ont tourné de 09:35 à 09:46 UTC, pendant que les jobs Spark 12 et 17 et le tableau de bord fonctionnaient en continu.

### Observations script par script

| Script | Observation réelle |
|---|---|
| 02 / 04 (pont) | Aucun effet : les EX sont injectées directement dans Kafka par le simulateur, le pont MQTT ne les voit pas. |
| 12 (Spark Π) | Aucune erreur. Les valeurs nulles sont omises par `to_json` : 528 messages de EX2 et 543 de EX4 n'ont que Π2 ; 535 messages de EX3 n'ont ni Π1 ni Π2, par exemple `{"eolienne": "EX3", "zone": "vallee", "horodatage": "…09:08:30…", "seq": 45931}`. |
| 13 / 16 (surveillance) | **EX1** : OK (médiane 1,02), la perte de deux mesures sur trois est invisible, la fenêtre de 60 couvre simplement environ 3 minutes. **EX2** : SOUS_PRODUCTION (0,86), le défaut de E2 reste détecté à partir du tiers de mesures complètes, mais l'absence de puissance n'est pas signalée. **EX3** : HORS_ABAQUE en permanence (39 à 42 mesures sur 60 sans Π2), un problème de capteur classé comme un régime de fonctionnement. **EX4** : OK, l'absence de ρ est invisible. **EX5** : pendant le silence, l'état reste figé (OK, médiane 1,015) et aucune alerte n'est émise ; seule la dernière mesure, bloquée à 09:36:59 alors qu'il est 09:38:57, trahit le problème. EX1, EX2 et EX4 passent aussi par moments en HORS_ABAQUE, comme leurs sources de la vallée, à cause du vent faible et non du défaut simulé. |
| 15 (tableau de bord) | **Plantage du tableau de bord sur EX3** : pandas représente le Π2 absent par `NaN` et non par `None`, et `floor(NaN)` lève `ValueError: cannot convert float NaN to integer`. Ce défaut a été corrigé dans la fonction partagée (`commun.manquant`, qui traite None et NaN de la même façon), un test de non-régression a été ajouté, et le tableau de bord a été relancé. Ensuite : courbes clairsemées pour EX2 à EX4, et silence de EX5 visible comme un trou que la médiane glissante enjambe en ligne droite (figure 8). |
| 17 (Spark minute) | **EX1** : 17 à 26 mesures par minute au lieu de 60. **EX2 et EX4** : 60 mesures par minute, comme si de rien n'était. **EX3** : HORS_ABAQUE à chaque minute. **EX5** : **aucun message** pour les minutes de silence (09:37, 09:38, 09:39, puis 09:42, 09:43). |
| 08, 10, 11, 19 (batch) | Non concernés : ils ne lisent que les clés E1 à E5. |

![Tableau de bord (première version) pendant un silence de EX5 (09:43 UTC) : trou de 09:37 à 09:40 sur EX5, points clairsemés pour EX3 (vent absent) et EX2 (puissance absente, sous-production de E2 toujours visible)](../results/captures/tableau_de_bord_EX_silence.png)

Une situation réelle a d'ailleurs reproduit le cas 2 à l'échelle de toute la ferme : la mise en veille du poste de 09:08 à 09:33 a interrompu la réception pendant 25 minutes. Aucun script n'a signalé ce trou ; on ne le voit qu'*a posteriori*, sur les courbes temporelles (figure 7).

### Comparaison avec la réponse initiale

Les prédictions se sont **vérifiées pour l'essentiel** : aucun plantage côté Kafka, Spark ou surveillance ; EX3 mal classée en HORS_ABAQUE ; état figé et absence d'alerte pendant le silence de EX5 ; aucune fenêtre Spark pour les minutes sans mesure ; nombre de mesures par minute réduit pour EX1. Deux points diffèrent :

1. **Le tableau de bord a planté**, alors que j'avais seulement prévu des trous dans la courbe. Le cas « champ absent » n'était géré que pour la valeur `None` des messages JSON : la conversion en DataFrame a introduit une autre représentation de l'absence (`NaN`), passée inaperçue. Le défaut est désormais corrigé et testé.
2. **Le cas « OK par défaut » ne s'est pas produit** pour EX2 et EX4, parce qu'il restait un tiers de mesures complètes : la médiane est calculée sur environ 20 mesures et donne le bon diagnostic (sous-production pour EX2, état sain pour EX4). Le risque prévu reste réel si la proportion de champs absents approche 100 % : c'est ce que montre le test unitaire `etat([(None, 6.0)] * 60) == "OK"`.

La conclusion générale est confirmée : **la chaîne ne plante plus, mais elle se tait**. Aucune donnée manquante n'est signalée explicitement.

### Comment pourrait-on détecter ces problèmes ?

- **Fraîcheur des données** (silence, EX5, veille du poste) : surveiller l'âge de la dernière mesure de chaque éolienne (heure courante moins heure capteur) et émettre un état `MUETTE` au-delà de quelques secondes. Dans Spark, comparer la liste des éoliennes attendues à celles présentes dans chaque minute (jointure avec une table de référence), ou utiliser un traitement à état avec délai d'expiration (`applyInPandasWithState`) pour publier une alerte quand une clé n'envoie plus rien.
- **Complétude** (mesures perdues, EX1) : le compteur `seq`, incrémenté chaque seconde, permet de compter exactement les mesures manquantes (sauts de `seq`). On peut aussi comparer `nb_mesures` à 60 dans `ferme.tableau` et déclencher un état `INCOMPLET` sous un seuil, par exemple 54 mesures (90 %).
- **Validité du schéma** (champs absents, EX2 à EX4) : valider les champs obligatoires dès le pont ou dans le job 12, publier les messages incomplets dans un topic de rejets (par exemple `ferme.eoliennes.rejets`) et compter, par fenêtre, les mesures sans puissance, sans vent ou sans ρ.
- **Distinguer les causes de HORS_ABAQUE** : séparer « Π2 absent » (problème de donnée, à classer en CAPTEUR ou INCOMPLET) de « Π2 hors du domaine » (régime non couvert), et croiser avec le vent mesuré (arrêt normal sous 3 m/s ou au-dessus de 25 m/s).
- **Robustesse du code** : traiter toute valeur manquante par une fonction unique (`commun.manquant`), tester les scripts avec des données dégradées (comme ici) et afficher dans le tableau de bord l'âge de la dernière mesure, en rouge au-delà d'un seuil.
