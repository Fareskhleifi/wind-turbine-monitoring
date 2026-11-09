# Construction de l'abaque Φ (parties 08 à 11)

## Partie 08 — La forme de Φ

### Objectif

Le théorème de Vaschy-Buckingham garantit l'existence d'une relation Π1 = Φ(Π2) mais n'en donne pas la forme. On la cherche expérimentalement en traçant le nuage Π1(Π2) des éoliennes E1 à E5.

### Méthode et implémentation

`08_phi_tracer.py` relit `ferme.eoliennes` (fonction `lire_topic`) en ne gardant que les clés E1 à E5, calcule pour chaque mesure, avec la fonction `groupes_pi` :

$$Π1 = puissance_w / (rho_kgm3 · vent_ms³ · diametre_m²)$$

$$Π2 = rotation_rads · diametre_m / vent_ms$$

et trace les cinq nuages sur un même graphique. Une mesure à laquelle il manque un champ (en particulier les mesures de la partie 02, sans `rho_kgm3`), ou dont le vent est nul, est écartée : on ne divise jamais par zéro. La fonction `points_par_eolienne` est réutilisée telle quelle par les parties 10, 11 et 19.

### Résultat

Le script a été exécuté à 09:02 UTC sur environ 33 minutes de mesures enrichies : 2004 mesures par éolienne.

![Nuage Π1(Π2) des cinq éoliennes de référence E1 à E5 (2004 mesures chacune)](../results/graphiques/pi1_pi2.png)

## Partie 09 — Lecture du graphique

Le graphique de la partie 08 montre d'abord que **les cinq éoliennes, de diamètres différents (3, 5, 5, 8 et 3 m), situées dans deux zones et soumises à des vents de 0 à 18 m/s, se rangent sur une seule et même courbe**. En variables dimensionnelles (partie 03), leurs courbes P(V) étaient séparées d'un facteur supérieur à 2 ; en variables sans dimension, elles se confondent. C'est exactement ce que prédit le théorème de Vaschy-Buckingham : si les éoliennes sont géométriquement semblables, Π1 ne dépend que de Π2, quelles que soient la taille de la machine, la densité de l'air et la vitesse du vent. La forme de Φ apparaît alors directement :

- une **courbe en cloche** : Π1 croît avec Π2, atteint un maximum d'environ 0,18 pour Π2 ≈ 14, puis décroît. C'est l'allure classique de la courbe C_p(λ) d'un rotor : si les pales tournent trop lentement par rapport au vent, l'air passe sans être freiné ; si elles tournent trop vite, le rotor se comporte comme un disque plein que l'air contourne ;
- un maximum cohérent avec la physique : Π1 ≈ 0,179 correspond à C_p = (8/π) · 0,179 ≈ 0,455, en dessous de la limite de Betz (0,593), pour un *tip speed ratio* λ = Π2/2 ≈ 7, valeur typique d'une éolienne tripale ;
- un **domaine limité** : en fonctionnement, Π2 reste compris entre 6 et 22 (λ entre 3 et 11), ce qui traduit la régulation de la vitesse de rotation ;
- un amas de points en **(0, 0)** : rotor arrêté (voir partie 10).

Le graphique fait enfin apparaître, d'un seul coup d'œil, une **bande de points nettement en dessous de la courbe commune, appartenant à E2** : à Π2 donné, E2 extrait moins de puissance du vent que ses voisines. L'analyse dimensionnelle transforme ainsi une comparaison impossible (machines différentes, vents différents) en une comparaison directe à une référence commune.

## Partie 10 — Construire l'abaque Φ

### Objectif

Construire une table Φ donnant, pour chaque tranche de Π2 de largeur 0,5, la médiane de Π1 des mesures de E1 à E5 (tranches de moins de 20 mesures écartées), l'enregistrer dans `phi.json`, et la tracer avec une marge de ±10 % et la moyenne de Π1 par tranche.

### Méthode et implémentation

1. Les points d'arrêt (Π1 = 0 et Π2 = 0) sont retirés (fonction `est_arret`).
2. Chaque mesure est affectée à la tranche k = ⌊Π2 / 0,5⌋, qui couvre l'intervalle [0,5k ; 0,5(k+1)[.
3. Pour chaque tranche contenant au moins 20 mesures, Φ(k) = médiane des Π1 (`construire_phi`).
4. `phi.json` contient, pour chaque tranche retenue, son indice, ses bornes, son effectif et la valeur de Φ ; `phi_detail.csv` contient en plus la moyenne et les tranches écartées.

Φ est volontairement une **table en escalier** (valeur constante par tranche), comme le demande l'énoncé ; aucune interpolation n'est ajoutée. Une mesure dont Π2 tombe dans une tranche absente de la table est dite « hors abaque ».

### Résultats

Exécution à 09:02 UTC : 10 055 mesures de E1 à E5, dont **2648 points d'arrêt (26 %) écartés**, soit 7407 mesures utiles réparties sur 34 tranches. **33 tranches sont retenues**, de Π2 = 6,0 à 22,5 ; seule la tranche [5,5 ; 6,0[ (5 mesures) est écartée. Extrait de la table :

| Tranche de Π2 | Mesures | Φ = médiane de Π1 | Moyenne de Π1 | Écart moyenne / médiane |
|---|---|---|---|---|
| [6,0 ; 6,5[ | 51 | 0,0304 | 0,0307 | +1,0 % |
| [8,0 ; 8,5[ | 98 | 0,0714 | 0,0703 | −1,5 % |
| [10,0 ; 10,5[ | 207 | 0,1210 | 0,1197 | −1,1 % |
| [12,0 ; 12,5[ | 364 | 0,1644 | 0,1609 | −2,1 % |
| [14,0 ; 14,5[ | 485 | 0,1786 | 0,1753 | −1,8 % |
| [16,0 ; 16,5[ | 358 | 0,1554 | 0,1522 | −2,1 % |
| [18,0 ; 18,5[ | 179 | 0,1074 | 0,1065 | −0,8 % |
| [20,0 ; 20,5[ | 54 | 0,0600 | 0,0593 | −1,2 % |
| [22,0 ; 22,5[ | 20 | 0,0307 | 0,0296 | −3,6 % |

![Abaque Φ (médiane par tranche), marge de ±10 % et moyenne de Π1 par tranche, sur le nuage des cinq éoliennes de référence](../results/graphiques/phi.png)

### Pourquoi la médiane plutôt que la moyenne ?

Dans chaque tranche, environ un cinquième des points provient d'une éolienne en panne. La moyenne donne le même poids à chaque point : les points de l'éolienne défaillante tirent toute la courbe vers le bas. On le mesure directement : **la moyenne est inférieure à la médiane dans presque toutes les tranches, de 1 à 2 % autour du maximum** (courbe rouge sous la courbe noire sur la figure). La médiane, elle, ne dépend que du rang des valeurs : tant que moins de la moitié des points d'une tranche sont aberrants, elle reste dans la population des éoliennes saines. Son point de rupture est de 50 % contre 0 % pour la moyenne : un seul point extrême, par exemple une mesure de capteur à Π1 = 0,7, suffit à déplacer la moyenne, pas la médiane. Pour construire une référence de **fonctionnement normal** à partir d'un échantillon qui contient des machines anormales, la médiane est donc l'estimateur adapté.

### Que représentent les points en (0, 0) et pourquoi les écarter ?

Un point (0, 0) correspond à une mesure où le rotor est immobile (ω = 0, donc Π2 = 0) et la puissance nulle (P = 0, donc Π1 = 0). Les données montrent que ces arrêts sont **tous** dus au vent : sur l'ensemble des mesures enrichies, le rotor ne tourne jamais en dessous de 3 m/s (vitesse de démarrage) ni au-dessus de 25 m/s (mise en sécurité par vent fort), et il tourne toujours entre ces deux bornes. Ces points décrivent donc une décision de la régulation (« ne pas produire »), pas le comportement aérodynamique du rotor que Φ doit représenter. Il faut les écarter car :

- ils ne sont pas sur la courbe Φ(Π2) : les inclure créerait une tranche [0 ; 0,5[ de valeur Φ = 0, qui n'a pas de sens physique ;
- avec Φ = 0, l'écart r = Π1/Φ deviendrait 0/0 pour toute éolienne à l'arrêt ;
- ils sont très nombreux (26 % des mesures de E1 à E5) : en les gardant, une éolienne bloquée à l'arrêt par vent favorable serait jugée « normale ».

## Partie 11 — Trouver la panne

### Objectif

Calculer pour chaque mesure de E1 à E5 l'écart à l'abaque r = Π1 / Φ(Π2), puis, par éolienne, la médiane de r et la part des mesures hors de la marge de ±10 %.

### Méthode et implémentation

La fonction `ecart` cherche la tranche de Π2 dans la table et renvoie r = Π1 / Φ, ou rien si la tranche est absente (hors abaque) ou si Π1 ou Π2 manque. Les points d'arrêt sont exclus comme en partie 10. Une mesure est hors marge si |r − 1| > 0,10.

### Résultats

| Éolienne | Mesures | Arrêts (0,0) | r calculés | Médiane de r | Hors marge ±10 % |
|---|---|---|---|---|---|
| E1 | 2017 | 590 | 1427 | 1,011 | 2,5 % |
| **E2** | 2017 | 595 | 1421 | **0,857** | **90,1 %** |
| E3 | 2017 | 600 | 1415 | 1,011 | 1,2 % |
| E4 | 2017 | 596 | 1419 | 1,010 | 1,6 % |
| E5 | 2017 | 282 | 1735 | 1,011 | 2,0 % |

(Le script, lancé quelques secondes après la partie 10, a lu 13 mesures de plus par éolienne ; Φ est celui de `phi.json`.)

![Distribution de r = Π1/Φ(Π2) par éolienne de référence (violons ; trait horizontal : médiane ; bande verte : marge ±10 %)](../results/graphiques/r_par_eolienne.png)

### Quelle éolienne est en panne ?

**E2.** Sa médiane de r vaut 0,857 : elle produit en moyenne 14 % de puissance de moins que ce que prévoit l'abaque pour son régime de fonctionnement, et 90 % de ses mesures sont hors de la marge. Les quatre autres éoliennes ont une médiane de 1,010 à 1,011 et seulement 1 à 2,5 % de mesures hors marge, ce qui correspond à la dispersion normale des mesures.

### Comment le voit-on, et pourquoi E2 n'a-t-elle pas faussé Φ ?

On le voit sur la figure 4 : la distribution de r de E2 est entièrement décalée vers le bas (son maximum dépasse à peine 1), alors que celles de E1, E3, E4 et E5 sont centrées sur 1 et presque entièrement dans la bande verte. Sur le nuage de la partie 08, cela se traduit par une bande de points séparée sous la courbe commune.

E2 n'a pas faussé Φ parce qu'elle ne représente qu'**une éolienne sur cinq, soit environ 20 % des points de chaque tranche**, et que Φ est une médiane : tant que les points anormaux restent minoritaires (moins de 50 %), la médiane de chaque tranche est fixée par les éoliennes saines. L'influence de E2 n'est cependant pas strictement nulle : les éoliennes saines ont une médiane de r de 1,011 et non de 1,000. Ce biais de 1 % est la trace résiduelle de E2 dans la médiane, à comparer aux 2 % environ qu'elle aurait introduits dans une moyenne. Il reste dix fois plus petit que la marge de ±10 % et ne change aucun diagnostic. On pourrait l'annuler en reconstruisant Φ sans E2, maintenant qu'elle est identifiée.
