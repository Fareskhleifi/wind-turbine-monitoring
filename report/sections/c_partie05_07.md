## Partie 05 — Analyse dimensionnelle

### Objectif

Recenser les grandeurs physiques présentes dans chaque mesure, donner leur dimension dans le système SI, puis construire la matrice dimensionnelle des variables utiles (sans la température ni la pression, déjà contenues dans ρ) et en calculer le rang.

### Variables physiques d'une mesure et dimensions SI

On note M la masse (kg), L la longueur (m), T le temps (s) et Θ la température (K). Les champs `eolienne`, `zone`, `horodatage` et `seq` sont des identifiants ou des repères temporels : ils ne sont pas des grandeurs physiques du problème.

| Champ du message | Grandeur | Symbole | Unité SI | Dimension |
|---|---|---|---|---|
| `diametre_m` | diamètre du rotor | D | m | L |
| `vent_ms` (et `vent_kmh`) | vitesse du vent | V | m·s⁻¹ | L·T⁻¹ |
| `temp_c` | température de l'air | T_air | K (après + 273,15) | Θ |
| `pression_hpa` | pression atmosphérique | p | Pa = kg·m⁻¹·s⁻² (après × 100) | M·L⁻¹·T⁻² |
| `rotation_rads` | vitesse angulaire du rotor | ω | rad·s⁻¹ | T⁻¹ (le radian est sans dimension) |
| `puissance_w` (et `puissance_kw`) | puissance électrique | P | W = kg·m²·s⁻³ | M·L²·T⁻³ |
| `rho_kgm3` (ajouté en partie 04) | masse volumique de l'air | ρ | kg·m⁻³ | M·L⁻³ |

`vent_kmh` et `puissance_kw` sont la même grandeur que `vent_ms` et `puissance_w` dans une autre unité : on ne les compte qu'une fois. La dimension de la puissance se retrouve par W = J·s⁻¹ = N·m·s⁻¹ = kg·m·s⁻²·m·s⁻¹ = kg·m²·s⁻³.

### Matrice dimensionnelle

Sans T_air ni p, il reste n = 5 variables : P, V, D, ω, ρ. Chaque colonne contient les exposants de M, L et T de la variable :

| Unité de base | P | V | D | ω | ρ |
|---|---|---|---|---|---|
| M (kg) | 1 | 0 | 0 | 0 | 1 |
| L (m) | 2 | 1 | 1 | 0 | −3 |
| T (s) | −3 | −1 | 0 | −1 | 0 |

Les autres unités SI de base (K, A, mol, cd) n'interviennent dans aucune de ces cinq variables : leurs lignes seraient entièrement nulles et ne changeraient pas le rang ; on ne les écrit donc pas.

### Rang de la matrice

La matrice A est de taille 3 × 5, donc rg(A) ≤ 3. Pour montrer que rg(A) = 3, il suffit de trouver un mineur d'ordre 3 non nul. On prend les colonnes V, D, ρ :

$$Δ = det [ (0, 0, 1) ; (1, 1, −3) ; (−1, 0, 0) ]   (lignes M, L, T ; colonnes V, D, ρ)$$

En développant selon la première ligne, seul le troisième terme est non nul :

$$det = (+1) × 1 × det [ 1 1 ; −1 0 ] = 1 × (1 × 0 − 1 × (−1)) = 1 ≠ 0$$

Le mineur est non nul, donc **rg(A) = 3**. Ce résultat a été vérifié numériquement avec `sympy` dans `tests/test_calculs.py` (`Matrix.rank() == 3`).

Physiquement, le rang 3 signifie que les trois dimensions M, L et T sont réellement indépendantes dans le problème : V, D et ρ suffisent à les « porter » toutes les trois (ρ apporte la masse, D la longueur, V le temps).

## Partie 06 — Groupes Π

### Objectif

Appliquer le théorème de Vaschy-Buckingham pour obtenir deux nombres sans dimension indépendants.

### Nombre de groupes

Avec n = 5 variables et une matrice de rang r = 3, le théorème garantit l'existence de **n − r = 5 − 3 = 2** groupes sans dimension indépendants, et toute relation physique f(P, V, D, ω, ρ) = 0 peut se réécrire F(Π1, Π2) = 0, soit Π1 = Φ(Π2).

### Choix des variables répétées

On choisit comme variables de base ρ, V et D : ce sont exactement les trois colonnes du mineur non nul de la partie 05, elles sont donc dimensionnellement indépendantes et engendrent M, L et T. Les deux variables restantes, P et ω, donnent chacune un groupe.

### Calcul de Π1 (à partir de la puissance P)

On cherche a, b, c tels que Π1 = P · ρᵃ · Vᵇ · Dᶜ soit sans dimension :

$$[Π1] = (M·L²·T⁻³) · (M·L⁻³)ᵃ · (L·T⁻¹)ᵇ · Lᶜ = M⁽¹⁺ᵃ⁾ · L⁽²⁻³ᵃ⁺ᵇ⁺ᶜ⁾ · T⁽⁻³⁻ᵇ⁾$$

On annule chaque exposant :

- M : 1 + a = 0, donc a = −1 ;
- T : −3 − b = 0, donc b = −3 ;
- L : 2 − 3a + b + c = 0, soit 2 + 3 − 3 + c = 0, donc c = −2.

$$Π1 = P / (ρ · V³ · D²)$$

Vérification d'homogénéité : kg·m²·s⁻³ / (kg·m⁻³ · m³·s⁻³ · m²) = kg·m²·s⁻³ / (kg·m²·s⁻³) = 1.

### Calcul de Π2 (à partir de la vitesse de rotation ω)

On cherche Π2 = ω · ρᵃ · Vᵇ · Dᶜ :

$$[Π2] = T⁻¹ · (M·L⁻³)ᵃ · (L·T⁻¹)ᵇ · Lᶜ = Mᵃ · L⁽⁻³ᵃ⁺ᵇ⁺ᶜ⁾ · T⁽⁻¹⁻ᵇ⁾$$

- M : a = 0 ;
- T : −1 − b = 0, donc b = −1 ;
- L : −3a + b + c = 0, soit −1 + c = 0, donc c = 1.

$$Π2 = ω · D / V$$

Vérification : s⁻¹ · m / (m·s⁻¹) = 1. Les vecteurs d'exposants (1, −3, −2, 0, −1) et (0, −1, 1, 1, 0) dans l'ordre (P, V, D, ω, ρ) sont bien dans le noyau de A et sont indépendants (vérifié avec `sympy`).

### Interprétation physique

- **Π1** compare la puissance produite au flux d'énergie cinétique du vent qui traverse le rotor. Le coefficient de puissance usuel est C_p = P / (½ ρ A V³) avec A = πD²/4, d'où **C_p = (8/π) · Π1 ≈ 2,55 · Π1**. La limite de Betz (C_p ≤ 16/27 ≈ 0,593) impose donc **Π1 ≤ 2π/27 ≈ 0,233** pour toute éolienne réelle.
- **Π2** compare la vitesse du bout des pales (ω·D/2) à celle du vent : c'est le double du *tip speed ratio* λ = ωR/V, soit **λ = Π2 / 2**.

Ces deux groupes sont donc les variables classiques de l'aérodynamique des éoliennes, et la relation Π1 = Φ(Π2) est, à un facteur près, la courbe C_p(λ) d'un rotor.

## Partie 07 — Rôle de ρ

L'introduction de ρ a été utile pour trois raisons.

1. **Sans ρ, la température ne pouvait entrer dans aucun groupe Π.** Avec les variables P, V, D, ω, p et T_air, la température est la seule à porter la dimension Θ : aucun produit de puissances de ces variables ne peut la rendre sans dimension. L'analyse aurait conclu, à tort, que le fonctionnement ne dépend pas de la température. C'est la constante R_air = 287,05 J·kg⁻¹·K⁻¹ (qui contient Θ) qui permet de combiner p et T_air en une grandeur mécanique ρ = p / (R_air · T_air), de dimension M·L⁻³. On injecte ainsi une loi physique connue (gaz parfait) dans le modèle.
2. **ρ est la grandeur qui intervient réellement dans la physique du problème.** La puissance disponible dans le vent vaut ½ ρ A V³ : c'est la masse d'air traversant le rotor qui compte, pas séparément sa pression et sa température.
3. **ρ réduit le nombre de variables.** Avec (P, V, D, ω, p, T_air), on aurait n = 6 et un rang 4 (M, L, T, Θ), donc encore 2 groupes, par exemple P/(p·V·D²) et ω·D/V : T_air en serait exclue, et le premier groupe ne mesurerait pas correctement l'énergie disponible, puisque p seule ne fixe pas la masse d'air. En remplaçant deux variables et une dimension par la seule variable ρ, on obtient 2 groupes qui contiennent toute l'information utile. Un abaque à une seule variable d'entrée (Π2) se construit et se lit beaucoup plus facilement. Enfin, ρ permet de comparer des éoliennes placées à des altitudes et des températures différentes (vallée, colline, plateau) : sa variation de quelques pour cent modifie directement la puissance disponible.
