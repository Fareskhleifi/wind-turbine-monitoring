### Réponse a priori (rédigée avant d'exécuter la simulation)

Cette analyse a été écrite à partir du code, avant de lancer `18_simuler_manques.py`.

**Cas 1 — un champ absent dans certaines mesures.**

| Script | Effet attendu |
|---|---|
| 01, 02 | Aucun : les messages sont enregistrés ou recopiés tels quels, sans lecture des champs. |
| 04 (pont ρ) | Si `pression_hpa` ou `temp_c` manque, ρ n'est pas calculé ; la mesure est transmise sans `rho_kgm3` (pas de plantage, pas de perte). Les autres champs absents passent inaperçus. |
| 03, 08, 10, 11, 19 | Les mesures incomplètes sont écartées (Π non calculable) : moins de points, Φ inchangé tant que la proportion reste faible. |
| 12 (Spark Π) | `from_json` met le champ à null et `try_divide` propage le null : sans `puissance_w` ou `rho_kgm3`, Π1 est absent mais Π2 est calculé ; sans `vent_ms`, Π1 et Π2 sont tous deux absents. |
| 13 / 16 | Sans Π1 : r n'est pas calculable, la médiane porte sur les seules mesures complètes ; si aucune ne l'est, la médiane est indéfinie et l'éolienne reste **OK par défaut** (faux négatif). Sans Π2 (vent absent) : la mesure compte comme « absente de l'abaque » ; au-delà de la moitié de la fenêtre, l'éolienne passe **HORS_ABAQUE**, ce qui est un mauvais diagnostic (problème de capteur, pas de régime de fonctionnement). |
| 15 (tableau de bord) | Les mesures sans r disparaissent de la courbe : trous dans la série, médiane glissante calculée sur moins de points. |
| 17 (Spark minute) | `nb_mesures` compte toutes les mesures, mais la médiane de r ne porte que sur les mesures complètes ; un vent absent fait monter `nb_hors_abaque` et peut faire passer la minute en HORS_ABAQUE. |

**Cas 2 — des mesures qui n'arrivent plus pendant un moment.**

| Script | Effet attendu |
|---|---|
| 02 / 04 | Rien à transmettre : aucune erreur, aucune trace du silence dans Kafka. |
| 13 / 16 | La fenêtre des 60 dernières mesures n'est plus renouvelée : l'état est figé sur sa dernière valeur et **aucune alerte n'est émise** ; seul l'horodatage de la dernière mesure, affiché dans la console, vieillit. |
| 15 | La courbe s'arrête, sans message d'erreur. |
| 17 | Aucune mesure, donc aucune fenêtre : **aucun message** n'est publié pour cette éolienne pendant ces minutes. L'absence de message n'est pas une alerte. |
| Pertes partielles | Avec des mesures perdues de façon sporadique, la fenêtre de 60 mesures couvre une durée plus longue ; la médiane reste correcte, mais `nb_mesures` par minute diminue dans `ferme.tableau`. |

En résumé, la chaîne ne plante pas, mais **elle se tait** : aucun script ne signale explicitement qu'une donnée manque, et deux défauts peuvent être mal classés (OK par défaut, HORS_ABAQUE au lieu de CAPTEUR).
