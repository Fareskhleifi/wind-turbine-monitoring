"""Assemble report/Compte_Rendu_TD_Ferme_Eolienne.md à partir de report/sections/*.md.

Usage : python report/assembler_md.py   puis   node report/construire_docx.js
"""
from pathlib import Path

ICI = Path(__file__).resolve().parent
S = ICI / "sections"

ENTETE = """---
etudiant: ……………………………… (nom et prénom à compléter)
binome: ……………………………… (à compléter)
enseignant: Gaétan GOTTIS
remise: 16 novembre 2026
---

"""
PARTIE18 = """# Robustesse aux données manquantes (partie 18)

## Partie 18 — Données manquantes

### Objectif

Prévoir puis observer l'effet de deux types de données manquantes sur chacun des scripts — un champ absent dans \
certaines mesures, et des mesures qui n'arrivent plus pendant un moment —, puis proposer des moyens de les détecter.

"""
morceaux = [
    ENTETE,
    (S / "a_intro_theorie.md").read_text(encoding="utf-8"),
    (S / "b_partie01_04.md").read_text(encoding="utf-8"),
    "# Analyse dimensionnelle (parties 05 à 07)\n\n",
    (S / "c_partie05_07.md").read_text(encoding="utf-8"),
    (S / "d_partie08_11.md").read_text(encoding="utf-8"),
    (S / "e_partie12_17.md").read_text(encoding="utf-8"),
    PARTIE18,
    (S / "f_partie18_prediction.md").read_text(encoding="utf-8"),
    (S / "f_partie18_observations.md").read_text(encoding="utf-8"),
    (S / "g_partie19_conclusion.md").read_text(encoding="utf-8"),
]
sortie = ICI / "Compte_Rendu_TD_Ferme_Eolienne.md"
sortie.write_text("\n".join(m.rstrip("\n") + "\n" for m in morceaux), encoding="utf-8")
print(f"{sortie.name} : {sum(1 for _ in sortie.open(encoding='utf-8'))} lignes")
