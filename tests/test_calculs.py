"""Tests de validation hors réseau : python tests/test_calculs.py (ou pytest tests/).

Couvre : ρ (partie 04), analyse dimensionnelle (parties 05-06, vérifiée avec sympy), Π1/Π2 et mesures
invalides, construction de Φ (partie 10), r et règles d'état (parties 13 et 16), simulation (partie 18).
"""
import importlib
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import commun  # noqa: E402

MESURE = {"eolienne": "E1", "diametre_m": 3.0, "vent_ms": 8.0, "temp_c": 15.0, "pression_hpa": 1013.25,
          "rotation_rads": 40.0, "puissance_w": 1500.0}


def test_rho():
    # air standard : 101 325 Pa, 288,15 K -> 1,225 kg/m³
    assert abs(commun.rho(1013.25, 15.0) - 1.2250) < 1e-3


def test_pont_ajoute_rho_et_tolere_les_erreurs():
    p04 = importlib.import_module("04_pont_masse_volumique")
    enrichi = json.loads(p04.ajouter_rho(json.dumps(MESURE).encode()))
    assert abs(enrichi["rho_kgm3"] - 1.225) < 1e-3 and enrichi["puissance_w"] == 1500.0
    assert p04.ajouter_rho(b"pas du json") is None                       # message invalide : ignoré
    sans_t = json.loads(p04.ajouter_rho(json.dumps({"eolienne": "E1", "pression_hpa": 1000}).encode()))
    assert "rho_kgm3" not in sans_t                                       # mesure conservée, sans ρ


def test_matrice_dimensionnelle_et_groupes_pi():
    import sympy
    # colonnes : P, V, D, ω, ρ ; lignes : M, L, T
    A = sympy.Matrix([[1, 0, 0, 0, 1],
                      [2, 1, 1, 0, -3],
                      [-3, -1, 0, -1, 0]])
    assert A.rank() == 3
    noyau = A.nullspace()
    assert len(noyau) == 5 - 3                                            # n − r = 2 groupes Π
    pi1 = sympy.Matrix([1, -3, -2, 0, -1])                                # P ρ⁻¹ V⁻³ D⁻²
    pi2 = sympy.Matrix([0, -1, 1, 1, 0])                                  # ω D V⁻¹
    assert A * pi1 == sympy.zeros(3, 1) and A * pi2 == sympy.zeros(3, 1)
    assert sympy.Matrix.hstack(pi1, pi2).rank() == 2                      # indépendants


def test_groupes_pi_et_mesures_incompletes():
    m = dict(MESURE, rho_kgm3=1.225)
    pi1, pi2 = commun.groupes_pi(m)
    assert abs(pi1 - 1500 / (1.225 * 8**3 * 3**2)) < 1e-12 and abs(pi2 - 15.0) < 1e-12
    for champ in ("puissance_w", "vent_ms", "rho_kgm3"):
        assert commun.groupes_pi({k: v for k, v in m.items() if k != champ}) == (None, None)
    assert commun.groupes_pi(dict(m, vent_ms=0)) == (None, None)          # pas de division par zéro
    assert commun.groupes_pi(dict(m, puissance_w=None)) == (None, None)


def test_construire_phi():
    couples = [(0.30, 5.1)] * 19 + [(0.40, 6.2)] * 15 + [(0.40, 6.3)] * 5 + [(9.0, 6.4)]
    phi = commun.construire_phi(couples)
    assert phi == {12: 0.40}                                              # tranche [5;5,5[ : 19 < 20 -> écartée
    assert commun.tranche(6.0) == 12 and commun.tranche(5.99) == 11


def test_etats():
    phi = {k: 0.4 for k in range(10, 20)}
    normal = [(0.4, 6.0)] * 60
    assert commun.etat(normal, phi)[0] == "OK"
    assert commun.etat([(0.3, 6.0)] * 60, phi)[0] == "SOUS_PRODUCTION"   # r = 0,75
    assert commun.etat([(0.5, 6.0)] * 60, phi)[0] == "SUR_PRODUCTION"    # r = 1,25
    assert commun.etat([(0.4, 1.0)] * 31 + normal[:29], phi)[0] == "HORS_ABAQUE"
    assert commun.etat([(0.4, 1.0)] * 30 + normal[:30], phi)[0] == "OK"  # exactement la moitié : pas « plus de »
    pic = [(1.0, 6.0)] + [(0.3, 6.0)] * 59                                # un r = 2,5 dans une fenêtre sous-productive
    assert commun.etat(pic, phi)[0] == "SOUS_PRODUCTION"                 # partie 13 : la médiane ignore le pic
    assert commun.etat(pic, phi, capteur=True)[0] == "CAPTEUR"           # partie 16 : CAPTEUR prioritaire
    assert commun.etat([(None, 6.0)] * 60, phi) == ("OK", None, 0)       # Π1 absent : aucun r, état OK trompeur
    assert commun.etat([(None, None)] * 60, phi)[0] == "HORS_ABAQUE"     # Π2 absent : compté hors abaque
    nan = float("nan")                                                    # pandas représente un champ absent par NaN
    assert commun.ecart(phi, 0.4, nan) is None and commun.ecart(phi, nan, 6.0) is None
    assert commun.etat([(nan, nan)] * 60, phi)[0] == "HORS_ABAQUE"       # régression partie 18 (EX3) : plus de plantage


def test_simulation_manques():
    p18 = importlib.import_module("18_simuler_manques")
    alea = random.Random(0)
    m = dict(MESURE, rho_kgm3=1.2)
    assert p18.abimer("EX5", m, 0, alea)["eolienne"] == "EX5"             # phase d'émission
    assert p18.abimer("EX5", m, 200, alea) is None                        # phase de silence
    sorties = [p18.abimer("EX3", m, 0, alea) for _ in range(3000)]
    part = sum("vent_ms" not in s for s in sorties) / len(sorties)
    assert 0.6 < part < 0.73 and all(s["puissance_w"] == 1500.0 for s in sorties)
    assert "eolienne" in m and m["eolienne"] == "E1"                      # la mesure source n'est pas modifiée


def test_tableau_de_bord_fidele_aux_calculs():
    """Le tableau de bord (partie 15) n'a pas de règle propre : il doit redonner commun.etat et la médiane sur 60."""
    import statistics

    import pandas as pd
    from tdb import composants, donnees
    phi = {k: 0.4 for k in range(10, 20)}
    # 120 mesures horodatées à 1 s d'intervalle ; seq 1004 et 1005 absentes (2 mesures perdues)
    seqs = [s for s in range(1000, 1122) if s not in (1004, 1005)]
    pi1 = [0.3 if i % 7 else 1.0 for i in range(len(seqs))]               # r = 0,75 avec quelques r = 2,5
    points = [(f"2026-11-09T10:{(s - 1000) // 60:02d}:{(s - 1000) % 60:02d}.000+00:00", s, a, 6.0)
              for s, a in zip(seqs, pi1)]
    res = donnees.resume("E9", points, 120, phi)
    etat_attendu = commun.etat([(a, b) for _, _, a, b in points[-60:]], phi, capteur=True)
    assert (res["etat"], res["mediane"], res["suspects"]) == etat_attendu and res["etat"] == "CAPTEUR"
    df = donnees.serie_r(points, phi, minutes=90)
    assert abs(df["mediane_60"].iloc[-1] - statistics.median(a / 0.4 for a in pi1[-60:])) < 1e-12
    q = donnees.qualite("E9", points, donnees.maintenant_utc())
    assert q["sauts"] == 2 and q["n_5min"] == 120 and q["pi1_absent"] == 0
    assert "impossible" in composants.jauge_cp(0.70) and "impossible" not in composants.jauge_cp(0.45)
    assert composants.duree(5119) == "85 min" and composants.nombre(1.0163) == "1,016"
    # état figé : dernière mesure (10:02:01) vieille de 2 h -> signalée ; la règle d'état reste celle de commun.etat
    vieux = donnees.resume("E9", points, 120, phi, maintenant=pd.Timestamp("2026-11-09T12:02:01Z"))
    assert vieux["etat"] == "CAPTEUR" and vieux["age_s"] == 7200 and composants.perimee(vieux)
    assert not composants.perimee(res) and composants.age(res) == ""
    # libellés en majuscules CSS : ρ, ω et r gardent leur casse (sinon ρ s'affiche « P »)
    assert composants.lib("Masse volumique ρ").endswith('<span class="tdb-math">ρ</span>')
    assert composants.lib("Médiane de r (60)") == 'Médiane de <span class="tdb-math">r</span> (60)'
    from tdb import graphiques
    assert graphiques.graphique_r(donnees.serie_r([], phi, 30), 2.5) is None   # message au lieu d'un cadre vide


if __name__ == "__main__":
    for nom, f in list(globals().items()):
        if nom.startswith("test_"):
            f()
            print("OK ", nom)
    print("Tous les tests passent.")
