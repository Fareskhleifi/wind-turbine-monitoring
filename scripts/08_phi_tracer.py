"""Partie 08 — La forme de Φ.

Relit les mesures de E1 à E5 dans ferme.eoliennes, calcule Π1 = P/(ρV³D²) et Π2 = ωD/V
et trace le nuage Π1(Π2) des cinq éoliennes : results/graphiques/pi1_pi2.png
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import commun  # noqa: E402


def points_par_eolienne(eoliennes=commun.REFERENCE):
    """{éolienne: [(Π1, Π2, mesure), ...]} triés chronologiquement ; mesures incomplètes écartées."""
    res = {e: [] for e in eoliennes}
    for m in commun.lire_topic(commun.TOPIC_MESURES, cles=set(eoliennes)):
        pi1, pi2 = commun.groupes_pi(m)
        if pi1 is not None and m.get("eolienne") in res:
            res[m["eolienne"]].append((pi1, pi2, m))
    for e in res:
        res[e].sort(key=lambda t: t[2]["seq"])
    return res


if __name__ == "__main__":
    donnees = points_par_eolienne()
    fig, ax = plt.subplots(figsize=(9, 5.5))
    for e, pts in donnees.items():
        d = pts[0][2]["diametre_m"] if pts else float("nan")
        ax.scatter([p[1] for p in pts], [p[0] for p in pts], s=6, alpha=0.4, label=f"{e} (D = {d:g} m, n = {len(pts)})")
        print(f"{e} : {len(pts)} mesures, D = {d}")
    ax.set_xlabel(r"$\Pi_2 = \omega D / V$  (sans dimension)")
    ax.set_ylabel(r"$\Pi_1 = P / (\rho V^3 D^2)$  (sans dimension)")
    ax.set_title(r"Nuage $\Pi_1(\Pi_2)$ des éoliennes de référence E1 à E5")
    ax.grid(alpha=0.3)
    ax.legend(markerscale=3)
    fig.tight_layout()
    fig.savefig(commun.GRAPHIQUES / "pi1_pi2.png", dpi=150)
    print("-> results/graphiques/pi1_pi2.png")
