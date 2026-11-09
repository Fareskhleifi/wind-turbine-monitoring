"""Partie 11 — Trouver la panne.

Pour chaque mesure de E1 à E5 : r = Π1 / Φ(Π2) ; par éolienne : médiane de r et part des mesures
hors de la marge ±10 %. Distribution de r par éolienne : results/graphiques/r_par_eolienne.png
"""
import importlib
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import commun  # noqa: E402

p08 = importlib.import_module("08_phi_tracer")

if __name__ == "__main__":
    phi = commun.charger_phi()
    donnees = p08.points_par_eolienne()
    distributions = {}
    with open(commun.TABLEAUX / "r_par_eolienne.csv", "w", encoding="utf-8") as f:
        f.write("eolienne;mesures;arrets_0_0;r_calcules;mediane_r;part_hors_marge\n")
        print(f"{'Éolienne':9s} {'mesures':>8s} {'arrêts':>7s} {'r calculés':>10s} {'médiane r':>10s} {'hors ±10%':>10s}")
        for e, pts in donnees.items():
            arrets = sum(1 for a, b, _ in pts if commun.est_arret(a, b))
            rs = [r for r in (commun.ecart(phi, a, b) for a, b, _ in pts if not commun.est_arret(a, b)) if r is not None]
            distributions[e] = rs
            med = statistics.median(rs) if rs else float("nan")
            hors = sum(1 for r in rs if abs(r - 1) > commun.MARGE) / len(rs) if rs else float("nan")
            print(f"{e:9s} {len(pts):8d} {arrets:7d} {len(rs):10d} {med:10.3f} {hors:10.1%}")
            f.write(f"{e};{len(pts)};{arrets};{len(rs)};{med:.4f};{hors:.4f}\n")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.axhspan(1 - commun.MARGE, 1 + commun.MARGE, color="green", alpha=0.12, label="marge ±10 %")
    ax.violinplot(list(distributions.values()), showmedians=True)
    ax.set_xticks(range(1, len(distributions) + 1), list(distributions))
    ax.axhline(1, color="k", lw=1)
    ax.set_ylabel(r"$r = \Pi_1 / \Phi(\Pi_2)$")
    ax.set_xlabel("Éolienne")
    ax.set_title("Distribution de l'écart à l'abaque r par éolienne de référence (trait : médiane)")
    ax.grid(alpha=0.3, axis="y")
    ax.legend()
    fig.tight_layout()
    fig.savefig(commun.GRAPHIQUES / "r_par_eolienne.png", dpi=150)
    print("-> results/graphiques/r_par_eolienne.png")
