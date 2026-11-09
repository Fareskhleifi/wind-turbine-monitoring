"""Partie 10 — Construire l'abaque Φ.

À partir des mesures de E1 à E5 : pour chaque tranche de Π2 de largeur 0,5, médiane de Π1
(tranches de moins de 20 mesures écartées, points d'arrêt (0,0) écartés).
Produit results/tableaux/phi.json, phi_detail.csv et results/graphiques/phi.png
(nuage, courbe Φ, marge ±10 %, moyenne de Π1 par tranche).
"""
import importlib
import json
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import commun  # noqa: E402

p08 = importlib.import_module("08_phi_tracer")


def couples_utiles(points):
    """[(Π1, Π2, m), ...] -> [(Π1, Π2), ...] sans les points d'arrêt (0,0)."""
    return [(a, b) for a, b, _ in points if not commun.est_arret(a, b)]


def ecrire_phi(phi, lignes, chemin):
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump({"definition": "phi = mediane de Pi1 par tranche de Pi2 de largeur 0.5 ; E1-E5 ; "
                                 "tranches de moins de 20 mesures et points (0,0) exclus",
                   "pi1": "P / (rho V^3 D^2)", "pi2": "omega D / V",
                   "largeur_tranche": commun.LARGEUR_TRANCHE, "min_mesures": commun.MIN_MESURES_TRANCHE,
                   "tranches": [{"tranche": l["tranche"], "pi2_min": l["pi2_min"], "pi2_max": l["pi2_max"],
                                 "n": l["n"], "phi": l["mediane_pi1"]} for l in lignes if l["retenue"]]},
                  f, indent=1)


if __name__ == "__main__":
    donnees = p08.points_par_eolienne()
    tous = [p for pts in donnees.values() for p in pts]
    couples = couples_utiles(tous)
    print(f"{len(tous)} mesures E1-E5 dont {len(tous) - len(couples)} points d'arrêt (0,0) écartés")
    phi = commun.construire_phi(couples)

    par_tranche = {}
    for a, b in couples:
        par_tranche.setdefault(commun.tranche(b), []).append(a)
    lignes = [{"tranche": k, "pi2_min": k * commun.LARGEUR_TRANCHE, "pi2_max": (k + 1) * commun.LARGEUR_TRANCHE,
               "n": len(v), "mediane_pi1": statistics.median(v), "moyenne_pi1": statistics.fmean(v),
               "retenue": k in phi} for k, v in sorted(par_tranche.items())]
    ecrire_phi(phi, lignes, commun.TABLEAUX / "phi.json")
    with open(commun.TABLEAUX / "phi_detail.csv", "w", encoding="utf-8") as f:
        f.write("tranche;pi2_min;pi2_max;n;mediane_pi1;moyenne_pi1;retenue\n")
        for l in lignes:
            f.write(f"{l['tranche']};{l['pi2_min']:.1f};{l['pi2_max']:.1f};{l['n']};{l['mediane_pi1']:.4f};"
                    f"{l['moyenne_pi1']:.4f};{'oui' if l['retenue'] else 'non'}\n")
            print(f"  Π2 ∈ [{l['pi2_min']:4.1f}, {l['pi2_max']:4.1f}[  n = {l['n']:5d}  médiane = {l['mediane_pi1']:.4f}"
                  f"  moyenne = {l['moyenne_pi1']:.4f}  {'retenue' if l['retenue'] else 'écartée (n < 20)'}")
    print(f"{len(phi)} tranches retenues sur {len(lignes)} -> results/tableaux/phi.json")

    fig, ax = plt.subplots(figsize=(9, 5.5))
    for e, pts in donnees.items():
        ax.scatter([p[1] for p in pts], [p[0] for p in pts], s=5, alpha=0.25, label=e)
    centres = [(k + 0.5) * commun.LARGEUR_TRANCHE for k in phi]
    valeurs = list(phi.values())
    ax.fill_between(centres, [v * (1 - commun.MARGE) for v in valeurs], [v * (1 + commun.MARGE) for v in valeurs],
                    color="grey", alpha=0.35, label="marge ±10 % autour de Φ", zorder=3)
    ax.plot(centres, valeurs, "k-o", ms=4, lw=2, label=r"$\Phi$ (médiane de $\Pi_1$ par tranche)", zorder=4)
    retenues = [l for l in lignes if l["retenue"]]
    ax.plot([(l["tranche"] + 0.5) * commun.LARGEUR_TRANCHE for l in retenues], [l["moyenne_pi1"] for l in retenues],
            "r--s", ms=4, label=r"moyenne de $\Pi_1$ par tranche", zorder=5)
    ax.set_xlabel(r"$\Pi_2 = \omega D / V$")
    ax.set_ylabel(r"$\Pi_1 = P / (\rho V^3 D^2)$")
    ax.set_title(r"Abaque de fonctionnement normal $\Phi$ construit sur E1 à E5")
    ax.grid(alpha=0.3)
    ax.legend(markerscale=3, fontsize=8)
    fig.tight_layout()
    fig.savefig(commun.GRAPHIQUES / "phi.png", dpi=150)
    print("-> results/graphiques/phi.png")
