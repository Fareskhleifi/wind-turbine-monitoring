"""Partie 19 — Une seule éolienne suffit-elle ?

1) phi_e1.png : nuage Π1(Π2) de E1 seule + abaque Φ construit sur E1 seule + abaque de référence (E1 à E5).
2) convergence.png : Φ construit avec les N premières mesures utiles (N de 30 à toutes), de E1 seule puis
   de E1 à E5, et son écart médian à l'abaque de référence :
       écart(N) = médiane sur les tranches communes de |Φ_N(tranche) / Φ_ref(tranche) − 1|
Les « mesures utiles » sont les mesures complètes hors points d'arrêt (0,0), prises dans l'ordre chronologique.
"""
import importlib
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import commun  # noqa: E402

p08 = importlib.import_module("08_phi_tracer")


def ecart_median(phi, ref):
    communes = [k for k in phi if k in ref]
    return statistics.median(abs(phi[k] / ref[k] - 1) for k in communes) if communes else None


def courbe_convergence(couples, ref, pas=30):
    """[(N, écart médian, nombre de tranches)] pour N = 30, 60, ... , len(couples)."""
    tailles = list(range(pas, len(couples), pas)) + [len(couples)]
    res = []
    for n in tailles:
        phi = commun.construire_phi(couples[:n])
        res.append((n, ecart_median(phi, ref), len(phi)))
    return res


def trace_phi(ax, phi, style, label):
    ax.plot([(k + 0.5) * commun.LARGEUR_TRANCHE for k in phi], list(phi.values()), style, ms=4, lw=2, label=label)


if __name__ == "__main__":
    donnees = p08.points_par_eolienne()
    utiles = {e: [(a, b) for a, b, _ in pts if not commun.est_arret(a, b)] for e, pts in donnees.items()}
    # E1 à E5 dans l'ordre chronologique commun (seq est partagé par toutes les éoliennes)
    ensemble = [(a, b) for a, b, m in sorted((p for pts in donnees.values() for p in pts), key=lambda p: p[2]["seq"])
                if not commun.est_arret(a, b)]
    phi_ref = commun.construire_phi(ensemble)
    phi_e1 = commun.construire_phi(utiles["E1"])

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.scatter([b for _, b in utiles["E1"]], [a for a, _ in utiles["E1"]], s=5, alpha=0.3, color="tab:blue",
               label=f"mesures de E1 (n = {len(utiles['E1'])})")
    trace_phi(ax, phi_e1, "b-o", r"$\Phi$ construit sur E1 seule")
    trace_phi(ax, phi_ref, "k--s", r"$\Phi$ de référence (E1 à E5)")
    ax.set_xlabel(r"$\Pi_2 = \omega D / V$")
    ax.set_ylabel(r"$\Pi_1 = P / (\rho V^3 D^2)$")
    ax.set_title(r"E1 seule : nuage $\Pi_1(\Pi_2)$, abaque E1 et abaque de référence")
    ax.grid(alpha=0.3)
    ax.legend(markerscale=2)
    fig.tight_layout()
    fig.savefig(commun.GRAPHIQUES / "phi_e1.png", dpi=150)
    print(f"Φ E1 seule : {len(phi_e1)} tranches, écart médian à la référence = {ecart_median(phi_e1, phi_ref):.2%}")

    conv_e1 = courbe_convergence(utiles["E1"], phi_ref)
    conv_tous = courbe_convergence(ensemble, phi_ref)
    with open(commun.TABLEAUX / "convergence.csv", "w", encoding="utf-8") as f:
        f.write("source;N;ecart_median;nb_tranches\n")
        for nom, conv in (("E1", conv_e1), ("E1-E5", conv_tous)):
            for n, e, k in conv:
                f.write(f"{nom};{n};{'' if e is None else f'{e:.5f}'};{k}\n")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 7), sharex=True, gridspec_kw={"height_ratios": [2, 1]})
    for nom, conv, style in (("E1 seule", conv_e1, "b-"), ("E1 à E5", conv_tous, "k-")):
        ax1.plot([n for n, e, _ in conv if e is not None], [100 * e for _, e, _ in conv if e is not None], style, label=nom)
        ax2.plot([n for n, _, _ in conv], [k for _, _, k in conv], style, label=nom)
    ax1.axhline(5, color="grey", ls=":", label="5 %")
    ax1.axhline(1, color="grey", ls="--", label="1 %")
    ax1.set_ylabel("écart médian à Φ de référence (%)")
    ax1.set_title(r"Convergence de $\Phi$ en fonction du nombre N de mesures utilisées")
    ax1.set_yscale("log")
    ax1.grid(alpha=0.3, which="both")
    ax1.legend()
    ax2.axhline(len(phi_ref), color="grey", ls=":", label=f"tranches de la référence ({len(phi_ref)})")
    ax2.set_ylabel("tranches retenues")
    ax2.set_xlabel("N (premières mesures utiles, hors points d'arrêt)")
    ax2.set_xscale("log")
    ax2.grid(alpha=0.3, which="both")
    ax2.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(commun.GRAPHIQUES / "convergence.png", dpi=150)
    for nom, conv in (("E1 seule", conv_e1), ("E1 à E5", conv_tous)):
        for seuil in (0.05, 0.02):
            stable = next((n for i, (n, _, _) in enumerate(conv)
                           if all(e is not None and e < seuil for _, e, _ in conv[i:])), None)
            print(f"{nom} : écart médian durablement < {seuil:.0%} à partir de N = {stable}")
        print(f"{nom} : écart final = {conv[-1][1]:.2%} avec N = {conv[-1][0]}, {conv[-1][2]} tranches")
    print("-> results/graphiques/phi_e1.png, results/graphiques/convergence.png, results/tableaux/convergence.csv")
