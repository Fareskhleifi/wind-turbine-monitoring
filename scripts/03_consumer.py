"""Partie 03 — Relire Kafka et tracer.

Relit depuis le début tout ferme.eoliennes, garde E1 et E2 et trace la puissance en fonction
du vent pour les deux éoliennes sur un même graphique : results/graphiques/puissance_vent.png
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

import commun  # noqa: E402

mesures = commun.lire_topic(commun.TOPIC_MESURES, cles={"E1", "E2"})
fig, ax = plt.subplots(figsize=(8, 5))
for nom, couleur in (("E1", "tab:blue"), ("E2", "tab:orange")):
    pts = [(m["vent_ms"], m["puissance_w"]) for m in mesures
           if m.get("eolienne") == nom and m.get("vent_ms") is not None and m.get("puissance_w") is not None]
    if not pts:
        continue
    diametre = next(m["diametre_m"] for m in mesures if m.get("eolienne") == nom)
    ax.scatter(*zip(*pts), s=8, alpha=0.5, color=couleur, label=f"{nom} (D = {diametre:g} m, {len(pts)} mesures)")
    print(f"{nom} : {len(pts)} mesures, D = {diametre} m, P max = {max(p for _, p in pts):.0f} W")
ax.set_xlabel("Vitesse du vent V (m/s)")
ax.set_ylabel("Puissance électrique P (W)")
ax.set_title("Puissance en fonction du vent — E1 et E2 (relu depuis ferme.eoliennes)")
ax.grid(alpha=0.3)
ax.legend()
fig.tight_layout()
fig.savefig(commun.GRAPHIQUES / "puissance_vent.png", dpi=150)
print("-> results/graphiques/puissance_vent.png")
