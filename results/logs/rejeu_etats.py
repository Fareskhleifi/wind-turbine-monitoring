"""Rejeu hors ligne des règles des parties 13/16 sur tout l'historique de ferme.eoliennes.pi.
Fenêtre = 60 dernières mesures, évaluée toutes les 10 mesures (≈ toutes les 10 s, comme en direct)."""
import collections, sys
sys.path.insert(0, "scripts")
import commun
phi = commun.charger_phi()
noms = [f"E{i}" for i in range(6, 16)]
par = collections.defaultdict(list)
for v in commun.lire_topic(commun.TOPIC_PI, cles=set(noms)):
    if "seq" in v:
        par[v["eolienne"]].append((v["seq"], v.get("pi1"), v.get("pi2")))
print(f"{'':5s}{'éval.':>6s} | partie 13 : " + " ".join(f"{s[:8]:>9s}" for s in ("OK", "SOUS_PRODUCTION", "SUR_PRODUCTION", "HORS_ABAQUE"))
      + " | partie 16 : CAPTEUR")
for e in noms:
    s = [(a, b) for _, a, b in sorted(par[e])]
    c13, c16 = collections.Counter(), collections.Counter()
    n = 0
    for i in range(60, len(s) + 1, 10):
        w = s[i - 60:i]
        c13[commun.etat(w, phi)[0]] += 1
        c16[commun.etat(w, phi, capteur=True)[0]] += 1
        n += 1
    print(f"{e:5s}{n:6d} | " + " ".join(f"{100*c13[k]/n:8.1f}%" for k in ("OK", "SOUS_PRODUCTION", "SUR_PRODUCTION", "HORS_ABAQUE"))
          + f" | {100*c16['CAPTEUR']/n:6.1f}%")
