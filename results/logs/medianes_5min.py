"""Médiane de r par tranche de 5 minutes (heure capteur) pour les éoliennes surveillées."""
import collections, statistics, sys
sys.path.insert(0, "scripts")
import commun
phi = commun.charger_phi()
noms = [f"E{i}" for i in range(6, 16)]
d = collections.defaultdict(lambda: collections.defaultdict(list))
for v in commun.lire_topic(commun.TOPIC_PI, cles=set(noms)):
    r = commun.ecart(phi, v.get("pi1"), v.get("pi2"))
    if r is not None:
        h = v["horodatage"][11:16]
        d[v["eolienne"]][h[:3] + f"{int(h[3:]) // 5 * 5:02d}"].append(r)
blocs = sorted({b for e in d for b in d[e]})
print("éol. " + " ".join(f"{b:>6s}" for b in blocs))
for e in noms:
    print(f"{e:5s}" + " ".join(f"{statistics.median(d[e][b]):6.3f}" if d[e][b] else "     -" for b in blocs))
