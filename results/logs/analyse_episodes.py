"""Analyse ponctuelle (rapport, parties 15-16) : épisodes de r > 1,5 par éolienne surveillée."""
import sys
sys.path.insert(0, "scripts")
import commun
phi = commun.charger_phi()
pts = commun.lire_topic(commun.TOPIC_PI, cles={f"E{i}" for i in range(6, 16)})
par = {}
for v in pts:
    r = commun.ecart(phi, v.get("pi1"), v.get("pi2"))
    if r is not None:
        par.setdefault(v["eolienne"], []).append((v["seq"], v["horodatage"][11:19], r))
for e in sorted(par, key=lambda x: int(x[1:])):
    s = sorted(par[e])
    sup2 = sum(1 for _, _, r in s if r > 2)
    episodes, cur = [], None
    for q, h, r in s:
        if r > 1.5:
            if cur and q - cur[1] <= 5: cur[1], cur[3] = q, cur[3] + 1
            else:
                cur = [q, q, h, 1]; episodes.append(cur)
    longs = [c for c in episodes if c[3] >= 5]
    print(f"{e:4s} n={len(s):5d}  r>2 : {sup2:4d}  isolés r>1.5 : {len(episodes)-len(longs):3d}  épisodes (>=5 mesures) : {len(longs)}")
    for c in longs:
        print(f"      début {c[2]} UTC  durée ~{c[1]-c[0]+1:3d} s  mesures r>1.5 : {c[3]}")
