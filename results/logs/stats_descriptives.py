"""Statistiques descriptives des mesures réelles enrichies (rapport, parties 03, 04, 08, 10)."""
import collections, statistics, sys
sys.path.insert(0, "scripts")
import commun
M = [m for m in commun.lire_topic(commun.TOPIC_MESURES) if "rho_kgm3" in m and m.get("eolienne", "").startswith("E") and not m["eolienne"].startswith("EX")]
print(f"mesures enrichies E1-E15 : {len(M)} ; horodatages {min(m['horodatage'] for m in M)[11:19]} -> {max(m['horodatage'] for m in M)[11:19]} UTC")
par = collections.defaultdict(list)
for m in M: par[m["eolienne"]].append(m)
print("éol. zone     D(m)  n     V moy  V max  rho min  rho max  arrêts")
for e in sorted(par, key=lambda x: int(x[1:])):
    L = par[e]
    arr = sum(1 for m in L if m["rotation_rads"] == 0 and m["puissance_w"] == 0)
    print(f"{e:4s} {L[0]['zone']:8s} {L[0]['diametre_m']:4.0f} {len(L):5d} {statistics.fmean(m['vent_ms'] for m in L):6.2f} {max(m['vent_ms'] for m in L):6.2f}"
          f"  {min(m['rho_kgm3'] for m in L):.4f}  {max(m['rho_kgm3'] for m in L):.4f}  {arr/len(L):5.1%}")
arr = [m for m in M if m["rotation_rads"] == 0]
print(f"vent max quand rotor à l'arrêt : {max(m['vent_ms'] for m in arr):.2f} m/s ; vent min rotor tournant : {min(m['vent_ms'] for m in M if m['rotation_rads'] > 0):.2f} m/s")
for v0 in (6, 8, 10):
    sel = lambda e: [m["puissance_w"] for m in par[e] if abs(m["vent_ms"] - v0) < 0.25]
    p1, p2 = statistics.median(sel("E1")), statistics.median(sel("E2"))
    print(f"V = {v0} ± 0,25 m/s : médiane P(E1) = {p1:.0f} W, P(E2) = {p2:.0f} W, rapport E2/E1 = {p2/p1:.2f}")
pi2 = [commun.groupes_pi(m)[1] for m in M if m["rotation_rads"] > 0]
pi1 = [commun.groupes_pi(m)[0] for m in M if m["rotation_rads"] > 0]
print(f"Π2 rotor tournant : {min(pi2):.2f} -> {max(pi2):.2f} ; Π1 max = {max(pi1):.4f} (Betz : {2*3.14159265/27:.4f})")
