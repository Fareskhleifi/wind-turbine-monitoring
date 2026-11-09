"""Partie 13 — Surveiller E6 à E15.

Lit en continu ferme.eoliennes.pi, garde les 60 dernières mesures (Π1, Π2) de chaque éolienne surveillée
et, toutes les 10 s, lui attribue un état (HORS_ABAQUE, SOUS_PRODUCTION, SUR_PRODUCTION, OK) ;
une alerte est publiée dans ferme.alertes à chaque changement d'état (y compris le premier état connu).

Usage : python scripts/13_surveiller.py [--eoliennes E6,...,E15] [--duree 0]
"""
import argparse
import collections
import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer

import commun

TAILLE_FENETRE = 60
PERIODE_S = 10


def surveiller(capteur=False, nom="13"):
    parser = argparse.ArgumentParser()
    parser.add_argument("--eoliennes", default=",".join(commun.SURVEILLEES))
    parser.add_argument("--duree", type=float, default=0, help="arrêt après N secondes (0 = sans fin)")
    args = parser.parse_args()
    suivies = args.eoliennes.split(",")

    phi = commun.charger_phi()
    conso = Consumer({"bootstrap.servers": commun.KAFKA_BOOTSTRAP, "group.id": f"surveillance-{nom}-{uuid.uuid4().hex[:6]}",
                      "auto.offset.reset": "earliest", "enable.auto.commit": False})
    conso.subscribe([commun.TOPIC_PI])
    producteur = commun.producteur_kafka()
    fenetres = {e: collections.deque(maxlen=TAILLE_FENETRE) for e in suivies}
    derniere = {e: None for e in suivies}
    etats = {e: None for e in suivies}
    debut = time.time()
    prochain = debut + PERIODE_S          # 1re évaluation une fois l'historique du topic relu
    try:
        while not args.duree or time.time() - debut < args.duree:
            lot = conso.consume(5000, timeout=0.5)
            if len(lot) == 5000:
                prochain = max(prochain, time.time() + 1)   # retard en cours de rattrapage : évaluation repoussée
            for m in lot:
                if m.error() or m.key() is None:
                    continue
                e = m.key().decode()
                v = commun.decoder(m.value())
                if e in fenetres and v is not None:
                    fenetres[e].append((v.get("pi1"), v.get("pi2")))
                    derniere[e] = v.get("horodatage")
            if time.time() < prochain:
                continue
            prochain += PERIODE_S
            print(f"--- {datetime.now(timezone.utc):%H:%M:%S} UTC")
            for e in suivies:
                if not fenetres[e]:
                    print(f"  {e:4s} aucune mesure reçue")
                    continue
                nouvel, med, suspects = commun.etat(list(fenetres[e]), phi, capteur=capteur)
                hors = sum(1 for _, b in fenetres[e] if commun.tranche(b) not in phi)
                med_txt = f"{med:.3f}" if med is not None else "  -  "
                print(f"  {e:4s} {nouvel:16s} médiane r = {med_txt}  hors abaque = {hors:2d}/{len(fenetres[e])}"
                      f"  r>2 = {suspects}  dernière mesure = {derniere[e]}")
                if nouvel != etats[e]:
                    alerte = {"eolienne": e, "ancien_etat": etats[e], "nouvel_etat": nouvel,
                              "mediane_r": med, "hors_abaque": hors, "r_sup_2": suspects,
                              "taille_fenetre": len(fenetres[e]), "derniere_mesure": derniere[e],
                              "emise_a": datetime.now(timezone.utc).isoformat(), "source": f"{nom}_surveiller"}
                    commun.envoyer(producteur, commun.TOPIC_ALERTES, e.encode(), json.dumps(alerte).encode())
                    print(f"  ALERTE {e} : {etats[e]} -> {nouvel}")
                    etats[e] = nouvel
            producteur.flush(5)
    except KeyboardInterrupt:
        pass
    finally:
        producteur.flush(10)
        conso.close()


if __name__ == "__main__":
    surveiller(capteur=False, nom="13")
