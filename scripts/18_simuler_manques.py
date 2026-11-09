"""Partie 18 — Données manquantes (simulation).

Lit en continu les nouvelles mesures de E1 à E5 dans ferme.eoliennes et les republie dans ce même topic
sous les noms EX1 à EX5, chacune abîmée d'une façon différente :
  EX1 (copie de E1) : mesures perdues — 2 mesures sur 3 en moyenne ne sont pas republiées
  EX2 (copie de E2) : champ puissance_w absent dans 2 mesures sur 3 en moyenne
  EX3 (copie de E3) : champ vent_ms absent dans 2 mesures sur 3 en moyenne
  EX4 (copie de E4) : champ rho_kgm3 absent dans 2 mesures sur 3 en moyenne
  EX5 (copie de E5) : silence — 2 min d'émission normale puis 3 min sans aucune mesure, en boucle
Les données EX* sont SIMULÉES à partir de mesures réelles ; elles ne doivent pas être confondues avec E1-E5.
"""
import json
import random
import time
import uuid

from confluent_kafka import Consumer

import commun

PROBA_DEFAUT = 2 / 3
CYCLE_SILENCE_S, EMISSION_S = 300, 120
SOURCES = {f"E{i}": f"EX{i}" for i in range(1, 6)}
CHAMP_SUPPRIME = {"EX2": "puissance_w", "EX3": "vent_ms", "EX4": "rho_kgm3"}


def abimer(nom, mesure, maintenant, alea):
    """Renvoie la mesure abîmée à republier, ou None si elle doit être perdue."""
    mesure = dict(mesure, eolienne=nom)
    if nom == "EX1" and alea.random() < PROBA_DEFAUT:
        return None
    if nom in CHAMP_SUPPRIME and alea.random() < PROBA_DEFAUT:
        mesure.pop(CHAMP_SUPPRIME[nom], None)
    if nom == "EX5" and maintenant % CYCLE_SILENCE_S >= EMISSION_S:
        return None
    return mesure


if __name__ == "__main__":
    alea = random.Random(18)
    conso = Consumer({"bootstrap.servers": commun.KAFKA_BOOTSTRAP, "group.id": f"simulation-{uuid.uuid4().hex[:6]}",
                      "auto.offset.reset": "latest", "enable.auto.commit": False})
    conso.subscribe([commun.TOPIC_MESURES])
    producteur = commun.producteur_kafka()
    compte = {n: [0, 0] for n in SOURCES.values()}   # [reçues, republiées]
    dernier_bilan = time.time()
    try:
        while True:
            for m in conso.consume(500, timeout=1.0):
                if m.error() or m.key() is None or m.key().decode() not in SOURCES:
                    continue           # on ne relit jamais nos propres EX* (pas de boucle)
                nom = SOURCES[m.key().decode()]
                mesure = commun.decoder(m.value())
                if mesure is None:
                    continue
                compte[nom][0] += 1
                abimee = abimer(nom, mesure, time.time(), alea)
                if abimee is not None:
                    commun.envoyer(producteur, commun.TOPIC_MESURES, nom.encode(), json.dumps(abimee).encode())
                    compte[nom][1] += 1
            if time.time() - dernier_bilan > 30:
                dernier_bilan = time.time()
                silence = time.time() % CYCLE_SILENCE_S >= EMISSION_S
                print(f"{time.strftime('%H:%M:%S')} " + "  ".join(f"{n}: {r}/{t}" for n, (t, r) in compte.items())
                      + f"  (EX5 {'en silence' if silence else 'émet'})", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        producteur.flush(10)
        conso.close()
