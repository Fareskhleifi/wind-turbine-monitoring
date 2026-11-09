"""Partie 01 — Écouter le broker.

Écoute tous les topics (#) pendant DUREE secondes et produit :
  data/topics.jsonl  : un objet par topic rencontré (une seule fois) avec un exemple de message
  data/mesures.jsonl : toutes les mesures d'éoliennes (topics .../Ei/mesures)
Usage : python scripts/01_mqtt.py [durée_s]   (défaut 20 s)
"""
import json
import sys
import time

import commun

DUREE = float(sys.argv[1]) if len(sys.argv) > 1 else 20
topics_vus = {}
mesures = []


def on_connect(client, userdata, flags, rc, props):
    print("Connexion :", rc)
    if not rc.is_failure:
        client.subscribe("#")


def on_message(client, userdata, msg):
    texte = msg.payload.decode("utf-8", errors="replace")
    if msg.topic not in topics_vus:
        topics_vus[msg.topic] = texte
    if msg.topic.endswith("/mesures"):
        mesure = commun.decoder(msg.payload)
        if mesure is not None:
            mesures.append(mesure)


client = commun.creer_client_mqtt("p01")
client.on_connect = on_connect
client.on_message = on_message
commun.connecter_mqtt(client)
client.loop_start()
time.sleep(DUREE)
client.loop_stop()
client.disconnect()

with open(commun.DATA / "topics.jsonl", "w", encoding="utf-8") as f:
    for topic, exemple in sorted(topics_vus.items()):
        f.write(json.dumps({"topic": topic, "exemple": commun.decoder(exemple.encode()) or exemple},
                           ensure_ascii=False) + "\n")
with open(commun.DATA / "mesures.jsonl", "w", encoding="utf-8") as f:
    for m in mesures:
        f.write(json.dumps(m, ensure_ascii=False) + "\n")
print(f"{len(topics_vus)} topics -> data/topics.jsonl ; {len(mesures)} mesures -> data/mesures.jsonl")
