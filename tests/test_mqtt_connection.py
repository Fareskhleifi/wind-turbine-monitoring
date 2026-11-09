"""Diagnostic de la connexion MQTT (DNS, TCP, TLS, WebSocket, authentification, réception).

Usage : python tests/test_mqtt_connection.py [durée_s]
Le mot de passe n'est jamais affiché.
"""
import os
import socket
import ssl
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import certifi  # noqa: E402
import commun  # noqa: E402

DUREE = float(sys.argv[1]) if len(sys.argv) > 1 else 10
hote, port = os.environ["MQTT_HOTE"], int(os.environ["MQTT_PORT"])


def etape(nom, ok, detail=""):
    print(f"[{'OK ' if ok else 'ÉCHEC'}] {nom} {detail}")
    if not ok:
        sys.exit(1)


ip = socket.gethostbyname(hote)
etape("DNS", True, f"{hote} -> {ip}")
with socket.create_connection((hote, port), timeout=10) as s:
    etape("TCP", True, f"port {port} ouvert")
    ctx = ssl.create_default_context(cafile=certifi.where())
    with ctx.wrap_socket(s, server_hostname=hote) as t:
        etape("TLS", True, f"{t.version()}, certificat valide pour {t.getpeercert()['subject'][0][0][1]}")

etat = {"rc": None, "messages": {}, "n": 0}
client = commun.creer_client_mqtt("diag")
client.on_connect = lambda c, u, f, rc, p: (etat.update(rc=rc), c.subscribe("#") if not rc.is_failure else None)
client.on_subscribe = lambda c, u, mid, rcs, p: print("     abonnement '#' :", [str(r) for r in rcs])


def on_message(c, u, msg):
    etat["n"] += 1
    etat["messages"].setdefault(msg.topic, msg.payload[:300])


client.on_message = on_message
print(f"     identifiant : {os.environ['MQTT_IDENTIFIANT']}, mot de passe : {'*' * 8} (masqué)")
commun.connecter_mqtt(client)
client.loop_start()
debut = time.time()
while etat["rc"] is None and time.time() - debut < 15:
    time.sleep(0.2)
etape("WebSocket + authentification", etat["rc"] is not None and not etat["rc"].is_failure, f"CONNACK = {etat['rc']}")
time.sleep(DUREE)
client.loop_stop()
client.disconnect()
etape("Réception", etat["n"] > 0, f"{etat['n']} messages en {DUREE:.0f} s sur {len(etat['messages'])} topics")
for topic, exemple in sorted(etat["messages"].items()):
    print(f"  {topic:40s} {exemple.decode(errors='replace')[:160]}")
