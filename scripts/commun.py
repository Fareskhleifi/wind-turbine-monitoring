"""Fonctions partagées par les scripts du TD3 (configuration, MQTT, Kafka, groupes Π).

Les identifiants sont lus dans le fichier .env (jamais écrits dans le code).
"""
import json
import os
import ssl
import uuid
from pathlib import Path

import certifi
from dotenv import load_dotenv

RACINE = Path(__file__).resolve().parent.parent
load_dotenv(RACINE / ".env")

DATA = RACINE / "data"
GRAPHIQUES = RACINE / "results" / "graphiques"
TABLEAUX = RACINE / "results" / "tableaux"
LOGS = RACINE / "results" / "logs"
for _d in (DATA, GRAPHIQUES, TABLEAUX, LOGS):
    _d.mkdir(parents=True, exist_ok=True)

# Dans le conteneur app, docker-compose fixe KAFKA_BOOTSTRAP=kafka:9092 ; depuis le PC : localhost:9094
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_MESURES = "ferme.eoliennes"
TOPIC_PI = "ferme.eoliennes.pi"
TOPIC_ALERTES = "ferme.alertes"
TOPIC_TABLEAU = "ferme.tableau"

R_AIR = 287.05  # J/(kg·K), constante spécifique de l'air sec (énoncé, partie 04)

REFERENCE = [f"E{i}" for i in range(1, 6)]       # éoliennes servant à construire Φ
SURVEILLEES = [f"E{i}" for i in range(6, 16)]    # éoliennes surveillées (partie 13)


def creer_client_mqtt(suffixe="td3"):
    """Client paho-mqtt 2.x configuré à partir du .env (WebSocket + TLS)."""
    import paho.mqtt.client as mqtt

    identifiant = os.environ["MQTT_IDENTIFIANT"]
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        # identifiant de session unique : deux scripts ne s'éjectent pas mutuellement du broker
        client_id=f"{identifiant}-{suffixe}-{uuid.uuid4().hex[:6]}",
        transport=os.getenv("MQTT_TRANSPORT", "websockets"),
    )
    client.username_pw_set(identifiant, os.environ["MQTT_MDP"])
    if os.getenv("MQTT_TLS", "1") == "1":
        client.tls_set(ca_certs=certifi.where(), tls_version=ssl.PROTOCOL_TLS_CLIENT)
    if client.transport == "websockets":
        client.ws_set_options(path=os.getenv("MQTT_CHEMIN_WS", "/mqtt"))
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def connecter_mqtt(client):
    client.connect(os.environ["MQTT_HOTE"], int(os.getenv("MQTT_PORT", "443")), keepalive=30)


def decoder(payload):
    """Décode un message JSON ; renvoie None s'il est invalide (message ignoré, jamais d'exception)."""
    try:
        valeur = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    return valeur if isinstance(valeur, dict) else None


# --------------------------------------------------------------------------- Kafka
def producteur_kafka():
    from confluent_kafka import Producer

    # acks=all + idempotence : un message accusé par Kafka est écrit, sans doublon en cas de renvoi
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all",
                     "enable.idempotence": True, "linger.ms": 20})


def envoyer(producteur, topic, cle, valeur):
    """Envoie un message ; si la file locale est pleine, on laisse partir des messages et on réessaie."""
    while True:
        try:
            producteur.produce(topic, key=cle, value=valeur)
            producteur.poll(0)
            return
        except BufferError:
            producteur.poll(0.5)


def lire_topic(topic, cles=None, delai_fin=5.0):
    """Relit un topic depuis le début jusqu'à sa fin actuelle.

    Renvoie une liste de dict (valeur JSON décodée), éventuellement filtrée sur les clés.
    """
    from confluent_kafka import Consumer, TopicPartition

    conso = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": f"relecture-{uuid.uuid4().hex}",
                      "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta = conso.list_topics(topic, timeout=10).topics[topic]
    fins = {}
    for p in meta.partitions:
        bas, haut = conso.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if haut > bas:
            fins[p] = haut
    conso.assign([TopicPartition(topic, p, -2) for p in fins])  # -2 = OFFSET_BEGINNING
    resultat, attente = [], 0.0
    while fins and attente < delai_fin:
        msgs = conso.consume(10000, timeout=1.0)
        attente = 0.0 if msgs else attente + 1.0
        for m in msgs:
            if m.error():
                continue
            if m.offset() + 1 >= fins.get(m.partition(), 0):
                fins.pop(m.partition(), None)
            cle = m.key().decode() if m.key() else None
            if cles is not None and cle not in cles:
                continue
            valeur = decoder(m.value())
            if valeur is not None:
                resultat.append(valeur)
    conso.close()
    return resultat


# --------------------------------------------------------------------------- Pont MQTT -> Kafka
TOPIC_MQTT_MESURES = "ferme/+/+/+/mesures"   # uniquement les mesures d'éoliennes


def pont(transformer=None, suffixe="pont"):
    """Recopie en continu chaque mesure MQTT dans ferme.eoliennes (clé = nom de l'éolienne).

    transformer(bytes) -> bytes : modification éventuelle du message (partie 04) ; None = message intact.
    Arrêt par Ctrl+C : on se déconnecte de MQTT puis flush() attend l'accusé de tous les messages en file.
    """
    import signal
    import time

    producteur = producteur_kafka()
    compteur = {"recus": 0, "invalides": 0}

    def on_connect(client, userdata, flags, rc, props):
        print("MQTT connecté :", rc, flush=True)
        if not rc.is_failure:
            client.subscribe(TOPIC_MQTT_MESURES, qos=1)

    def on_disconnect(client, userdata, flags, rc, props):
        print("MQTT déconnecté :", rc, "(reconnexion automatique)", flush=True)

    def on_message(client, userdata, msg):
        eolienne = msg.topic.split("/")[-2]           # ferme/<site>/<zone>/<Ei>/mesures
        valeur = msg.payload
        if transformer is not None:
            valeur = transformer(valeur)
            if valeur is None:
                compteur["invalides"] += 1
                return
        envoyer(producteur, TOPIC_MESURES, eolienne.encode(), valeur)
        compteur["recus"] += 1
        if compteur["recus"] % 1000 == 0:
            print(f"{compteur['recus']} mesures envoyées ({compteur['invalides']} invalides)", flush=True)

    client = creer_client_mqtt(suffixe)
    client.on_connect, client.on_disconnect, client.on_message = on_connect, on_disconnect, on_message
    arret = {"demande": False}
    signal.signal(signal.SIGTERM, lambda *a: arret.update(demande=True))
    connecter_mqtt(client)
    client.loop_start()
    try:
        while not arret["demande"]:
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    client.loop_stop()       # plus aucun nouveau message accepté
    client.disconnect()
    restants = producteur.flush(30)
    print(f"Arrêt : {compteur['recus']} mesures transmises, {restants} non accusées", flush=True)


# --------------------------------------------------------------------------- Physique
def rho(pression_hpa, temp_c):
    """Masse volumique de l'air ρ = P / (R_air·T), P en Pa et T en K -> kg/m³."""
    return pression_hpa * 100.0 / (R_AIR * (temp_c + 273.15))


def groupes_pi(m):
    """Π1 = P/(ρ V³ D²), Π2 = ω D / V ; None si une grandeur manque ou si V = 0."""
    try:
        p, v, d, w, r = (float(m[k]) for k in ("puissance_w", "vent_ms", "diametre_m", "rotation_rads", "rho_kgm3"))
    except (KeyError, TypeError, ValueError):
        return None, None
    if v <= 0 or d <= 0 or r <= 0:
        return None, None
    return p / (r * v**3 * d**2), w * d / v


# --------------------------------------------------------------------------- Abaque Φ
LARGEUR_TRANCHE = 0.5
MIN_MESURES_TRANCHE = 20
MARGE = 0.10
SEUIL_CAPTEUR = 2.0


def manquant(x):
    """Valeur absente : None (JSON) ou NaN (pandas, qui convertit les champs absents en NaN)."""
    return x is None or x != x


def tranche(pi2):
    """Indice de la tranche de Π2 (largeur 0,5) ; None si Π2 manque."""
    import math
    return None if manquant(pi2) else math.floor(pi2 / LARGEUR_TRANCHE)


def construire_phi(couples):
    """couples : liste de (Π1, Π2) hors points (0,0). Renvoie {indice_tranche: médiane Π1}."""
    import statistics
    par_tranche = {}
    for pi1, pi2 in couples:
        par_tranche.setdefault(tranche(pi2), []).append(pi1)
    return {k: statistics.median(v) for k, v in sorted(par_tranche.items()) if len(v) >= MIN_MESURES_TRANCHE}


def est_arret(pi1, pi2):
    """Point (0,0) : éolienne à l'arrêt (rotor immobile, puissance nulle)."""
    return pi1 == 0 and pi2 == 0


def charger_phi():
    with open(TABLEAUX / "phi.json", encoding="utf-8") as f:
        table = json.load(f)
    return {int(t["tranche"]): t["phi"] for t in table["tranches"]}


def ecart(phi, pi1, pi2):
    """r = Π1/Φ(Π2) ; None si Π1/Π2 manquant ou Π2 hors abaque."""
    if manquant(pi1) or manquant(pi2):
        return None
    ref = phi.get(tranche(pi2))
    return pi1 / ref if ref else None


def etat(fenetre, phi, capteur=False):
    """État d'une éolienne sur sa fenêtre de mesures [(Π1, Π2), ...] (parties 13 et 16).

    Priorité : HORS_ABAQUE > CAPTEUR (si activé) > SOUS_PRODUCTION > SUR_PRODUCTION > OK.
    Renvoie (état, médiane de r ou None, nombre de r > 2).
    """
    import statistics
    hors = sum(1 for _, pi2 in fenetre if tranche(pi2) not in phi)   # Π2 absent compté hors abaque
    rs = [r for r in (ecart(phi, a, b) for a, b in fenetre) if r is not None]
    mediane = statistics.median(rs) if rs else None
    suspects = sum(1 for r in rs if r > SEUIL_CAPTEUR)
    if hors > len(fenetre) / 2:
        return "HORS_ABAQUE", mediane, suspects
    if capteur and suspects > 0:
        return "CAPTEUR", mediane, suspects
    if mediane is not None and mediane < 1 - MARGE:
        return "SOUS_PRODUCTION", mediane, suspects
    if mediane is not None and mediane > 1 + MARGE:
        return "SUR_PRODUCTION", mediane, suspects
    return "OK", mediane, suspects
