"""Lecture continue de Kafka pour le tableau de bord, et résumés calculés avec les règles de commun.py.

Trois fils de fond lisent depuis le début :
  ferme.eoliennes.pi -> (horodatage, seq, Π1, Π2) des 3600 dernières mesures par éolienne (r, états)
  ferme.eoliennes    -> 900 dernières mesures brutes par éolienne (grandeurs physiques réelles)
  ferme.alertes      -> 300 dernières alertes publiées par les parties 13 et 16
"""
import collections
import statistics
import threading
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer

import commun

HISTORIQUE_PI = 3600        # 1 h à 1 mesure/s
HISTORIQUE_BRUT = 900       # 15 min de grandeurs physiques
FENETRE = 60                # fenêtre des parties 13 et 16
REELLES = [f"E{i}" for i in range(1, 16)]
SIMULEES = [f"EX{i}" for i in range(1, 6)]
TOUTES = REELLES + SIMULEES
CHAMPS_BRUTS = ("horodatage", "seq", "zone", "diametre_m", "vent_ms", "temp_c", "pression_hpa",
                "rotation_rads", "puissance_w", "rho_kgm3")


class Flux:
    """Tampons partagés alimentés par les consommateurs Kafka (un fil par topic)."""

    def __init__(self):
        self.verrou = threading.Lock()
        self.pi = collections.defaultdict(lambda: collections.deque(maxlen=HISTORIQUE_PI))
        self.brut = collections.defaultdict(lambda: collections.deque(maxlen=HISTORIQUE_BRUT))
        self.alertes = collections.deque(maxlen=300)
        self.recus = collections.Counter()          # mesures Π reçues par éolienne (non plafonné)
        self.messages = collections.Counter()       # messages lus par topic
        self.reception = {}                         # topic -> heure murale de la dernière réception
        self.erreur = None
        # Π et alertes depuis le début (historique d'1 h, compteurs exacts) ; mesures brutes : seulement la fin du
        # topic, qui suffit à remplir les 900 dernières mesures par éolienne (premier chargement bien plus rapide)
        for topic, ranger, fin in ((commun.TOPIC_PI, self._ranger_pi, None), (commun.TOPIC_MESURES, self._ranger_brut, 3000),
                                   (commun.TOPIC_ALERTES, self._ranger_alerte, None)):
            threading.Thread(target=self._consommer, args=(topic, ranger, fin), daemon=True).start()

    def _ranger_pi(self, cle, v):
        self.pi[cle].append((v.get("horodatage"), v.get("seq"), v.get("pi1"), v.get("pi2")))
        self.recus[cle] += 1

    def _ranger_brut(self, cle, v):
        self.brut[cle].append({k: v.get(k) for k in CHAMPS_BRUTS})

    def _ranger_alerte(self, cle, v):
        self.alertes.append(v)

    def _consommer(self, topic, ranger, derniers=None):
        """derniers = N : chaque partition est lue à partir de ses N derniers messages (sinon depuis le début)."""
        def positionner(conso, partitions):
            for p in partitions:
                bas, haut = conso.get_watermark_offsets(p, timeout=10)
                p.offset = max(bas, haut - derniers)
            conso.assign(partitions)

        while True:
            try:
                conso = Consumer({"bootstrap.servers": commun.KAFKA_BOOTSTRAP,
                                  "group.id": f"tableau-{topic}-{uuid.uuid4().hex[:6]}",
                                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
                if derniers:
                    conso.subscribe([topic], on_assign=positionner)
                else:
                    conso.subscribe([topic])
                while True:
                    lot = conso.consume(2000, timeout=1.0)
                    erreurs = [f"{topic} : {m.error()}" for m in lot if m.error()]
                    decodes = [(m.key().decode(), commun.decoder(m.value())) for m in lot if not m.error() and m.key()]
                    with self.verrou:                    # décodage fait hors verrou : la page n'attend pas
                        if erreurs:
                            self.erreur = erreurs[-1]
                        for cle, v in decodes:
                            if v is not None:
                                ranger(cle, v)
                                self.messages[topic] += 1
                        if lot:
                            self.reception[topic] = time.time()
            except Exception as exc:  # broker indisponible : on le signale et on réessaie
                with self.verrou:
                    self.erreur = f"{topic} : {exc}"
                time.sleep(5)

    def statut(self):
        """État de la connexion (léger) : dernière réception par topic, compteurs, dernière erreur."""
        with self.verrou:
            return {"reception": dict(self.reception), "messages": dict(self.messages), "erreur": self.erreur}

    def instantane(self):
        """Copie cohérente des tampons, pour calculer sans bloquer les consommateurs."""
        with self.verrou:
            return {"pi": {e: list(d) for e, d in self.pi.items()},
                    "brut": {e: list(d) for e, d in self.brut.items()},
                    "alertes": list(self.alertes), "recus": dict(self.recus),
                    "messages": dict(self.messages), "reception": dict(self.reception), "erreur": self.erreur}


def resume(eolienne, points, recus, phi, maintenant=None):
    """État d'une éolienne selon les règles des parties 13/16 (commun.etat, capteur=True) sur ses 60 dernières mesures.

    maintenant (Timestamp UTC) : si fourni, age_s = âge de la dernière mesure, pour signaler un état figé.
    """
    if not points:
        return {"eolienne": eolienne, "etat": "AUCUNE_DONNEE", "mediane": None, "suspects": 0, "hors": 0,
                "fenetre": 0, "recus": 0, "derniere": None, "suspects_historique": 0, "age_s": None}
    fenetre = [(a, b) for _, _, a, b in points[-FENETRE:]]
    etat, mediane, suspects = commun.etat(fenetre, phi, capteur=True)
    rs = (commun.ecart(phi, a, b) for _, _, a, b in points)
    derniere = pd.to_datetime(points[-1][0], utc=True, errors="coerce")
    age_s = None if maintenant is None or pd.isna(derniere) else (maintenant - derniere).total_seconds()
    return {"eolienne": eolienne, "etat": etat, "mediane": mediane, "suspects": suspects,
            "hors": sum(1 for _, b in fenetre if commun.tranche(b) not in phi), "fenetre": len(fenetre),
            "recus": recus, "derniere": points[-1][0], "age_s": age_s,
            "suspects_historique": sum(1 for r in rs if r is not None and r > commun.SEUIL_CAPTEUR)}


def serie_r(points, phi, minutes):
    """r de chaque mesure et médiane glissante sur 60 mesures (calcul identique à la version initiale)."""
    df = pd.DataFrame(points, columns=["horodatage", "seq", "pi1", "pi2"])
    df["t"] = pd.to_datetime(df["horodatage"], utc=True, errors="coerce")
    df["r"] = [commun.ecart(phi, a, b) for a, b in zip(df["pi1"], df["pi2"])]
    df = df.dropna(subset=["t", "r"]).sort_values("t")
    df["mediane_60"] = df["r"].rolling(FENETRE, min_periods=1).median()
    if not df.empty:
        df = df[df["t"] >= df["t"].max() - pd.Timedelta(minutes=minutes)]
    return df[["t", "r", "mediane_60"]]


def mesures_brutes(lignes, minutes):
    """Grandeurs physiques réelles (DataFrame indexé par l'heure capteur) sur la période affichée."""
    df = pd.DataFrame(lignes)
    if df.empty:
        return df
    df["t"] = pd.to_datetime(df["horodatage"], utc=True, errors="coerce")
    df = df.dropna(subset=["t"]).sort_values("t")
    return df[df["t"] >= df["t"].max() - pd.Timedelta(minutes=minutes)]


def cp_median(lignes_brutes):
    """Médiane de C_p = (8/π)·Π1 sur les 60 dernières mesures en production (rotor tournant)."""
    cps = []
    for m in lignes_brutes[-FENETRE:]:
        pi1, pi2 = commun.groupes_pi(m)
        if pi1 is not None and not commun.est_arret(pi1, pi2):
            cps.append(8 / 3.141592653589793 * pi1)
    return statistics.median(cps) if cps else None


def qualite(eolienne, points, maintenant):
    """Indicateurs de qualité de données (partie 18) : fraîcheur, complétude, champs absents, sauts de seq."""
    if not points:
        return {"eolienne": eolienne, "age_s": None, "n_5min": 0, "pi1_absent": None, "pi2_absent": None, "sauts": 0}
    derniere = pd.to_datetime(points[-1][0], utc=True, errors="coerce")
    # horodatages ISO de même format (…T10:54:00.000+00:00) : la comparaison de chaînes suit l'ordre du temps
    limite = (derniere - pd.Timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
    recents = []
    for p in reversed(points):
        if p[0] is None or p[0] <= limite:
            break
        recents.append(p)
    seqs = sorted(p[1] for p in recents if p[1] is not None)
    sauts = sum(b - a - 1 for a, b in zip(seqs, seqs[1:]) if b - a > 1)
    n = len(recents)
    return {"eolienne": eolienne, "age_s": (maintenant - derniere).total_seconds(), "n_5min": n,
            "pi1_absent": sum(1 for p in recents if commun.manquant(p[2])) / n,
            "pi2_absent": sum(1 for p in recents if commun.manquant(p[3])) / n, "sauts": sauts}


def maintenant_utc():
    return pd.Timestamp(datetime.now(timezone.utc))
