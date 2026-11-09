"""Partie 02 — Recopier les mesures dans Kafka.

Pont continu MQTT -> topic Kafka ferme.eoliennes : clé = nom de l'éolienne (E1, E2…),
valeur = message JSON intact (octets reçus, non modifiés).
Usage (dans le conteneur app) : python scripts/02_pont_kafka.py   — arrêt par Ctrl+C sans perte.
"""
import commun

if __name__ == "__main__":
    commun.pont(transformer=None, suffixe="p02")
