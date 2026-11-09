"""Partie 16 — Détecter les défauts ponctuels.

Surveillance de la partie 13 avec un état supplémentaire CAPTEUR, prioritaire sur la sous- et la
sur-production : une éolienne y passe dès qu'au moins une mesure de sa fenêtre de 60 a un r > 2
(règle implémentée dans commun.etat, paramètre capteur=True).

Usage : python scripts/16_surveiller_capteur.py [--eoliennes E6,...,E15] [--duree 0]
"""
import importlib

surveillance = importlib.import_module("13_surveiller")

if __name__ == "__main__":
    surveillance.surveiller(capteur=True, nom="16")
