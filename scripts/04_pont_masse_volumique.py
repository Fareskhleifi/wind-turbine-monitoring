"""Partie 04 — Enrichir les mesures.

Même pont que la partie 02, mais chaque mesure reçoit avant l'envoi dans Kafka le champ
rho_kgm3 = P / (R_air · T), avec R_air = 287,05 J/(kg·K), P = pression_hpa·100 (Pa), T = temp_c + 273,15 (K).
Une mesure sans pression ou sans température est transmise sans rho_kgm3 (on ne perd pas la mesure).
"""
import json

import commun


def ajouter_rho(payload):
    mesure = commun.decoder(payload)
    if mesure is None:
        return None                      # message non JSON : ignoré (compté comme invalide)
    try:
        mesure["rho_kgm3"] = round(commun.rho(float(mesure["pression_hpa"]), float(mesure["temp_c"])), 6)
    except (KeyError, TypeError, ValueError):
        pass
    return json.dumps(mesure, ensure_ascii=False).encode()


if __name__ == "__main__":
    commun.pont(transformer=ajouter_rho, suffixe="p04")
