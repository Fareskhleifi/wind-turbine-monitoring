"""Composants HTML du tableau de bord (cartes, en-tête, tableaux, alertes, jauge, illustration d'éolienne).

Toute valeur issue de Kafka est échappée (html.escape) avant insertion.
"""
import html
import math
import re

import commun

from . import theme

BETZ = 16 / 27
PERIME_S = 60       # au-delà, la dernière mesure est signalée comme ancienne (cadence attendue : 1 mesure/s)


def _e(x):
    return html.escape(str(x))


_MATH = re.compile(r"\b(r|[α-ω])\b")


def lib(texte):
    """Libellé échappé dont les symboles r, ρ, ω… échappent aux majuscules CSS (sinon ρ s'affiche « P », ω « Ω »)."""
    return _MATH.sub(r'<span class="tdb-math">\1</span>', _e(texte))


def section(texte):
    return f'<div class="tdb-section">{lib(texte)}</div>'


def nombre(x, fmt="{:.3f}", vide="—"):
    return vide if x is None or (isinstance(x, float) and math.isnan(x)) else fmt.format(x).replace(".", ",")


def duree(secondes):
    if secondes is None:
        return "—"
    if secondes < 120:
        return f"{secondes:.0f} s"
    return f"{secondes / 60:.0f} min" if secondes < 7200 else f"{secondes / 3600:.1f} h".replace(".", ",")


def milliers(n):
    return f"{n:,}".replace(",", " ")


def heure(horodatage):
    """'2026-11-09T10:54:00.000+00:00' -> '09/11 10:54:00 UTC'."""
    if not horodatage:
        return "—"
    h = str(horodatage)
    return f"{h[8:10]}/{h[5:7]} {h[11:19]} UTC"


def badge(etat):
    return f'<span class="tdb-badge" style="background:{theme.COULEUR_ETAT[etat]}">{_e(theme.LIBELLE_ETAT[etat])}</span>'


def pastille(texte, couleur):
    return (f'<div class="tdb-pastille"><span class="tdb-point" style="background:{couleur};color:{couleur}"></span>'
            f'{_e(texte)}</div>')


def entete(connexion, flux, actualisation):
    """connexion / flux : (texte, couleur) calculés à partir de l'état réel des consommateurs Kafka."""
    icone = ('<svg width="46" height="46" viewBox="0 0 200 200"><circle cx="100" cy="100" r="96" fill="#062333" '
             'stroke="#21B6A8" stroke-width="6"/><g fill="#F4F7FA">'
             + "".join(f'<path transform="rotate({a} 100 92)" d="M100 92 C96 70 97 45 100 22 C103 45 104 70 100 92Z"/>'
                       for a in (0, 120, 240))
             + '</g><circle cx="100" cy="92" r="8" fill="#21B6A8"/><path d="M97 98 L103 98 L106 178 L94 178Z" fill="#A9BBC8"/></svg>')
    return (f'<div class="tdb-entete">{icone}<div><div class="tdb-titre">WIND TURBINE MONITORING</div>'
            f'<div class="tdb-soustitre">Real-time monitoring &amp; predictive maintenance — ferme « grand-pré », '
            f'15 éoliennes · TD3 jumeau numérique</div></div><div class="tdb-statuts">'
            f'{pastille(*connexion)}{pastille(*flux)}{pastille(actualisation, theme.TURQUOISE)}</div></div>')


def kpi(libelle, valeur, sous_titre, couleur):
    return (f'<div class="tdb-kpi" style="border-left-color:{couleur}"><div class="lib">{lib(libelle)}</div>'
            f'<div class="val">{_e(valeur)}</div><div class="sous">{_e(sous_titre)}</div></div>')


def illustration_eolienne(omega):
    """Éolienne vectorielle ; le rotor tourne si la vitesse de rotation mesurée ω est non nulle.

    La vitesse d'animation est indicative (période quantifiée pour rester lisible), la valeur réelle est affichée à côté.
    """
    if omega and omega > 0:
        periode = max(1, min(8, round(40 / omega)))
        rotor = f'<g class="rotor" style="animation-duration:{periode}s">'
    else:
        rotor = '<g class="rotor arret">'
    pales = "".join(f'<path transform="rotate({a} 100 92)" d="M100 92 C95 66 96 36 100 8 C104 36 105 66 100 92Z" '
                    f'fill="url(#pale)"/>' for a in (0, 120, 240))
    return ('<svg class="tdb-eol" viewBox="0 0 200 260" xmlns="http://www.w3.org/2000/svg"><defs>'
            '<linearGradient id="mat" x1="0" x2="1"><stop offset="0" stop-color="#8FA6B6"/><stop offset="1" stop-color="#E8EEF3"/></linearGradient>'
            '<linearGradient id="pale" x1="0" x2="1"><stop offset="0" stop-color="#CFDCE5"/><stop offset="1" stop-color="#FFFFFF"/></linearGradient>'
            '<radialGradient id="sol" cx=".5" cy=".5" r=".5"><stop offset="0" stop-color="#21B6A8" stop-opacity=".45"/>'
            '<stop offset="1" stop-color="#21B6A8" stop-opacity="0"/></radialGradient></defs>'
            '<ellipse cx="100" cy="250" rx="70" ry="8" fill="url(#sol)"/>'
            '<path d="M96 96 L104 96 L110 250 L90 250Z" fill="url(#mat)"/>'
            '<rect x="88" y="84" width="34" height="15" rx="6" fill="#DDE6EC"/>'
            f'{rotor}{pales}<circle cx="100" cy="92" r="7" fill="#21B6A8" stroke="#E8EEF3" stroke-width="2"/></g></svg>')


def carte_eolienne(resume, derniere_mesure, simulee):
    """Grande carte de l'éolienne choisie : état (parties 13/16), r, compteurs et grandeurs techniques mesurées."""
    m = derniere_mesure or {}
    omega = m.get("rotation_rads")
    infos = [("Zone", m.get("zone") or "—"),
             ("Diamètre du rotor", nombre(m.get("diametre_m"), "{:.0f} m")),
             ("Médiane de r (60)", nombre(resume["mediane"])),
             ("Mesures r > 2 (fenêtre)", str(resume["suspects"])),
             ("Mesures reçues", milliers(resume["recus"])),
             ("Hors abaque (fenêtre)", f'{resume["hors"]}/{resume["fenetre"]}'),
             ("Rotation ω", nombre(omega, "{:.2f} rad/s")),
             ("Dernière mesure", heure(resume["derniere"]))]
    grille = "".join(f'<div><div class="lib">{lib(a)}</div><div class="val">{_e(b)}</div></div>' for a, b in infos)
    mention = '<div class="tdb-mini" style="margin-top:6px">Données simulées (partie 18)</div>' if simulee else ""
    if perimee(resume):
        mention += (f'<div class="tdb-mini" style="margin-top:6px;color:{theme.ORANGE}">Aucune mesure depuis '
                    f'{duree(resume["age_s"])} : état figé à la dernière mesure reçue.</div>')
    return (f'<div class="tdb-carte"><div style="display:flex;justify-content:space-between;align-items:center">'
            f'<div class="tdb-nom">{_e(resume["eolienne"])}</div>{badge(resume["etat"])}</div>{mention}'
            f'{illustration_eolienne(omega)}<div class="tdb-mini" style="text-align:center">'
            f'{_e(theme.REGLE_ETAT[resume["etat"]])}</div><div class="tdb-grille">{grille}</div></div>')


def jauge_cp(cp):
    """Jauge circulaire du coefficient de puissance C_p = (8/π)·Π1, bornée par la limite physique de Betz (16/27)."""
    ratio = None if cp is None else cp / BETZ          # valeur réelle, non plafonnée
    frac = 0 if ratio is None else max(0.0, min(1.0, ratio))
    impossible = ratio is not None and ratio > 1        # au-delà de Betz : mesure physiquement impossible
    couleur = theme.ORANGE if impossible else theme.TURQUOISE
    angle = math.pi * (1 - frac)
    x, y = 100 + 80 * math.cos(angle), 100 - 80 * math.sin(angle)
    arc = (f'<path d="M20 100 A80 80 0 0 1 {x:.1f} {y:.1f}" stroke="{couleur}" stroke-width="14" '
           f'fill="none" stroke-linecap="round"/>' if frac > 0 else "")
    if cp is None:
        commentaire = "Aucune mesure en production dans les 60 dernières (rotor à l'arrêt)."
    elif impossible:
        commentaire = (f"<b style='color:{theme.ORANGE}'>{nombre(100 * ratio, '{:.0f} %')} de la limite de Betz : "
                       "physiquement impossible, signe de mesures fausses (voir l'état CAPTEUR).</b>")
    else:
        commentaire = f"{nombre(100 * ratio, '{:.0f} %')} de la limite de Betz."
    return (f'<div class="tdb-carte"><h4>Coefficient de puissance</h4><svg viewBox="0 0 200 135" width="100%">'
            f'<path d="M20 100 A80 80 0 0 1 180 100" stroke="#0E4560" stroke-width="14" fill="none" stroke-linecap="round"/>'
            f'{arc}<text x="100" y="86" text-anchor="middle" font-size="26" font-weight="800" fill="{theme.TEXTE}">'
            f'{nombre(cp, "{:.3f}")}</text>'
            f'<text x="20" y="120" text-anchor="middle" font-size="9" fill="{theme.TEXTE2}">0</text>'
            f'<text x="172" y="120" text-anchor="middle" font-size="9" fill="{theme.TEXTE2}">Betz 0,593</text>'
            f'<text x="100" y="133" text-anchor="middle" font-size="9" fill="{theme.TEXTE2}">C_p médian · 60 mesures en '
            f'production</text></svg><div class="tdb-mini">{commentaire} C_p = (8/π)·Π1. La puissance nominale '
            f'n\'est pas publiée par les capteurs : la jauge utilise la seule limite physique disponible.</div></div>')


def carte_mesure(libelle, valeur, unite, sous_titre=""):
    return (f'<div class="tdb-kpi" style="border-left-color:{theme.TURQUOISE}"><div class="lib">{lib(libelle)}</div>'
            f'<div class="val" style="font-size:1.55rem">{_e(valeur)} <span style="font-size:.9rem;color:{theme.TEXTE2}">'
            f'{_e(unite)}</span></div><div class="sous">{_e(sous_titre)}</div></div>')


def tableau_etats(resumes):
    """Tableau des éoliennes : état, médiane r, r > 2, mesures reçues, dernière mesure."""
    lignes = "".join(
        f'<tr><td style="border-left-color:{theme.COULEUR_ETAT[r["etat"]]}">{_e(r["eolienne"])}</td><td>{badge(r["etat"])}</td>'
        f'<td class="num">{nombre(r["mediane"])}</td><td class="num">{r["suspects"]}</td>'
        f'<td class="num">{milliers(r["recus"])}</td><td>{_e(heure(r["derniere"]))}{age(r)}</td></tr>'
        for r in resumes)
    return ('<div class="tdb-scroll"><table class="tdb-table"><thead><tr><th>Éolienne</th><th>État</th>'
            '<th style="text-align:right">Médiane <span class="tdb-math">r</span></th>'
            '<th style="text-align:right"><span class="tdb-math">r</span> &gt; 2</th><th style="text-align:right">Mesures reçues</th>'
            f'<th>Dernière mesure</th></tr></thead><tbody>{lignes}</tbody></table></div>')


def perimee(resume):
    """Vrai si la dernière mesure date de plus de PERIME_S : l'état affiché n'est plus d'actualité."""
    return resume.get("age_s") is not None and resume["age_s"] > PERIME_S


def age(resume):
    return (f'<br><span class="tdb-mini" style="color:{theme.ORANGE}">il y a {duree(resume["age_s"])}</span>'
            if perimee(resume) else "")


def tableau_qualite(qualites):
    """Indicateurs de qualité des données (partie 18) ; n'influencent pas l'état calculé."""
    def couleur(q):
        if q["age_s"] is None or q["age_s"] > 30:
            return theme.ROUGE
        if q["n_5min"] < 270 or (q["pi2_absent"] or 0) > 0.1 or (q["pi1_absent"] or 0) > 0.1:
            return theme.ORANGE
        return theme.VERT
    lignes = "".join(
        f'<tr><td style="border-left-color:{couleur(q)}">{_e(q["eolienne"])}</td>'
        f'<td class="num">{duree(q["age_s"])}</td><td class="num">{q["n_5min"]} / 300</td>'
        f'<td class="num">{nombre(None if q["pi1_absent"] is None else 100 * q["pi1_absent"], "{:.0f} %")}</td>'
        f'<td class="num">{nombre(None if q["pi2_absent"] is None else 100 * q["pi2_absent"], "{:.0f} %")}</td>'
        f'<td class="num">{q["sauts"]}</td></tr>' for q in qualites)
    return ('<div class="tdb-scroll"><table class="tdb-table"><thead><tr><th>Éolienne</th>'
            '<th style="text-align:right">Âge dernière mesure</th>'
            '<th style="text-align:right">Mesures (5 dernières min)</th><th style="text-align:right">Π1 absent</th>'
            '<th style="text-align:right">Π2 absent</th><th style="text-align:right">Mesures perdues (sauts de seq)</th>'
            f'</tr></thead><tbody>{lignes}</tbody></table></div>')


def carte_anomalie(r):
    return (f'<div class="tdb-alerte" style="border-left-color:{theme.COULEUR_ETAT[r["etat"]]}">'
            f'<div style="display:flex;justify-content:space-between"><span class="t">{_e(r["eolienne"])}</span>'
            f'{badge(r["etat"])}</div><div class="d">Médiane r = {nombre(r["mediane"])} · r &gt; 2 : {r["suspects"]} · '
            f'hors abaque : {r["hors"]}/{r["fenetre"]}</div><div class="d">Règle : {_e(theme.REGLE_ETAT[r["etat"]])}</div>'
            f'<div class="d">Dernière mesure : {_e(heure(r["derniere"]))}{age(r)}</div></div>')


def colonne_etat(etat, resumes):
    noms = [r["eolienne"] for r in resumes if r["etat"] == etat]
    puces = " ".join(f'<span class="tdb-badge" style="background:{theme.COULEUR_ETAT[etat]};margin:2px">{_e(n)}</span>'
                     for n in noms) or '<span class="tdb-mini">aucune</span>'
    return (f'<div class="tdb-kpi" style="border-left-color:{theme.COULEUR_ETAT[etat]}"><div class="lib">{_e(theme.LIBELLE_ETAT[etat])}</div>'
            f'<div class="val">{len(noms)}</div><div style="margin-top:6px">{puces}</div></div>')


def flux_alertes(alertes, limite=25):
    """Dernières alertes réellement publiées dans ferme.alertes par les parties 13 et 16."""
    if not alertes:
        return '<div class="tdb-mini">Aucune alerte reçue dans ferme.alertes.</div>'
    lignes = []
    for a in sorted(alertes, key=lambda a: a.get("emise_a") or "", reverse=True)[:limite]:
        nouvel = a.get("nouvel_etat") if a.get("nouvel_etat") in theme.COULEUR_ETAT else "AUCUNE_DONNEE"
        ancien = a.get("ancien_etat")
        lignes.append(
            f'<tr><td style="border-left-color:{theme.COULEUR_ETAT[nouvel]}">{_e(a.get("eolienne", "?"))}</td>'
            f'<td>{_e(theme.LIBELLE_ETAT.get(ancien, "—") if ancien else "—")} → {badge(nouvel)}</td>'
            f'<td class="num">{nombre(a.get("mediane_r"))}</td><td class="num">{_e(a.get("r_sup_2", "—"))}</td>'
            f'<td>{_e(heure(a.get("emise_a")))}</td><td>{_e(a.get("source", ""))}</td></tr>')
    return ('<div class="tdb-scroll"><table class="tdb-table"><thead><tr><th>Éolienne</th><th>Transition</th>'
            '<th style="text-align:right">Médiane <span class="tdb-math">r</span></th>'
            '<th style="text-align:right"><span class="tdb-math">r</span> &gt; 2</th><th>Émise à</th><th>Source</th></tr></thead>'
            f'<tbody>{"".join(lignes)}</tbody></table></div>')


def legende_r(ymax):
    return ('<div class="tdb-legende">'
            f'<span><i style="background:{theme.POINTS}"></i>r de chaque mesure</span>'
            f'<span><i style="background:{theme.TURQUOISE}"></i>médiane glissante (60 mesures)</span>'
            f'<span><i style="background:{theme.VERT};opacity:.45"></i>marge ±10 % (0,9 – 1,1)</span>'
            f'<span><i style="background:{theme.ORANGE}"></i>▲ r &gt; {nombre(ymax, "{:g}")} (écrêté)</span></div>')


SEUILS_TEXTE = (f"Seuils inchangés (parties 13 et 16) : marge ±{commun.MARGE:.0%}, CAPTEUR si r > {commun.SEUIL_CAPTEUR:g}, "
                f"fenêtre de 60 mesures, HORS_ABAQUE si plus de la moitié des Π2 sont hors de Φ.")
