"""Graphiques Altair du tableau de bord (rendus côté navigateur, axes en heure capteur UTC)."""
import altair as alt
import pandas as pd

import commun

from . import theme

TITRE_R = "r = Π1 / Φ(Π2)"
ECART_MAX = pd.Timedelta(seconds=120)   # au-delà, la courbe est interrompue (trou réel dans les données)


def _x(d):
    """Axe du temps capteur en UTC ; secondes affichées seulement si la période montrée dure moins de 10 minutes."""
    court = d.empty or d["t"].max() - d["t"].min() < pd.Timedelta(minutes=10)
    return alt.X("t:T", title="Heure capteur (UTC)", scale=alt.Scale(type="utc"),
                 axis=alt.Axis(format="%H:%M:%S" if court else "%H:%M", labelAngle=0, tickCount=8))


def _segments(df, par=None):
    """Numéro de segment : une nouvelle portion de courbe commence après un trou de plus de 2 minutes."""
    trou = df["t"].diff() > ECART_MAX
    return (trou.cumsum().astype(str) if par is None else df[par] + "-" + trou.cumsum().astype(str))


def _bande(echelle, titre=TITRE_R):
    marge = pd.DataFrame({"y": [1 - commun.MARGE], "y2": [1 + commun.MARGE], "u": [1.0]})
    bande = alt.Chart(marge).mark_rect(color=theme.VERT, opacity=0.15).encode(
        y=alt.Y("y:Q", scale=echelle, title=titre), y2="y2:Q")
    unite = alt.Chart(marge).mark_rule(color=theme.VERT, strokeDash=[5, 4], opacity=0.8).encode(
        y=alt.Y("u:Q", scale=echelle, title=titre))
    return bande + unite


def graphique_r(df, ymax, hauteur=330):
    """Partie 15 : r de chaque mesure, médiane glissante sur 60 mesures, marge ±10 %, valeurs > ymax écrêtées.
    None si aucune mesure exploitable (sinon Vega dessine un cadre vide : « Infinite extent for field t »)."""
    if df.empty:
        return None
    d = df.copy()
    d["r_aff"] = d["r"].clip(upper=ymax)
    d["heure"] = d["t"].dt.strftime("%H:%M:%S")
    d["segment"] = _segments(d)
    echelle = alt.Scale(domain=[0, ymax], clamp=True)
    y = lambda champ: alt.Y(f"{champ}:Q", scale=echelle, title=TITRE_R)
    infos = [alt.Tooltip("heure:N", title="Heure UTC"), alt.Tooltip("r:Q", title="r", format=".3f"),
             alt.Tooltip("mediane_60:Q", title="Médiane 60", format=".3f")]
    zoom = alt.selection_interval(bind="scales", encodings=["x"])
    points = alt.Chart(d).mark_circle(size=13, opacity=0.5, color=theme.POINTS).encode(
        x=_x(d), y=y("r_aff"), tooltip=infos).add_params(zoom)
    mediane = alt.Chart(d).mark_line(color=theme.TURQUOISE, strokeWidth=2.6).encode(
        x=_x(d), y=y("mediane_60"), detail="segment:N", tooltip=infos)
    couches = [_bande(echelle), points, mediane]
    if (d["r"] > ymax).any():   # couche ajoutée seulement si elle a des points (sinon étendue vide côté Vega)
        couches.insert(2, alt.Chart(d[d["r"] > ymax]).mark_point(shape="triangle-up", filled=True, size=55,
                                                                 color=theme.ORANGE).encode(x=_x(d), y=y("r_aff"), tooltip=infos))
    return alt.layer(*couches).properties(height=hauteur)


def graphique_comparaison(series, hauteur=300):
    """Médiane glissante (60 mesures) de r pour plusieurs éoliennes, sur le même axe temporel."""
    morceaux = [df.assign(eolienne=e) for e, df in series.items() if not df.empty]
    if not morceaux:
        return None
    d = pd.concat(morceaux, ignore_index=True)
    d["heure"] = d["t"].dt.strftime("%H:%M:%S")
    d = d.sort_values(["eolienne", "t"])
    d["trace"] = pd.concat([_segments(g, "eolienne") for _, g in d.groupby("eolienne", sort=False)])
    haut = max(1.3, float(d["mediane_60"].max()) * 1.05)
    echelle = alt.Scale(domain=[0, haut], clamp=True)
    noms = list(series)
    titre = "médiane glissante de r (60 mesures)"
    lignes = alt.Chart(d).mark_line(strokeWidth=2.2).encode(
        x=_x(d), y=alt.Y("mediane_60:Q", scale=echelle, title=titre),
        color=alt.Color("eolienne:N", title="Éolienne", sort=noms,
                        scale=alt.Scale(domain=noms, range=theme.SERIES[:len(noms)] or theme.SERIES)),
        detail="trace:N",
        tooltip=[alt.Tooltip("eolienne:N", title="Éolienne"), alt.Tooltip("heure:N", title="Heure UTC"),
                 alt.Tooltip("mediane_60:Q", title="Médiane 60", format=".3f")])
    return alt.layer(_bande(echelle, titre), lignes).properties(height=hauteur)


def graphique_medianes(resumes, hauteur=260):
    """Médiane actuelle de r (60 dernières mesures) par éolienne, colorée selon l'état."""
    d = pd.DataFrame([{"eolienne": r["eolienne"], "mediane": r["mediane"], "etat": r["etat"],
                       "libelle": theme.LIBELLE_ETAT[r["etat"]]} for r in resumes if r["mediane"] is not None])
    if d.empty:
        return None
    haut = max(1.3, float(d["mediane"].max()) * 1.08)
    echelle = alt.Scale(domain=[0, haut])
    etats = list(theme.COULEUR_ETAT)
    ordre = [r["eolienne"] for r in resumes]
    # largeur de barre relative à la colonne (une largeur fixe débordait quand la colonne est étroite)
    barres = alt.Chart(d).mark_bar(cornerRadiusTopLeft=5, cornerRadiusTopRight=5).encode(
        x=alt.X("eolienne:N", sort=ordre, title=None, axis=alt.Axis(labelAngle=0),
                scale=alt.Scale(paddingInner=0.25)),
        y=alt.Y("mediane:Q", scale=echelle, title="médiane de r (60 dernières mesures)"),
        color=alt.Color("libelle:N", title="État", scale=alt.Scale(domain=[theme.LIBELLE_ETAT[e] for e in etats],
                        range=[theme.COULEUR_ETAT[e] for e in etats]),
                        legend=alt.Legend(columns=3, labelFontSize=10, symbolSize=60)),
        tooltip=[alt.Tooltip("eolienne:N", title="Éolienne"), alt.Tooltip("libelle:N", title="État"),
                 alt.Tooltip("mediane:Q", title="Médiane r", format=".3f")])
    texte = barres.mark_text(dy=-8, color=theme.TEXTE, fontSize=10).encode(text=alt.Text("mediane:Q", format=".2f"))
    marge = pd.DataFrame({"y": [1 - commun.MARGE], "y2": [1 + commun.MARGE], "u": [1.0]})
    bande = alt.Chart(marge).mark_rect(color=theme.VERT, opacity=0.13).encode(y=alt.Y("y:Q", scale=echelle), y2="y2:Q")
    unite = alt.Chart(marge).mark_rule(color=theme.VERT, strokeDash=[5, 4]).encode(y=alt.Y("u:Q", scale=echelle))
    return alt.layer(bande, unite, barres, texte).properties(height=hauteur)


def graphique_mesure(df, champ, titre, couleur, hauteur=150, format_axe=None):
    """Évolution d'une grandeur physique réellement mesurée (vent, puissance, température, pression, ρ)."""
    d = df[["t", champ]].dropna()
    if d.empty:
        return None
    d = d.assign(heure=d["t"].dt.strftime("%H:%M:%S"), segment=_segments(d))
    return alt.Chart(d).mark_line(color=couleur, strokeWidth=1.8).encode(
        x=_x(d), y=alt.Y(f"{champ}:Q", title=titre, scale=alt.Scale(zero=False),
                        axis=alt.Axis(format=format_axe) if format_axe else alt.Axis()), detail="segment:N",
        tooltip=[alt.Tooltip("heure:N", title="Heure UTC"), alt.Tooltip(f"{champ}:Q", title=titre, format=".2f")]
    ).properties(height=hauteur)
