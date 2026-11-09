"""Partie 15 — Suivre r dans le temps : tableau de bord « Wind Turbine Monitoring » (Streamlit).

Affiche en direct, pour les éoliennes choisies, r = Π1/Φ(Π2) de chaque mesure en fonction de l'heure capteur,
sa médiane glissante sur 60 mesures et la marge ±10 % (mise à jour toutes les 5 s), ainsi que l'état de toute la
ferme (règles des parties 13 et 16), les alertes de ferme.alertes, les grandeurs physiques mesurées et la qualité
des données (éoliennes simulées EX1-EX5 de la partie 18, séparées des éoliennes réelles).

Lancement (conteneur app, depuis /td pour que .streamlit/config.toml soit pris en compte) :
  streamlit run scripts/15_tableau_r.py --server.port 8501 --server.address 0.0.0.0
puis http://localhost:8501   (paramètres facultatifs : ?vue=Overview&eolienne=E6&eoliennes=E6,E7&minutes=60)

Présentation : paquet tdb/ (donnees, theme, graphiques, composants) ; calculs : commun.py (inchangé).
"""
import time

import streamlit as st

import commun
from tdb import composants as C
from tdb import donnees as D
from tdb import graphiques as G
from tdb import theme as T

st.set_page_config(page_title="Wind Turbine Monitoring — TD3", layout="wide", initial_sidebar_state="expanded")
st.markdown(T.CSS, unsafe_allow_html=True)

VUES = ["Overview", "Wind Farm", "Real-time Monitoring", "Alerts & Anomalies", "Data Quality"]
qp = st.query_params


@st.cache_resource
def flux():
    return D.Flux()


def html(contenu):
    st.markdown(contenu, unsafe_allow_html=True)


def afficher(graphique, message="Pas encore de mesure exploitable pour ce graphique."):
    """Affiche un graphique Altair. La clé change à chaque rafraîchissement : le graphique est recréé au lieu d'être
    mis à jour en place, ce qui évite l'erreur Vega « Unrecognized data set » des graphiques à plusieurs couches."""
    if graphique is None:
        st.info(message)
    else:
        st.altair_chart(T.configurer(graphique), use_container_width=True, theme=None, key=f"g{time.time_ns()}")


def liste_param(nom, defaut, valides):
    v = [e for e in qp.get(nom, defaut).split(",") if e in valides]
    return v or [e for e in defaut.split(",") if e in valides]


def entier_param(nom, defaut, mini, maxi):
    """Paramètre d'URL entier borné : ?minutes=abc ou ?minutes=999 faisaient planter la page."""
    try:
        return min(maxi, max(mini, int(qp.get(nom, defaut))))
    except ValueError:
        return defaut


# --------------------------------------------------------------------------- contrôles (barre latérale)
with st.sidebar:
    st.markdown("### Contrôles")
    defaut = qp.get("eolienne", "E6")
    principale = st.selectbox("Éolienne sélectionnée", D.TOUTES,
                              index=D.TOUTES.index(defaut) if defaut in D.TOUTES else D.TOUTES.index("E6"))
    choix = st.multiselect("Éoliennes affichées (temps réel)", D.TOUTES,
                           default=liste_param("eoliennes", principale, D.TOUTES))
    comparees = st.multiselect("Comparaison", D.REELLES, default=liste_param("comparer", "E1,E2,E3", D.REELLES))
    minutes = st.slider("Période affichée (minutes)", 5, 90, entier_param("minutes", 30, 5, 90), 5)
    ymax = st.slider("Échelle verticale max de r", 1.5, 10.0, 2.5, 0.5)
    st.caption(C.SEUILS_TEXTE)
    st.caption("Couleurs : vert = OK · rouge = sous-production · orange = capteur / sur-production · "
               "bleu = hors abaque · gris = aucune donnée.")


# --------------------------------------------------------------------------- en-tête (statut réel de Kafka)
def zone_entete():
    s = flux().statut()
    derniere = s["reception"].get(commun.TOPIC_PI)
    age = None if derniere is None else time.time() - derniere
    if s["erreur"] and (age is None or age > 30):
        connexion = ("Kafka : erreur de connexion", T.ROUGE)
    elif derniere is not None:
        connexion = ("Kafka connecté", T.VERT)
    else:
        connexion = ("Kafka : connexion en cours", T.ORANGE)
    if age is None:
        flux_txt = ("Flux de données : en attente", T.GRIS)
    elif age < 10:
        flux_txt = (f"Flux de données actif · {age:.0f} s", T.VERT)
    elif age < 60:
        flux_txt = (f"Flux ralenti · dernière réception il y a {age:.0f} s", T.ORANGE)
    else:
        flux_txt = (f"Flux interrompu depuis {age / 60:.0f} min", T.ROUGE)
    html(C.entete(connexion, flux_txt, f"Actualisé {time.strftime('%H:%M:%S', time.gmtime())} UTC"))


VUE_DEFAUT = qp.get("vue", "Overview") if qp.get("vue", "Overview") in VUES else "Overview"

# --------------------------------------------------------------------------- vues
def vue_overview(s, resumes, phi):
    reels = [resumes[e] for e in D.REELLES]
    n = {etat: sum(1 for r in reels if r["etat"] == etat) for etat in T.COULEUR_ETAT}
    en_service = sum(1 for r in reels if r["etat"] != "AUCUNE_DONNEE" and not C.perimee(r))
    cartes = [("Éoliennes actives", f"{en_service} / {len(reels)}", f"mesure reçue il y a moins de {C.PERIME_S} s",
               T.TURQUOISE),
              ("État OK", n["OK"], "règles des parties 13 et 16", T.VERT),
              ("Sous-production", n["SOUS_PRODUCTION"], "médiane de r < 0,9", T.ROUGE),
              ("Défaut capteur", n["CAPTEUR"], "au moins un r > 2 dans la fenêtre", T.ORANGE),
              ("Mesures traitées", C.milliers(s["messages"].get(commun.TOPIC_PI, 0)), "messages Π lus dans Kafka", T.BLEU),
              ("Mesures suspectes", C.milliers(sum(r["suspects_historique"] for r in reels)),
               f"r > 2 dans l'historique en mémoire · {sum(r['suspects'] for r in reels)} dans les fenêtres", T.ORANGE)]
    for col, (lib, val, sous, coul) in zip(st.columns(6), cartes):
        with col:
            html(C.kpi(lib, val, sous, coul))
    st.caption(f"Sur-production : {n['SUR_PRODUCTION']} · Hors abaque : {n['HORS_ABAQUE']} · "
               f"Aucune donnée : {n['AUCUNE_DONNEE']} (éoliennes réelles E1 à E15 ; EX1 à EX5 dans « Data Quality »)")
    gauche, droite = st.columns([1, 2.3], gap="medium")
    brut = s["brut"].get(principale, [])
    with gauche:
        html(C.carte_eolienne(resumes[principale], brut[-1] if brut else None, principale in D.SIMULEES))
    with droite:
        html(C.section(f"Performance en temps réel — {principale} · écart à l'abaque Φ") + C.legende_r(ymax))
        afficher(G.graphique_r(D.serie_r(s["pi"].get(principale, []), phi, minutes), ymax, hauteur=300))
        panneau_mesures(brut, compact=True)
    a, b = st.columns([1, 1.6], gap="medium")
    with a:
        html(C.section("Anomalies actives"))
        actives = [r for r in reels if r["etat"] not in ("OK", "AUCUNE_DONNEE")]
        html("".join(C.carte_anomalie(r) for r in actives[:6]) or '<div class="tdb-mini">Aucune anomalie active.</div>')
        if len(actives) > 6:
            st.caption(f"+ {len(actives) - 6} autres : voir « Alerts & Anomalies ».")
    with b:
        html(C.section("Médiane actuelle de r par éolienne"))
        afficher(G.graphique_medianes(reels))


def panneau_mesures(brut, compact=False):
    """Grandeurs réellement publiées par les capteurs (aucune température de composant n'est disponible)."""
    if not brut:
        st.info("Aucune mesure brute reçue pour cette éolienne.")
        return
    m = brut[-1]
    kw = None if m.get("puissance_w") is None else m["puissance_w"] / 1000
    pmax = max((x["puissance_w"] for x in brut if x.get("puissance_w") is not None), default=None)
    cartes = [("Puissance électrique", C.nombre(kw, "{:.2f}"), "kW",
               f"max sur 15 min : {C.nombre(None if pmax is None else pmax / 1000, '{:.2f}')} kW"),
              ("Vitesse du vent", C.nombre(m.get("vent_ms"), "{:.1f}"), "m/s", "anémomètre"),
              ("Température de l'air", C.nombre(m.get("temp_c"), "{:.1f}"), "°C", "air ambiant"),
              ("Pression", C.nombre(m.get("pression_hpa"), "{:.1f}"), "hPa", "atmosphérique"),
              ("Masse volumique ρ", C.nombre(m.get("rho_kgm3"), "{:.3f}"), "kg/m³", "ρ = P / (R_air·T), partie 04")]
    for col, (lib, val, unite, sous) in zip(st.columns(5), cartes):
        with col:
            html(C.carte_mesure(lib, val, unite, sous))
    if compact:
        return
    df = D.mesures_brutes(brut, minutes)
    if not df.empty and "puissance_w" in df:
        df = df.assign(puissance_kw=df["puissance_w"] / 1000)
    g1, g2 = st.columns([1, 2.2], gap="medium")
    with g1:
        html(C.jauge_cp(D.cp_median(brut)))
    with g2:
        x, y = st.columns(2)
        with x:
            afficher(G.graphique_mesure(df, "puissance_kw", "puissance (kW)", T.TURQUOISE, format_axe="~f"))
            afficher(G.graphique_mesure(df, "temp_c", "température air (°C)", T.ORANGE))
        with y:
            afficher(G.graphique_mesure(df, "vent_ms", "vent (m/s)", T.BLEU))
            afficher(G.graphique_mesure(df, "pression_hpa", "pression (hPa)", "#B39DFB", format_axe="d"))
    st.caption("Grandeurs physiques sur les 15 dernières minutes au plus (900 mesures brutes gardées en mémoire).")


def vue_parc(s, resumes, phi):
    reels = [resumes[e] for e in D.REELLES]
    # graphique et écarts en haut, tableau pleine largeur dessous : côte à côte, le tableau débordait sur le graphique
    gauche, droite = st.columns([1.8, 1], gap="medium")
    with gauche:
        html(C.section("Médiane actuelle de r"))
        afficher(G.graphique_medianes(reels, hauteur=280))
    with droite:
        html(C.section("Écarts les plus importants à l'abaque"))
        ecarts = sorted((r for r in reels if r["mediane"] is not None), key=lambda r: abs(r["mediane"] - 1), reverse=True)
        html('<div class="tdb-carte">'
             + "".join(f'<div style="margin:8px 0">{C.badge(r["etat"])} <b>{r["eolienne"]}</b> · médiane r = '
                       f'{C.nombre(r["mediane"])} ({C.nombre(100 * (r["mediane"] - 1), "{:+.1f}")} %)</div>'
                       for r in ecarts[:5]) + "</div>")
    html(C.section(f"État des {len(reels)} éoliennes"))
    html(C.tableau_etats(reels))
    html(C.section(f'Comparaison — médiane glissante de r : {", ".join(comparees) or "aucune"}'))
    afficher(G.graphique_comparaison({e: D.serie_r(s["pi"].get(e, []), phi, minutes) for e in comparees}),
             "Pas encore de mesure exploitable pour les éoliennes choisies." if comparees
             else "Choisissez des éoliennes à comparer dans la barre latérale.")


def vue_temps_reel(s, resumes, phi):
    if not choix:
        st.info("Choisissez au moins une éolienne dans la barre latérale.")
    for e in choix:
        r = resumes[e]
        html(f'<div class="tdb-carte"><div style="display:flex;justify-content:space-between;align-items:center">'
             f'<h4 style="margin:0">{e} — écart à l\'abaque Φ : <span class="tdb-math">r</span> = Π1 / Φ(Π2)'
             f'{" · données simulées" if e in D.SIMULEES else ""}</h4>{C.badge(r["etat"])}</div>'
             f'<div class="tdb-mini">médiane (60) = {C.nombre(r["mediane"])} · r &gt; 2 dans la fenêtre : {r["suspects"]} · '
             f'mesures reçues : {C.milliers(r["recus"])} · dernière mesure : {C.heure(r["derniere"])}{C.age(r)}</div>'
             f'{C.legende_r(ymax)}</div>')
        afficher(G.graphique_r(D.serie_r(s["pi"].get(e, []), phi, minutes), ymax),
                 f"{e} : aucune mesure exploitable (r non calculable).")
    html(C.section(f"Indicateurs physiques — {principale}"))
    panneau_mesures(s["brut"].get(principale, []))


def vue_alertes(s, resumes, phi):
    reels = [resumes[e] for e in D.REELLES]
    for col, etat in zip(st.columns(5), ["OK", "SOUS_PRODUCTION", "SUR_PRODUCTION", "CAPTEUR", "HORS_ABAQUE"]):
        with col:
            html(C.colonne_etat(etat, reels))
    html(C.section("Anomalies actives (60 dernières mesures)"))
    actives = [r for r in reels if r["etat"] not in ("OK", "AUCUNE_DONNEE")]
    if not actives:
        st.success("Aucune anomalie active sur les éoliennes réelles.")
    for i in range(0, len(actives), 3):
        for col, r in zip(st.columns(3), actives[i:i + 3]):
            with col:
                html(C.carte_anomalie(r))
    reelles = [a for a in s["alertes"] if a.get("eolienne") in D.REELLES]
    html(f'<div class="tdb-carte"><h4>Historique des alertes publiées dans ferme.alertes '
         f'({len(reelles)} reçues, 25 dernières)</h4>{C.flux_alertes(reelles)}</div>')


def vue_qualite(s, resumes, phi):
    maintenant = D.maintenant_utc()
    html('<div class="tdb-section">Éoliennes réelles — fraîcheur et complétude des données</div>'
         '<div class="tdb-mini">Cadence attendue : 1 mesure/s (cadence observée du broker), soit 300 mesures en 5 min. '
         'Ces indicateurs sont informatifs et ne modifient pas les états des parties 13 et 16.</div>')
    html(C.tableau_qualite([D.qualite(e, s["pi"].get(e, []), maintenant) for e in D.REELLES]))
    html('<div class="tdb-bandeau"><b>Données simulées.</b> EX1 à EX5 sont produites par 18_simuler_manques.py à partir '
         'des mesures réelles de E1 à E5, volontairement abîmées : EX1 mesures perdues, EX2 puissance absente, '
         'EX3 vent absent, EX4 ρ absent, EX5 silences. Elles ne font pas partie de la surveillance réelle.</div>')
    simulees = [resumes[e] for e in D.SIMULEES]
    html('<div class="tdb-section">EX1 à EX5 — états (mêmes règles que les parties 13 et 16)</div>')
    html(C.tableau_etats(simulees))
    html('<div class="tdb-section">EX1 à EX5 — qualité des données</div>')
    html(C.tableau_qualite([D.qualite(e, s["pi"].get(e, []), maintenant) for e in D.SIMULEES]))
    ex = st.selectbox("Éolienne simulée à tracer", D.SIMULEES, key="ex_trace")
    html(C.section(f"{ex} — r = Π1 / Φ(Π2) (données simulées)") + C.legende_r(ymax))
    afficher(G.graphique_r(D.serie_r(s["pi"].get(ex, []), phi, minutes), ymax),
             f"{ex} : aucune mesure exploitable (simulateur arrêté ou champs absents).")


def garder_vue():
    """Recliquer l'onglet actif le désélectionnait : la page basculait sur Overview sans onglet surligné.
    On garde la vue précédente, et on la recopie dans l'URL (un rechargement reste sur la même vue)."""
    if st.session_state.vue is None:
        st.session_state.vue = st.session_state.get("vue_prec", VUE_DEFAUT)
    st.session_state.vue_prec = st.session_state.vue
    st.query_params["vue"] = st.session_state.vue


@st.fragment(run_every=5)
def page():
    """Un seul fragment (en-tête, navigation, vue) rafraîchi toutes les 5 s : deux fragments périodiques qui se
    chevauchent pendant le rattrapage de l'historique Kafka faisaient disparaître la page."""
    zone_entete()
    vue = st.segmented_control("Navigation", VUES, default=VUE_DEFAUT, label_visibility="collapsed",
                               key="vue", on_change=garder_vue) or VUE_DEFAUT
    s = flux().instantane()
    phi = commun.charger_phi()
    maintenant = D.maintenant_utc()
    resumes = {e: D.resume(e, s["pi"].get(e, []), s["recus"].get(e, 0), phi, maintenant) for e in D.TOUTES}
    {"Overview": vue_overview, "Wind Farm": vue_parc, "Real-time Monitoring": vue_temps_reel,
     "Alerts & Anomalies": vue_alertes, "Data Quality": vue_qualite}[vue](s, resumes, phi)


page()
