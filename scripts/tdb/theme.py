"""Identité visuelle du tableau de bord : palettes nuit / jour, couleurs des états, CSS, thème des graphiques Altair.

Le HTML n'utilise que des variables CSS (var(--texte)…) : changer de mode ne change que le bloc :root.
"""

TURQUOISE = "#21B6A8"
VERT = "#42B883"
ORANGE = "#F07845"
ROUGE = "#EF5350"
BLEU = "#4FA3E0"
GRIS = "#7F95A6"

# Couleurs dépendant du mode, utilisables dans le HTML (pas dans les graphiques, rendus sur canvas)
TEXTE = "var(--texte)"
TEXTE2 = "var(--texte2)"
POINTS = "var(--points)"

MODES = {"nuit": "Nuit", "jour": "Jour"}
PALETTES = {
    "nuit": {"fond": "#020D19", "fond2": "#062333", "carte": "#073044", "bord": "#0E4560", "texte": "#F4F7FA",
             "texte2": "#A9BBC8", "points": "#7FC8F8", "halo": "#08304a", "pastille": "rgba(2,13,25,.55)",
             "ombre": "0 1px 2px rgba(0,0,0,.25)", "grille": "rgba(169,187,200,0.13)", "axe": "rgba(169,187,200,0.35)"},
    "jour": {"fond": "#EEF3F7", "fond2": "#FFFFFF", "carte": "#FFFFFF", "bord": "#D6E0E9", "texte": "#0E2233",
             "texte2": "#5A6C7D", "points": "#3D86C6", "halo": "#D5E9F5", "pastille": "rgba(255,255,255,.85)",
             "ombre": "0 1px 3px rgba(14,34,51,.08), 0 1px 2px rgba(14,34,51,.04)",
             "grille": "rgba(14,34,51,0.08)", "axe": "rgba(14,34,51,0.28)"},
}

# Code couleur des états (règles des parties 13 et 16, inchangées) :
# vert = normal, rouge = anomalie critique de production, orange = à vérifier, bleu/gris = information
COULEUR_ETAT = {"OK": VERT, "SOUS_PRODUCTION": ROUGE, "SUR_PRODUCTION": ORANGE, "CAPTEUR": ORANGE,
                "HORS_ABAQUE": BLEU, "AUCUNE_DONNEE": GRIS}
LIBELLE_ETAT = {"OK": "OK", "SOUS_PRODUCTION": "Sous-production", "SUR_PRODUCTION": "Sur-production",
                "CAPTEUR": "Capteur", "HORS_ABAQUE": "Hors abaque", "AUCUNE_DONNEE": "Aucune donnée"}
REGLE_ETAT = {"OK": "médiane de r entre 0,9 et 1,1, aucune mesure r > 2",
              "SOUS_PRODUCTION": "médiane de r < 0,9 sur les 60 dernières mesures",
              "SUR_PRODUCTION": "médiane de r > 1,1 sur les 60 dernières mesures",
              "CAPTEUR": "au moins une mesure r > 2 dans la fenêtre (prioritaire sur sous/sur-production)",
              "HORS_ABAQUE": "plus de 30 des 60 mesures ont un Π2 absent de l'abaque Φ",
              "AUCUNE_DONNEE": "aucune mesure reçue"}
SERIES = [TURQUOISE, ORANGE, "#B39DFB", "#F2C94C", BLEU, "#E86FA8", VERT, "#9AD0C2"]

_BASE = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
.stApp { background: radial-gradient(1200px 600px at 10% -10%, var(--halo) 0%, var(--fond) 55%) fixed;
  font-family: Inter, "Segoe UI", system-ui, sans-serif; }
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1500px; }
[data-testid="stHeader"] { background: transparent; }
[data-testid="stToolbar"] { display: none; }
[data-testid="stSidebar"] { background: var(--fond2); border-right: 1px solid var(--bord); }
h1, h2, h3, h4 { color: var(--texte); letter-spacing: .01em; }
.tdb-entete { display:flex; align-items:center; gap:18px; padding:16px 22px; border-radius:14px;
  background: linear-gradient(90deg, var(--fond2), var(--carte)); border:1px solid var(--bord); margin-bottom:14px;
  box-shadow: var(--ombre); }
.tdb-titre { font-size:1.45rem; font-weight:800; letter-spacing:.1em; color:var(--texte); line-height:1.1; }
.tdb-soustitre { color:var(--texte2); font-size:.88rem; margin-top:4px; }
.tdb-statuts { margin-left:auto; display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
.tdb-pastille { display:flex; align-items:center; gap:8px; padding:6px 12px; border-radius:999px;
  background: var(--pastille); border:1px solid var(--bord); color:var(--texte); font-size:.8rem; white-space:nowrap; }
.tdb-point { width:8px; height:8px; border-radius:50%; display:inline-block; box-shadow:0 0 6px currentColor; }
.tdb-carte { background:var(--carte); border:1px solid var(--bord); border-radius:14px; padding:16px 18px; height:100%;
  box-shadow: var(--ombre); }
.tdb-carte h4 { margin:0 0 10px 0; font-size:.9rem; color:var(--texte2); font-weight:600; letter-spacing:.06em;
  text-transform:uppercase; }
.tdb-kpi { background:var(--carte); border:1px solid var(--bord); border-radius:14px; padding:14px 16px;
  border-left:4px solid #21B6A8; min-height:118px; box-sizing:border-box; box-shadow: var(--ombre); }
.tdb-kpi .lib { color:var(--texte2); font-size:.74rem; font-weight:600; text-transform:uppercase; letter-spacing:.08em; }
.tdb-kpi .val { color:var(--texte); font-size:1.9rem; font-weight:800; line-height:1.15; margin-top:6px;
  font-variant-numeric: tabular-nums; }
.tdb-kpi .val small { font-size:.9rem; font-weight:600; color:var(--texte2); margin-left:3px; }
.tdb-kpi .sous { color:var(--texte2); font-size:.76rem; margin-top:4px; }
.tdb-badge { display:inline-block; padding:3px 10px; border-radius:999px; font-size:.74rem; font-weight:700;
  letter-spacing:.03em; color:#06121F; background:#7F95A6; white-space:nowrap; }
.tdb-grille { display:grid; grid-template-columns: 1fr 1fr; gap:10px 18px; margin-top:12px; }
.tdb-grille .lib { color:var(--texte2); font-size:.72rem; text-transform:uppercase; letter-spacing:.06em; }
.tdb-grille .val { color:var(--texte); font-size:1.02rem; font-weight:700; font-variant-numeric: tabular-nums; }
.tdb-nom { font-size:1.7rem; font-weight:800; color:var(--texte); }
.tdb-eol { width:100%; max-height:240px; border-radius:12px; margin-top:10px; display:block; }
.tdb-eol .rotor { transform-box: view-box; transform-origin: 100px 92px; animation: tdb-rot linear infinite; }
.tdb-eol .rotor.arret { animation: none; }
@keyframes tdb-rot { to { transform: rotate(360deg); } }
.tdb-feu { animation: tdb-feu 1.6s steps(1) infinite; }
@keyframes tdb-feu { 50% { opacity: .12; } }
table.tdb-table { width:100%; border-collapse:separate; border-spacing:0 6px; font-size:.86rem; }
table.tdb-table th { color:var(--texte2); font-weight:600; text-transform:uppercase; font-size:.7rem; letter-spacing:.07em;
  text-align:left; padding:4px 12px; border:none !important; background:transparent !important; white-space:nowrap; }
table.tdb-table td { background:var(--carte) !important; color:var(--texte); padding:8px 12px; white-space:nowrap;
  border-left:none; border-right:none; border-top:1px solid var(--bord) !important; border-bottom:1px solid var(--bord) !important; }
table.tdb-table tr:hover td { filter: brightness(1.06); }
table.tdb-table td:first-child { border-left-width:4px !important; border-left-style:solid !important;
  border-radius:10px 0 0 10px; font-weight:700; }
table.tdb-table td:last-child { border-right:1px solid var(--bord) !important; border-radius:0 10px 10px 0; }
table.tdb-table td.num { text-align:right; font-variant-numeric: tabular-nums; }
.tdb-alerte { background:var(--carte); border:1px solid var(--bord); border-left:5px solid #7F95A6; border-radius:12px;
  padding:12px 14px; margin-bottom:10px; box-shadow: var(--ombre); }
.tdb-alerte .t { font-weight:800; color:var(--texte); font-size:1.02rem; }
.tdb-alerte .d { color:var(--texte2); font-size:.8rem; margin-top:4px; }
.tdb-bandeau { padding:10px 14px; border-radius:12px; background:rgba(240,120,69,.12); border:1px solid #F07845;
  color:var(--texte); margin-bottom:12px; font-size:.88rem; }
.tdb-legende { display:flex; gap:16px; flex-wrap:wrap; color:var(--texte2); font-size:.78rem; margin:2px 0 4px 2px; }
.tdb-legende span i { display:inline-block; width:14px; height:10px; border-radius:3px; margin-right:6px; vertical-align:middle; }
.tdb-section { color:var(--texte); font-size:.82rem; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
  border-left:3px solid #21B6A8; padding:2px 0 2px 10px; margin:18px 0 10px 0; }
.tdb-mini { color:var(--texte2); font-size:.76rem; }
.tdb-math { text-transform:none; }
.tdb-marque { display:flex; align-items:center; gap:10px; margin:-8px 0 14px 0; color:var(--texte); font-weight:800;
  letter-spacing:.08em; font-size:.95rem; }
.tdb-marque small { display:block; color:var(--texte2); font-weight:500; letter-spacing:0; font-size:.72rem; }
.tdb-pied { color:var(--texte2); font-size:.74rem; border-top:1px solid var(--bord); margin-top:28px; padding-top:10px;
  display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }
/* Streamlit met une marge de -1rem sous chaque bloc Markdown (prévue pour le <p> final) : nos cartes HTML, sans
   marge, débordaient alors sur l'élément suivant (en-tête de carte posé sur le haut du graphique). */
[data-testid="stMarkdownContainer"]:has(> div[class^="tdb-"]) { margin-bottom:0 !important; }
.tdb-scroll { width:100%; overflow-x:auto; }
div[data-testid="stSegmentedControl"] button, div[data-testid="stButtonGroup"] button { border-radius:10px !important; }
"""

# Le thème Streamlit de base (.streamlit/config.toml) est sombre : en mode jour, on recolore ses widgets.
_WIDGETS_JOUR = """
.stApp, [data-testid="stSidebar"] { color: var(--texte); }
[data-testid="stWidgetLabel"] p, [data-testid="stSidebar"] h3, [data-testid="stMarkdownContainer"] p { color: var(--texte); }
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p,
[data-testid="stSliderTickBarMin"], [data-testid="stSliderTickBarMax"] { color: var(--texte2) !important; }
[data-baseweb="select"] > div { background: var(--carte) !important; border-color: var(--bord) !important;
  color: var(--texte) !important; }
[data-baseweb="select"] svg { fill: var(--texte2) !important; color: var(--texte2) !important; }
[data-baseweb="popover"] ul, [data-baseweb="popover"] li { background: var(--carte) !important; color: var(--texte) !important; }
[data-baseweb="popover"] li:hover, [data-baseweb="popover"] li[aria-selected="true"] { background: var(--fond) !important; }
[data-testid="stButtonGroup"] button { background: var(--carte) !important; color: var(--texte) !important;
  border-color: var(--bord) !important; }
[data-testid="stButtonGroup"] button[kind$="Active"] { color: #138C80 !important; border-color: #21B6A8 !important;
  background: rgba(33,182,168,.10) !important; }
[data-testid="stButtonGroup"] button p { color: inherit !important; }
[data-testid="stAlert"] p { color: var(--texte) !important; }
[data-testid="stSidebarCollapseButton"] svg, [data-testid="stSidebarCollapsedControl"] svg { color: var(--texte2); }
"""


def css(mode):
    """Feuille de style complète du mode choisi ('nuit' ou 'jour')."""
    p = PALETTES[mode]
    variables = ":root { " + " ".join(f"--{k}:{v};" for k, v in p.items()) + " }"
    return f"<style>{variables}{_BASE}{_WIDGETS_JOUR if mode == 'jour' else ''}</style>"


def configurer(graphique, mode="nuit"):
    """Applique la palette du mode à un graphique Altair (à appeler sur le graphique de plus haut niveau).

    La couleur des points de r et des étiquettes de valeurs vient d'ici (configure_circle / configure_text)."""
    p = PALETTES[mode]
    return (graphique.configure(background="transparent", font="Inter, Segoe UI, sans-serif")
            .configure_view(stroke=None)
            .configure_axis(gridColor=p["grille"], domainColor=p["axe"], tickColor=p["axe"], labelColor=p["texte2"],
                            titleColor=p["texte2"], labelFontSize=11, titleFontSize=12, titleFontWeight="normal")
            .configure_legend(labelColor=p["texte"], titleColor=p["texte2"], orient="top", labelFontSize=11)
            .configure_title(color=p["texte"], fontSize=13, anchor="start")
            .configure_circle(color=p["points"])
            .configure_text(color=p["texte"]))
