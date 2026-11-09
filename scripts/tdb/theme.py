"""Identité visuelle du tableau de bord : palette, couleurs des états, CSS, configuration des graphiques Altair."""

FOND = "#020D19"
FOND2 = "#062333"
CARTE = "#073044"
BORD = "#0E4560"
TURQUOISE = "#21B6A8"
VERT = "#42B883"
ORANGE = "#F07845"
ROUGE = "#EF5350"
BLEU = "#4FA3E0"
GRIS = "#7F95A6"
TEXTE = "#F4F7FA"
TEXTE2 = "#A9BBC8"
POINTS = "#7FC8F8"

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

CSS = f"""
<style>
:root {{ --fond:{FOND}; --fond2:{FOND2}; --carte:{CARTE}; --bord:{BORD}; --turquoise:{TURQUOISE};
        --texte:{TEXTE}; --texte2:{TEXTE2}; }}
.stApp {{ background: radial-gradient(1200px 600px at 10% -10%, #08304a 0%, {FOND} 55%) fixed; }}
.block-container {{ padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1500px; }}
[data-testid="stHeader"] {{ background: transparent; }}
[data-testid="stToolbar"] {{ display: none; }}
[data-testid="stSidebar"] {{ background: {FOND2}; border-right: 1px solid {BORD}; }}
h1, h2, h3, h4 {{ color: {TEXTE}; letter-spacing: .01em; }}
.tdb-entete {{ display:flex; align-items:center; gap:18px; padding:16px 22px; border-radius:16px;
  background: linear-gradient(90deg, {FOND2}, {CARTE}); border:1px solid {BORD}; margin-bottom:14px; }}
.tdb-titre {{ font-size:1.55rem; font-weight:800; letter-spacing:.12em; color:{TEXTE}; line-height:1.1; }}
.tdb-soustitre {{ color:{TEXTE2}; font-size:.92rem; margin-top:4px; }}
.tdb-statuts {{ margin-left:auto; display:flex; gap:10px; flex-wrap:wrap; justify-content:flex-end; }}
.tdb-pastille {{ display:flex; align-items:center; gap:8px; padding:7px 12px; border-radius:999px;
  background: rgba(2,13,25,.55); border:1px solid {BORD}; color:{TEXTE}; font-size:.82rem; white-space:nowrap; }}
.tdb-point {{ width:9px; height:9px; border-radius:50%; display:inline-block; box-shadow:0 0 8px currentColor; }}
.tdb-carte {{ background:{CARTE}; border:1px solid {BORD}; border-radius:16px; padding:16px 18px; height:100%; }}
.tdb-carte h4 {{ margin:0 0 10px 0; font-size:.95rem; color:{TEXTE2}; font-weight:600; letter-spacing:.06em;
  text-transform:uppercase; }}
.tdb-kpi {{ background:{CARTE}; border:1px solid {BORD}; border-radius:16px; padding:14px 16px; border-left:4px solid {TURQUOISE}; }}
.tdb-kpi .lib {{ color:{TEXTE2}; font-size:.78rem; text-transform:uppercase; letter-spacing:.08em; }}
.tdb-kpi .val {{ color:{TEXTE}; font-size:2rem; font-weight:800; line-height:1.15; margin-top:4px; }}
.tdb-kpi .sous {{ color:{TEXTE2}; font-size:.78rem; margin-top:2px; }}
.tdb-badge {{ display:inline-block; padding:3px 10px; border-radius:999px; font-size:.75rem; font-weight:700;
  letter-spacing:.04em; color:{FOND}; background:{GRIS}; white-space:nowrap; }}
.tdb-grille {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px 18px; margin-top:12px; }}
.tdb-grille .lib {{ color:{TEXTE2}; font-size:.75rem; text-transform:uppercase; letter-spacing:.06em; }}
.tdb-grille .val {{ color:{TEXTE}; font-size:1.05rem; font-weight:700; }}
.tdb-nom {{ font-size:1.8rem; font-weight:800; color:{TEXTE}; }}
.tdb-eol {{ width:100%; max-height:230px; }}
.tdb-eol .rotor {{ transform-box: view-box; transform-origin: 100px 92px; animation: tdb-rot linear infinite; }}
.tdb-eol .rotor.arret {{ animation: none; }}
@keyframes tdb-rot {{ to {{ transform: rotate(360deg); }} }}
table.tdb-table {{ width:100%; border-collapse:separate; border-spacing:0 6px; font-size:.88rem; }}
table.tdb-table th {{ color:{TEXTE2}; font-weight:600; text-transform:uppercase; font-size:.72rem; letter-spacing:.07em;
  text-align:left; padding:4px 12px; }}
table.tdb-table td {{ background:{CARTE} !important; color:{TEXTE}; padding:7px 12px; border-top:1px solid {BORD} !important; border-bottom:1px solid {BORD} !important; }}
table.tdb-table td:first-child {{ border-left-width:4px !important; border-left-style:solid !important; border-radius:10px 0 0 10px; font-weight:700; }}
table.tdb-table td:last-child {{ border-right:1px solid {BORD}; border-radius:0 10px 10px 0; }}
table.tdb-table td.num {{ text-align:right; font-variant-numeric: tabular-nums; }}
.tdb-alerte {{ background:{CARTE}; border:1px solid {BORD}; border-left:5px solid {GRIS}; border-radius:12px;
  padding:12px 14px; margin-bottom:10px; }}
.tdb-alerte .t {{ font-weight:800; color:{TEXTE}; font-size:1.05rem; }}
.tdb-alerte .d {{ color:{TEXTE2}; font-size:.82rem; margin-top:4px; }}
.tdb-bandeau {{ padding:10px 14px; border-radius:12px; background:rgba(240,120,69,.12); border:1px solid {ORANGE};
  color:{TEXTE}; margin-bottom:12px; font-size:.9rem; }}
.tdb-legende {{ display:flex; gap:16px; flex-wrap:wrap; color:{TEXTE2}; font-size:.8rem; margin:2px 0 4px 2px; }}
.tdb-legende span i {{ display:inline-block; width:14px; height:10px; border-radius:3px; margin-right:6px; vertical-align:middle; }}
.tdb-section {{ color:{TEXTE}; font-size:.86rem; font-weight:700; letter-spacing:.08em; text-transform:uppercase;
  border-left:3px solid {TURQUOISE}; padding:2px 0 2px 10px; margin:16px 0 10px 0; }}
table.tdb-table, table.tdb-table th, table.tdb-table td {{ border-left:none; border-right:none; }}
table.tdb-table th {{ border:none !important; background:transparent !important; white-space:nowrap; }}
table.tdb-table td {{ white-space:nowrap; }}
.tdb-mini {{ color:{TEXTE2}; font-size:.78rem; }}
.tdb-math {{ text-transform:none; }}
.tdb-scroll {{ width:100%; overflow-x:auto; }}
.tdb-kpi {{ min-height:118px; box-sizing:border-box; }}
div[data-testid="stSegmentedControl"] button, div[data-testid="stButtonGroup"] button {{ border-radius:10px !important; }}
</style>
"""


def configurer(graphique):
    """Applique le thème sombre à un graphique Altair (à appeler sur le graphique de plus haut niveau)."""
    return (graphique.configure(background="transparent", font="Inter, Segoe UI, sans-serif")
            .configure_view(stroke=None)
            .configure_axis(gridColor="rgba(169,187,200,0.13)", domainColor="rgba(169,187,200,0.35)",
                            tickColor="rgba(169,187,200,0.35)", labelColor=TEXTE2, titleColor=TEXTE2,
                            labelFontSize=11, titleFontSize=12, titleFontWeight="normal")
            .configure_legend(labelColor=TEXTE, titleColor=TEXTE2, orient="top", labelFontSize=11)
            .configure_title(color=TEXTE, fontSize=13, anchor="start"))
