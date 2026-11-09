// Convertit report/Compte_Rendu_TD_Ferme_Eolienne.md en .docx (source unique : le Markdown).
// Usage : NODE_PATH=<dossier contenant node_modules/docx> node report/construire_docx.js
// Markdown pris en charge : # à ####, paragraphes, **gras**, *italique*, `code`, listes « - » et « 1. »,
// tableaux |…|, images ![légende](chemin), blocs ``` ```, équations centrées « $$ … $$ » (une ligne),
// encadrés « > », saut de page « \newpage ».  La page de garde est générée ici.
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, AlignmentType, Table, TableRow, TableCell,
  WidthType, ShadingType, BorderStyle, ImageRun, PageBreak, TableOfContents, Header, Footer,
  PageNumber, LevelFormat, TabStopType,
} = require("docx");

const RACINE = path.resolve(__dirname, "..");
const MD = path.join(__dirname, "Compte_Rendu_TD_Ferme_Eolienne.md");
const SORTIE = path.join(__dirname, "Compte_Rendu_TD_Ferme_Eolienne.docx");
const LARGEUR = 9026; // A4 portrait, marges 1440 : 11906 - 2*1440
const POLICE = "Calibri";
const BLEU = "1F4E79";

function pngTaille(buf) { return { w: buf.readUInt32BE(16), h: buf.readUInt32BE(20) }; }

// --- texte en ligne : **gras**, *italique*, `code`
function runs(texte, base = {}) {
  const res = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*)/g;
  let i = 0, m;
  while ((m = re.exec(texte))) {
    if (m.index > i) res.push(new TextRun({ text: texte.slice(i, m.index), ...base }));
    const t = m[0];
    if (t.startsWith("**")) res.push(new TextRun({ text: t.slice(2, -2), bold: true, ...base }));
    else if (t.startsWith("`")) res.push(new TextRun({ text: t.slice(1, -1), font: "Consolas", size: 19, color: "7A2E0E", ...base }));
    else res.push(new TextRun({ text: t.slice(1, -1), italics: true, ...base }));
    i = m.index + t.length;
  }
  if (i < texte.length) res.push(new TextRun({ text: texte.slice(i), ...base }));
  return res;
}

const bord = { style: BorderStyle.SINGLE, size: 4, color: "A6A6A6" };
const bords = { top: bord, bottom: bord, left: bord, right: bord };

function tableau(lignes) {
  const cellules = lignes.map((l) => l.replace(/^\||\|$/g, "").split("|").map((c) => c.trim()));
  const entete = cellules[0];
  const corps = cellules.slice(2); // ligne 1 = séparateur ---
  const n = entete.length;
  // largeur proportionnelle à la longueur maximale du contenu de chaque colonne (bornée)
  const longueurs = entete.map((_, j) => Math.min(60, Math.max(6, ...cellules.filter((_, k) => k !== 1).map((r) => (r[j] || "").length))));
  const total = longueurs.reduce((a, b) => a + b, 0);
  const largeurs = longueurs.map((x) => Math.floor((x / total) * LARGEUR));
  largeurs[n - 1] += LARGEUR - largeurs.reduce((a, b) => a + b, 0);
  const ligne = (r, tete) => new TableRow({
    tableHeader: tete,
    children: r.map((c, j) => new TableCell({
      borders: bords, width: { size: largeurs[j], type: WidthType.DXA },
      shading: tete ? { fill: "DCE6F1", type: ShadingType.CLEAR, color: "auto" } : undefined,
      margins: { top: 50, bottom: 50, left: 90, right: 90 },
      children: [new Paragraph({ spacing: { before: 0, after: 0 }, children: runs(c, { size: 19, bold: tete || undefined }) })],
    })),
  });
  return new Table({
    width: { size: LARGEUR, type: WidthType.DXA }, columnWidths: largeurs,
    rows: [ligne(entete, true), ...corps.map((r) => ligne(r, false))],
  });
}

let numFigure = 0;
function image(legende, chemin) {
  const fichier = path.resolve(__dirname, chemin);
  const buf = fs.readFileSync(fichier);
  const { w, h } = pngTaille(buf);
  const largeurPx = 600; // ~15,9 cm
  const hauteurPx = Math.round((h / w) * largeurPx);
  const limite = 760;
  const echelle = hauteurPx > limite ? limite / hauteurPx : 1;
  numFigure += 1;
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 120, after: 60 },
      children: [new ImageRun({ type: "png", data: buf, transformation: { width: Math.round(largeurPx * echelle), height: Math.round(hauteurPx * echelle) },
        altText: { title: legende, description: legende, name: path.basename(fichier) } })] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 },
      children: [new TextRun({ text: `Figure ${numFigure} – `, bold: true, size: 19, color: BLEU }), ...runs(legende, { size: 19, italics: true })] }),
  ];
}

function convertir(md) {
  const L = md.replace(/\r/g, "").split("\n");
  const blocs = [];
  let para = [];
  let instance = 0, dansListe = false;   // une instance de numérotation par liste : chaque liste repart de 1
  const vider = () => {
    if (para.length) blocs.push(new Paragraph({ alignment: AlignmentType.JUSTIFIED, spacing: { after: 120, line: 276 }, children: runs(para.join(" ")) }));
    para = [];
  };
  for (let i = 0; i < L.length; i++) {
    const l = L[i];
    if (/^\s*$/.test(l)) { vider(); continue; }
    if (!/^\d+\. /.test(l) && !/^\s+\S/.test(l)) dansListe = false;
    if (l.trim() === "\\newpage") { vider(); blocs.push(new Paragraph({ children: [new PageBreak()] })); continue; }
    let m;
    if ((m = /^(#{1,4}) (.*)$/.exec(l))) {
      vider();
      const niveaux = [HeadingLevel.HEADING_1, HeadingLevel.HEADING_2, HeadingLevel.HEADING_3, HeadingLevel.HEADING_4];
      blocs.push(new Paragraph({ heading: niveaux[m[1].length - 1], pageBreakBefore: m[1].length === 1 && blocs.length > 0, children: runs(m[2]) }));
      continue;
    }
    if ((m = /^!\[(.*)\]\((.*)\)\s*$/.exec(l))) { vider(); blocs.push(...image(m[1], m[2])); continue; }
    if ((m = /^\$\$(.*)\$\$\s*$/.exec(l))) {
      vider();
      blocs.push(new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 100, after: 140 },
        children: [new TextRun({ text: m[1].trim(), font: "Cambria Math", size: 24 })] }));
      continue;
    }
    if (l.startsWith("```")) {
      vider();
      const code = [];
      for (i++; i < L.length && !L[i].startsWith("```"); i++) code.push(L[i]);
      code.forEach((c, k) => blocs.push(new Paragraph({
        shading: { fill: "F2F2F2", type: ShadingType.CLEAR, color: "auto" },
        spacing: { before: k === 0 ? 80 : 0, after: k === code.length - 1 ? 140 : 0, line: 240 },
        children: [new TextRun({ text: c.length ? c : " ", font: "Consolas", size: 15 })] })));
      continue;
    }
    if (l.startsWith("|")) {
      vider();
      const t = [];
      for (; i < L.length && L[i].startsWith("|"); i++) t.push(L[i]);
      i--;
      blocs.push(tableau(t));
      blocs.push(new Paragraph({ spacing: { after: 120 }, children: [] }));
      continue;
    }
    if ((m = /^(\s*)- (.*)$/.exec(l))) {
      vider();
      blocs.push(new Paragraph({ numbering: { reference: "puces", level: m[1].length >= 2 ? 1 : 0 }, alignment: AlignmentType.JUSTIFIED, spacing: { after: 60 }, children: runs(m[2]) }));
      continue;
    }
    if ((m = /^\d+\. (.*)$/.exec(l))) {
      vider();
      if (!dansListe) { instance += 1; dansListe = true; }
      blocs.push(new Paragraph({ numbering: { reference: "numeros", level: 0, instance }, alignment: AlignmentType.JUSTIFIED, spacing: { after: 60 }, children: runs(m[1]) }));
      continue;
    }
    if ((m = /^> ?(.*)$/.exec(l))) {
      vider();
      blocs.push(new Paragraph({ shading: { fill: "EAF1F8", type: ShadingType.CLEAR, color: "auto" },
        border: { left: { style: BorderStyle.SINGLE, size: 18, color: BLEU, space: 8 } },
        indent: { left: 200 }, spacing: { before: 60, after: 60 }, children: runs(m[1]) }));
      continue;
    }
    para.push(l.trim());
  }
  vider();
  return blocs;
}

function pageDeGarde(meta) {
  const ligne = (t, o = {}) => new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: o.after ?? 120 }, children: [new TextRun({ text: t, ...o })] });
  return [
    ligne("Le Mans Université", { size: 32, bold: true, color: BLEU, after: 60 }),
    ligne("Master Informatique — 2e année", { size: 26, after: 60 }),
    ligne("Année universitaire 2026-2027", { size: 24, after: 1600 }),
    ligne("TD3 — Une ferme éolienne connectée", { size: 44, bold: true, color: BLEU, after: 200 }),
    ligne("Jumeau numérique et analyse dimensionnelle", { size: 30, italics: true, after: 300 }),
    ligne("Compte rendu", { size: 28, after: 1600 }),
    ligne(`Étudiant(e)s : ${meta.etudiant}`, { size: 24 }),
    ligne(`Binôme : ${meta.binome}`, { size: 24 }),
    ligne(`Enseignant : ${meta.enseignant}`, { size: 24 }),
    ligne(`Date de remise : ${meta.remise}`, { size: 24, after: 600 }),
    new Paragraph({ children: [new PageBreak()] }),
    new Paragraph({ spacing: { after: 200 }, children: [new TextRun({ text: "Table des matières", font: POLICE, size: 32, bold: true, color: BLEU })] }),
    new TableOfContents("Table des matières", { hyperlink: true, headingStyleRange: "1-2" }),
  ];
}

const md = fs.readFileSync(MD, "utf8");
// En-tête YAML minimal : etudiant, binome, enseignant, remise
const yaml = /^---\n([\s\S]*?)\n---\n/.exec(md.replace(/\r/g, ""));
const meta = {};
(yaml ? yaml[1] : "").split("\n").forEach((l) => { const k = l.indexOf(":"); if (k > 0) meta[l.slice(0, k).trim()] = l.slice(k + 1).trim(); });
const corps = yaml ? md.replace(/\r/g, "").slice(yaml[0].length) : md;

const doc = new Document({
  creator: meta.etudiant || "", title: "TD3 — Une ferme éolienne connectée", description: "Compte rendu",
  styles: {
    default: { document: { run: { font: POLICE, size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: POLICE, size: 32, bold: true, color: BLEU }, paragraph: { spacing: { before: 240, after: 160 }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: POLICE, size: 27, bold: true, color: BLEU }, paragraph: { spacing: { before: 280, after: 120 }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: POLICE, size: 23, bold: true, color: "2E75B6" }, paragraph: { spacing: { before: 200, after: 80 }, keepNext: true, outlineLevel: 2 } },
      { id: "Heading4", name: "Heading 4", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: POLICE, size: 22, bold: true, italics: true, color: "404040" }, paragraph: { spacing: { before: 160, after: 60 }, keepNext: true, outlineLevel: 3 } },
    ],
  },
  numbering: { config: [
    { reference: "puces", levels: [
      { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } },
      { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1000, hanging: 270 } } } } ] },
    { reference: "numeros", levels: [
      { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 300 } } } } ] },
  ] },
  sections: [{
    properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1440, bottom: 1300, left: 1440, right: 1440 } }, titlePage: true },
    headers: { default: new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT,
      border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "BFBFBF", space: 4 } },
      children: [new TextRun({ text: "TD3 — Ferme éolienne connectée · Master Informatique · Le Mans Université", size: 16, color: "808080" })] })] }) },
    footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER,
      children: [new TextRun({ children: ["Page ", PageNumber.CURRENT, " / ", PageNumber.TOTAL_PAGES], size: 16, color: "808080" })] })] }) },
    children: [...pageDeGarde(meta), ...convertir(corps)],
  }],
});

Packer.toBuffer(doc).then((b) => { fs.writeFileSync(SORTIE, b); console.log(`${SORTIE} : ${numFigure} figures, ${b.length} octets`); });
