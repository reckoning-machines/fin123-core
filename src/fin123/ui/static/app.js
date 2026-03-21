// fin123 UI — Dark, dense, keyboard-first canvas grid
"use strict";

// ── Constants ──
const COL_W = 90;
const ROW_H = 22;
const HDR_H = 22;  // column header height
const HDR_W = 46;  // row header width
const FONT = "12px 'JetBrains Mono','SF Mono','Cascadia Code','Fira Code','Consolas',monospace";
const FONT_HDR = "11px 'JetBrains Mono','SF Mono','Cascadia Code','Fira Code','Consolas',monospace";

// ── Theme ──
const T = {
  bg:       "#1b1917",
  hdr:      "#201e1b",
  corner:   "#201e1b",
  gridline: "#2a2622",
  hdrText:  "#8e8376",
  cellText: "#e7e2db",
  fmText:   "#f59e0b",  // formula cells
  errMark:  "#fb7185",  // error marker
  selFill:  "rgba(96, 165, 250, 0.12)",
  selBorder:"rgba(96, 165, 250, 0.50)",
  cursor:   "rgba(96, 165, 250, 0.75)",
  precFill: "rgba(168, 85, 247, 0.15)",
  precBorder: "rgba(168, 85, 247, 0.55)",
};

// ── State ──
const S = {
  // Per-sheet state keyed by sheet name
  sheetState: {},
  activeSheet: "Sheet1",
  sheets: ["Sheet1"],
  nRows: 200,
  nCols: 40,
  scrollRow: 0,
  scrollCol: 0,
  curRow: 0,
  curCol: 0,
  selRow: 0,
  selCol: 0,
  selecting: false,
  cells: {},
  fmt: {},       // addr -> { color: "#hex" }
  editing: false,
  dirty: false,
  snapVer: null,
  lastRunId: null,
  outputTables: [],
  workflows: [],
  hoverRow: -1,
  panelOpen: true,
  kbOverlay: false,
  errors: {},
  readOnly: false,
  modelVersions: [],
  precedents: [],           // addresses of direct precedent cells
  showPrecedents: localStorage.getItem("fin123_showPrecedents") === "1",
  uiMode: localStorage.getItem("fin123_uiMode") || "spreadsheet",
};

// Save current sheet state before switching
function saveSheetState() {
  S.sheetState[S.activeSheet] = {
    scrollRow: S.scrollRow, scrollCol: S.scrollCol,
    curRow: S.curRow, curCol: S.curCol,
    selRow: S.selRow, selCol: S.selCol,
    cells: S.cells, fmt: S.fmt,
    errors: S.errors,
    nRows: S.nRows, nCols: S.nCols,
  };
}

// Restore sheet state (or defaults)
function restoreSheetState(name) {
  const st = S.sheetState[name];
  if (st) {
    S.scrollRow = st.scrollRow; S.scrollCol = st.scrollCol;
    S.curRow = st.curRow; S.curCol = st.curCol;
    S.selRow = st.selRow; S.selCol = st.selCol;
    S.cells = st.cells; S.fmt = st.fmt;
    S.errors = st.errors;
    S.nRows = st.nRows; S.nCols = st.nCols;
  } else {
    S.scrollRow = 0; S.scrollCol = 0;
    S.curRow = 0; S.curCol = 0;
    S.selRow = 0; S.selCol = 0;
    S.cells = {}; S.fmt = {};
    S.errors = {};
    S.nRows = 200; S.nCols = 40;
  }
}

// ── DOM refs ──
const canvas   = document.getElementById("grid-canvas");
const ctx      = canvas.getContext("2d");
const cellAddr = document.getElementById("cell-addr");
const fbar     = document.getElementById("formula-bar");
const editor   = document.getElementById("cell-editor");
const dirtyDot = document.getElementById("dirty-dot");
const dirtyLbl = document.getElementById("dirty-label");
const snapEl   = document.getElementById("snap-ver");
const logEl    = document.getElementById("log-msg");
const posEl    = document.getElementById("pos-info");
const sidePanel= document.getElementById("side-panel");
const kbOverlay= document.getElementById("kb-overlay");
const colorDot = document.getElementById("color-dot");
const sheetTabList = document.getElementById("sheet-tab-list");

// ── Helpers ──
function colLetter(c) {
  let s = "";
  let n = c + 1;
  while (n > 0) { n--; s = String.fromCharCode(65 + (n % 26)) + s; n = Math.floor(n / 26); }
  return s;
}
function addr(r, c) { return colLetter(c) + (r + 1); }

function log(msg, cls) {
  logEl.textContent = msg;
  logEl.className = cls || "";
}

function showToast(msg) { log(msg, "success"); }

function updateStatus() {
  if (S.dirty) {
    dirtyDot.className = "dot dot-dirty";
    dirtyLbl.textContent = "Uncommitted";
  } else {
    dirtyDot.className = "dot dot-clean";
    dirtyLbl.textContent = "Committed";
  }
  snapEl.textContent = S.snapVer ? S.snapVer : "\u2014";
  posEl.textContent = addr(S.curRow, S.curCol);
  updateColorDot();
}

function updateColorDot() {
  const a = addr(S.curRow, S.curCol);
  const f = S.fmt[a];
  if (f && f.color) {
    colorDot.style.background = f.color;
    colorDot.classList.add("visible");
  } else {
    colorDot.classList.remove("visible");
  }
}

// Extract direct precedent cell addresses from a raw formula string.
// Returns an array of uppercase address strings (e.g. ["A1","B2"]).
// Only handles bare A1 refs on the current sheet — no cross-sheet refs.
function extractPrecedents(raw) {
  if (!raw || !raw.startsWith("=")) return [];
  const formula = raw.slice(1);
  const refs = new Set();
  // Skip string literals
  const strRanges = [];
  const strRe = /"(?:[^"\\]|\\.)*"/g;
  let sm;
  while ((sm = strRe.exec(formula)) !== null) {
    strRanges.push([sm.index, sm.index + sm[0].length]);
  }
  function inStr(idx) {
    for (const [a, b] of strRanges) { if (idx >= a && idx < b) return true; }
    return false;
  }
  // Match bare A1 refs (not preceded/followed by identifier chars)
  const refRe = /(?<![A-Za-z_])([A-Z]{1,3})(\d+)(?![A-Za-z0-9_])/g;
  let m;
  while ((m = refRe.exec(formula)) !== null) {
    if (inStr(m.index)) continue;
    refs.add(m[1] + m[2]);
  }
  return Array.from(refs);
}

function selRect() {
  const r0 = Math.min(S.curRow, S.selRow);
  const r1 = Math.max(S.curRow, S.selRow);
  const c0 = Math.min(S.curCol, S.selCol);
  const c1 = Math.max(S.curCol, S.selCol);
  return { r0, r1, c0, c1 };
}

// ── API helpers ──
async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch("/api" + path, opts);
  if (!resp.ok) {
    const text = await resp.text();
    let detail = text;
    try { detail = JSON.parse(text).detail || text; } catch(_) {}
    throw new Error(detail);
  }
  return resp.json();
}

// ── Canvas drawing ──
let _resizeTimer = null;
function resizeCanvas() {
  clearTimeout(_resizeTimer);
  _resizeTimer = setTimeout(() => {
    const container = document.getElementById("grid-container");
    canvas.width = container.clientWidth;
    canvas.height = container.clientHeight;
    draw();
  }, 16);
}

function visibleRows() { return Math.floor((canvas.height - HDR_H) / ROW_H) + 1; }
function visibleCols() { return Math.floor((canvas.width - HDR_W) / COL_W) + 1; }

function draw() {
  const w = canvas.width, h = canvas.height;
  if (!w || !h) return;
  ctx.clearRect(0, 0, w, h);

  const vRows = visibleRows();
  const vCols = visibleCols();
  const sr = selRect();

  // Background
  ctx.fillStyle = T.bg;
  ctx.fillRect(0, 0, w, h);

  // Grid lines (thin)
  ctx.strokeStyle = T.gridline;
  ctx.lineWidth = 1;
  for (let ri = 0; ri <= vRows; ri++) {
    const y = HDR_H + ri * ROW_H + 0.5;
    ctx.beginPath(); ctx.moveTo(HDR_W, y); ctx.lineTo(w, y); ctx.stroke();
  }
  for (let ci = 0; ci <= vCols; ci++) {
    const x = HDR_W + ci * COL_W + 0.5;
    ctx.beginPath(); ctx.moveTo(x, HDR_H); ctx.lineTo(x, h); ctx.stroke();
  }

  // Hover row highlight
  if (S.hoverRow >= 0) {
    const hri = S.hoverRow - S.scrollRow;
    if (hri >= 0 && hri < vRows) {
      ctx.fillStyle = "rgba(255, 255, 255, 0.015)";
      ctx.fillRect(HDR_W, HDR_H + hri * ROW_H, w - HDR_W, ROW_H);
    }
  }

  // Selection highlight
  const sr0vi = sr.r0 - S.scrollRow;
  const sr1vi = sr.r1 - S.scrollRow;
  const sc0vi = sr.c0 - S.scrollCol;
  const sc1vi = sr.c1 - S.scrollCol;
  ctx.fillStyle = T.selFill;
  for (let ri = Math.max(0, sr0vi); ri <= Math.min(vRows - 1, sr1vi); ri++) {
    for (let ci = Math.max(0, sc0vi); ci <= Math.min(vCols - 1, sc1vi); ci++) {
      const x = HDR_W + ci * COL_W;
      const y = HDR_H + ri * ROW_H;
      ctx.fillRect(x + 1, y + 1, COL_W - 1, ROW_H - 1);
    }
  }

  // Active cell border
  const acri = S.curRow - S.scrollRow;
  const acci = S.curCol - S.scrollCol;
  if (acri >= 0 && acri < vRows && acci >= 0 && acci < vCols) {
    const x = HDR_W + acci * COL_W;
    const y = HDR_H + acri * ROW_H;
    ctx.strokeStyle = T.cursor;
    ctx.lineWidth = 2;
    ctx.strokeRect(x + 0.5, y + 0.5, COL_W - 1, ROW_H - 1);
  }

  // Precedent highlights
  if (S.showPrecedents && S.precedents.length > 0) {
    for (const pa of S.precedents) {
      const pm = pa.match(/^([A-Z]+)(\d+)$/);
      if (!pm) continue;
      let pc = 0;
      for (const ch of pm[1]) pc = pc * 26 + (ch.charCodeAt(0) - 64);
      pc -= 1;
      const pr = parseInt(pm[2]) - 1;
      const pri = pr - S.scrollRow;
      const pci = pc - S.scrollCol;
      if (pri >= 0 && pri < vRows && pci >= 0 && pci < vCols) {
        const px = HDR_W + pci * COL_W;
        const py = HDR_H + pri * ROW_H;
        ctx.fillStyle = T.precFill;
        ctx.fillRect(px + 1, py + 1, COL_W - 1, ROW_H - 1);
        ctx.strokeStyle = T.precBorder;
        ctx.lineWidth = 1;
        ctx.strokeRect(px + 0.5, py + 0.5, COL_W, ROW_H);
      }
    }
  }

  // Column headers
  ctx.fillStyle = T.hdr;
  ctx.fillRect(HDR_W, 0, w - HDR_W, HDR_H);
  ctx.font = FONT_HDR;
  ctx.fillStyle = T.hdrText;
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  for (let ci = 0; ci < vCols; ci++) {
    const c = S.scrollCol + ci;
    if (c >= S.nCols) break;
    const x = HDR_W + ci * COL_W;
    ctx.fillText(colLetter(c), x + COL_W / 2, HDR_H / 2);
  }

  // Row headers
  ctx.fillStyle = T.hdr;
  ctx.fillRect(0, HDR_H, HDR_W, h - HDR_H);
  ctx.fillStyle = T.hdrText;
  ctx.textAlign = "center";
  for (let ri = 0; ri < vRows; ri++) {
    const r = S.scrollRow + ri;
    if (r >= S.nRows) break;
    const y = HDR_H + ri * ROW_H;
    ctx.fillText(String(r + 1), HDR_W / 2, y + ROW_H / 2);
  }

  // Corner
  ctx.fillStyle = T.corner;
  ctx.fillRect(0, 0, HDR_W, HDR_H);

  // Header border lines
  ctx.strokeStyle = T.gridline;
  ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(0, HDR_H + 0.5); ctx.lineTo(w, HDR_H + 0.5); ctx.stroke();
  ctx.beginPath(); ctx.moveTo(HDR_W + 0.5, 0); ctx.lineTo(HDR_W + 0.5, h); ctx.stroke();

  // Cell values + error markers + font color
  ctx.font = FONT;
  ctx.textAlign = "left";
  ctx.textBaseline = "middle";
  for (let ri = 0; ri < vRows; ri++) {
    for (let ci = 0; ci < vCols; ci++) {
      const r = S.scrollRow + ri;
      const c = S.scrollCol + ci;
      if (r >= S.nRows || c >= S.nCols) continue;
      const a = addr(r, c);
      const cellX = HDR_W + ci * COL_W;
      const cellY = HDR_H + ri * ROW_H;

      // Error marker: small red triangle in top-right corner
      if (S.errors[a]) {
        ctx.fillStyle = T.errMark;
        ctx.beginPath();
        ctx.moveTo(cellX + COL_W - 1, cellY + 1);
        ctx.lineTo(cellX + COL_W - 7, cellY + 1);
        ctx.lineTo(cellX + COL_W - 1, cellY + 7);
        ctx.closePath();
        ctx.fill();
      }

      const cell = S.cells[a];
      if (!cell) continue;
      const x = cellX + 4;
      const y = cellY + ROW_H / 2;

      // Determine text color: fmt.color > formula blue > default
      const fmtEntry = S.fmt[a];
      if (fmtEntry && fmtEntry.color) {
        ctx.fillStyle = fmtEntry.color;
      } else if (cell.raw.startsWith("=")) {
        ctx.fillStyle = T.fmText;
      } else {
        ctx.fillStyle = T.cellText;
      }

      const text = cell.display || cell.raw;
      ctx.save();
      ctx.beginPath();
      ctx.rect(cellX + 1, cellY + 1, COL_W - 2, ROW_H - 2);
      ctx.clip();
      ctx.fillText(text, x, y);
      ctx.restore();
    }
  }
}

// ── Scroll management ──
function ensureVisible(r, c) {
  const vr = visibleRows() - 1;
  const vc = visibleCols() - 1;
  if (r < S.scrollRow) S.scrollRow = r;
  else if (r >= S.scrollRow + vr) S.scrollRow = r - vr + 1;
  if (c < S.scrollCol) S.scrollCol = c;
  else if (c >= S.scrollCol + vc) S.scrollCol = c - vc + 1;
}

function moveCursor(dr, dc, shift) {
  const nr = Math.max(0, Math.min(S.nRows - 1, S.curRow + dr));
  const nc = Math.max(0, Math.min(S.nCols - 1, S.curCol + dc));
  S.curRow = nr; S.curCol = nc;
  if (!shift) { S.selRow = nr; S.selCol = nc; }
  ensureVisible(nr, nc);
  updateCellInfo();
  draw();
}

function updateCellInfo() {
  const a = addr(S.curRow, S.curCol);
  cellAddr.value = a;
  const cell = S.cells[a];
  fbar.value = cell ? cell.raw : "";
  // Recompute precedents for the active cell
  S.precedents = cell ? extractPrecedents(cell.raw) : [];
  updateStatus();
}

// ── Error tracking ──
function addErrors(errs) {
  for (const e of errs) {
    if (e.addr) S.errors[e.addr] = { code: e.code, message: e.message, position: e.position };
  }
  renderErrors();
  draw();
}

function clearError(a) {
  delete S.errors[a];
  renderErrors();
}

function renderErrors() {
  const el = document.getElementById("error-list");
  if (!el) return;
  el.innerHTML = "";
  const errEntries = Object.entries(S.errors);
  if (errEntries.length === 0) {
    el.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No errors</div>';
    return;
  }
  for (const [a, err] of errEntries) {
    const item = document.createElement("div");
    item.className = "err-item";
    item.innerHTML = `<span class="err-addr">${esc(a)}</span><span class="err-msg">${esc(err.message)}</span>`;
    item.addEventListener("click", () => jumpToCell(a));
    el.appendChild(item);
  }
}

function jumpToCell(a) {
  const m = a.match(/^([A-Z]+)(\d+)$/);
  if (!m) return;
  let col = 0;
  for (const ch of m[1]) col = col * 26 + (ch.charCodeAt(0) - 64);
  col -= 1;
  const row = parseInt(m[2]) - 1;
  S.curRow = row; S.curCol = col;
  S.selRow = row; S.selCol = col;
  ensureVisible(row, col);
  updateCellInfo();
  draw();
  canvas.focus();
}

function showErrorsPanel() {
  if (!S.panelOpen) togglePanel();
  const tabSection = document.querySelector("#side-panel .panel-section:last-child");
  if (!tabSection) return;
  tabSection.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  tabSection.querySelectorAll(".tab-content").forEach(tc => tc.classList.remove("active"));
  const errTab = tabSection.querySelector('[data-tab="errors"]');
  if (errTab) errTab.classList.add("active");
  const errContent = document.getElementById("tab-errors");
  if (errContent) errContent.classList.add("active");
}

// ── Cell editing ──
function startEdit(initialText) {
  S.editing = true;
  const ri = S.curRow - S.scrollRow;
  const ci = S.curCol - S.scrollCol;
  if (ri < 0 || ci < 0) return;
  editor.style.display = "block";
  editor.style.left = (HDR_W + ci * COL_W) + "px";
  editor.style.top = (HDR_H + ri * ROW_H) + "px";
  editor.style.width = COL_W + "px";
  editor.style.height = ROW_H + "px";

  if (initialText !== undefined) {
    editor.value = initialText;
  } else {
    const a = addr(S.curRow, S.curCol);
    const cell = S.cells[a];
    editor.value = cell ? cell.raw : "";
  }
  editor.focus();
  if (initialText !== undefined) {
    editor.selectionStart = editor.selectionEnd = editor.value.length;
  } else {
    editor.select();
  }
}

function commitEdit() {
  const raw = editor.value;
  const a = addr(S.curRow, S.curCol);
  cancelEdit();
  if (raw === "") {
    delete S.cells[a];
  } else {
    S.cells[a] = { raw, display: raw };
  }
  clearError(a);
  fbar.value = raw;
  api("POST", "/sheet/cells", {
    sheet: S.activeSheet,
    edits: [{ addr: a, value: raw.startsWith("=") ? undefined : raw, formula: raw.startsWith("=") ? raw : undefined }]
  }).then(res => {
    S.dirty = res.dirty;
    if (res.errors && res.errors.length) {
      addErrors(res.errors);
      log("Error: " + res.errors[0].message, "error");
    }
    updateStatus();
    // Reload viewport to get computed display values
    loadSheet();
  }).catch(err => log("Error: " + err.message, "error"));
  draw();
}

function cancelEdit() {
  S.editing = false;
  editor.style.display = "none";
  canvas.focus();
}

// ── Formula bar editing ──
let _fbarValidateTimer = null;
fbar.addEventListener("keydown", e => {
  if (e.key === "Enter") {
    e.preventDefault();
    const raw = fbar.value;
    const a = addr(S.curRow, S.curCol);
    if (raw === "") {
      delete S.cells[a];
    } else {
      S.cells[a] = { raw, display: raw };
    }
    clearError(a);
    api("POST", "/sheet/cells", {
      sheet: S.activeSheet,
      edits: [{ addr: a, value: raw.startsWith("=") ? undefined : raw, formula: raw.startsWith("=") ? raw : undefined }]
    }).then(res => {
      S.dirty = res.dirty;
      if (res.errors && res.errors.length) {
        addErrors(res.errors);
        log("Error: " + res.errors[0].message, "error");
      }
      updateStatus();
      loadSheet();
    }).catch(err => log("Error: " + err.message, "error"));
    draw();
    canvas.focus();
  } else if (e.key === "Escape") {
    fbar.value = S.cells[addr(S.curRow, S.curCol)]?.raw || "";
    canvas.focus();
  }
});

// Live formula parse feedback in status bar
fbar.addEventListener("input", () => {
  clearTimeout(_fbarValidateTimer);
  const text = fbar.value;
  if (!text.startsWith("=") || text.length < 2) return;
  _fbarValidateTimer = setTimeout(() => {
    api("POST", "/validate-formula", { text })
      .then(res => {
        if (res.valid) {
          log("Formula OK", "success");
        } else {
          const pos = res.position !== undefined ? ` at position ${res.position}` : "";
          log("Parse error" + pos, "error");
        }
      })
      .catch(() => {});
  }, 300);
});

// ── Clipboard: Copy (TSV) ──
async function doCopy() {
  const sr_ = selRect();
  const lines = [];
  for (let r = sr_.r0; r <= sr_.r1; r++) {
    const cols = [];
    for (let c = sr_.c0; c <= sr_.c1; c++) {
      const a = addr(r, c);
      const cell = S.cells[a];
      cols.push(cell ? cell.raw : "");
    }
    lines.push(cols.join("\t"));
  }
  const tsv = lines.join("\n");

  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(tsv);
      const count = (sr_.r1 - sr_.r0 + 1) * (sr_.c1 - sr_.c0 + 1);
      log("Copied " + count + " cell(s)", "success");
      return;
    } catch (_) {}
  }
  const ta = document.createElement("textarea");
  ta.value = tsv;
  ta.style.position = "fixed";
  ta.style.left = "-9999px";
  document.body.appendChild(ta);
  ta.select();
  try {
    document.execCommand("copy");
    const count = (sr_.r1 - sr_.r0 + 1) * (sr_.c1 - sr_.c0 + 1);
    log("Copied " + count + " cell(s)", "success");
  } catch (_) {
    log("Copy failed", "error");
  }
  document.body.removeChild(ta);
}

// ── Clipboard: Paste (TSV) ──
async function doPaste() {
  let text;
  if (navigator.clipboard && navigator.clipboard.readText) {
    try {
      text = await navigator.clipboard.readText();
    } catch (_) {
      text = prompt("Paste clipboard content:");
    }
  } else {
    text = prompt("Paste clipboard content:");
  }
  if (!text) return;

  const lines = text.split(/\r?\n/);
  if (lines.length > 1 && lines[lines.length - 1] === "") lines.pop();

  const grid = lines.map(line => line.split("\t"));
  const startRow = S.curRow;
  const startCol = S.curCol;

  const edits = [];
  let clipped = false;

  for (let ri = 0; ri < grid.length; ri++) {
    const r = startRow + ri;
    if (r >= S.nRows) { clipped = true; break; }
    for (let ci = 0; ci < grid[ri].length; ci++) {
      const c = startCol + ci;
      if (c >= S.nCols) { clipped = true; continue; }
      const raw = grid[ri][ci];
      const a = addr(r, c);

      if (raw === "") {
        delete S.cells[a];
      } else {
        S.cells[a] = { raw, display: raw };
      }
      clearError(a);

      edits.push({
        addr: a,
        value: raw.startsWith("=") ? undefined : raw,
        formula: raw.startsWith("=") ? raw : undefined,
      });
    }
  }

  if (edits.length) {
    try {
      const res = await api("POST", "/sheet/cells", { sheet: S.activeSheet, edits });
      S.dirty = res.dirty;
      if (res.errors && res.errors.length) {
        addErrors(res.errors);
      }
      updateStatus();
      const msg = "Pasted " + edits.length + " cell(s)";
      log(clipped ? msg + " (clipped to sheet size)" : msg, "success");
    } catch (err) { log("Paste error: " + err.message, "error"); }
  }
  draw();
}

// ── Panel toggle ──
function togglePanel() {
  S.panelOpen = !S.panelOpen;
  const mainEl = document.getElementById("main");
  if (S.panelOpen) {
    sidePanel.classList.remove("collapsed");
    mainEl.classList.remove("panel-collapsed");
  } else {
    sidePanel.classList.add("collapsed");
    mainEl.classList.add("panel-collapsed");
  }
  setTimeout(() => resizeCanvas(), 170);
}

// Rail label click opens panel
const panelRail = document.getElementById("panel-rail");
if (panelRail) {
  panelRail.addEventListener("click", () => {
    if (!S.panelOpen) togglePanel();
  });
}

// ── Keyboard overlay ──
function toggleKbOverlay() {
  S.kbOverlay = !S.kbOverlay;
  if (S.kbOverlay) {
    kbOverlay.classList.add("visible");
  } else {
    kbOverlay.classList.remove("visible");
  }
}

// ── Sheet management ──
function renderSheetTabs() {
  sheetTabList.innerHTML = "";
  for (const name of S.sheets) {
    const tab = document.createElement("div");
    tab.className = "sheet-tab" + (name === S.activeSheet ? " active" : "");
    tab.textContent = name;
    tab.addEventListener("click", () => switchSheet(name));
    tab.addEventListener("dblclick", () => renameSheetPrompt(name));
    tab.addEventListener("contextmenu", e => {
      e.preventDefault();
      if (S.sheets.length > 1 && confirm("Delete sheet '" + name + "'?")) {
        deleteSheet(name);
      }
    });
    sheetTabList.appendChild(tab);
  }
}

async function switchSheet(name) {
  if (name === S.activeSheet) return;
  if (S.editing) commitEdit();
  saveSheetState();
  S.activeSheet = name;
  restoreSheetState(name);
  // If no cached state, load from server
  if (!S.sheetState[name]) {
    await loadSheet(name);
  }
  renderSheetTabs();
  updateCellInfo();
  draw();
}

function switchSheetByOffset(offset) {
  const idx = S.sheets.indexOf(S.activeSheet);
  if (idx < 0) return;
  const newIdx = Math.max(0, Math.min(S.sheets.length - 1, idx + offset));
  if (newIdx !== idx) {
    switchSheet(S.sheets[newIdx]);
  }
}

async function addSheet() {
  let name = prompt("New sheet name:");
  if (!name) return;
  name = name.trim();
  if (!name) return;
  try {
    await api("POST", "/sheets", { name });
    S.sheets.push(name);
    S.dirty = true;
    renderSheetTabs();
    switchSheet(name);
    updateStatus();
    log("Added sheet " + name, "success");
  } catch (err) { log("Error: " + err.message, "error"); }
}

async function deleteSheet(name) {
  try {
    const res = await api("DELETE", "/sheets", { name });
    S.sheets = res.remaining;
    delete S.sheetState[name];
    S.dirty = true;
    if (S.activeSheet === name) {
      S.activeSheet = S.sheets[0];
      restoreSheetState(S.activeSheet);
      if (!S.sheetState[S.activeSheet]) {
        await loadSheet(S.activeSheet);
      }
    }
    renderSheetTabs();
    updateCellInfo();
    draw();
    updateStatus();
    log("Deleted sheet " + name, "success");
  } catch (err) { log("Error: " + err.message, "error"); }
}

async function renameSheetPrompt(oldName) {
  const newName = prompt("Rename sheet:", oldName);
  if (!newName || newName.trim() === oldName) return;
  const trimmed = newName.trim();
  try {
    await api("PATCH", "/sheets", { old_name: oldName, new_name: trimmed });
    const idx = S.sheets.indexOf(oldName);
    if (idx >= 0) S.sheets[idx] = trimmed;
    // Update state key
    if (S.sheetState[oldName]) {
      S.sheetState[trimmed] = S.sheetState[oldName];
      delete S.sheetState[oldName];
    }
    if (S.activeSheet === oldName) S.activeSheet = trimmed;
    S.dirty = true;
    renderSheetTabs();
    updateStatus();
    log("Renamed to " + trimmed, "success");
  } catch (err) { log("Error: " + err.message, "error"); }
}

document.getElementById("btn-add-sheet").addEventListener("click", addSheet);

// ── Color formatting ──
async function setColor(color) {
  const sr_ = selRect();
  const updates = [];
  for (let r = sr_.r0; r <= sr_.r1; r++) {
    for (let c = sr_.c0; c <= sr_.c1; c++) {
      const a = addr(r, c);
      updates.push({ addr: a, color });
      if (color) {
        S.fmt[a] = { color };
      } else {
        delete S.fmt[a];
      }
    }
  }
  if (updates.length) {
    try {
      const res = await api("POST", "/sheet/format", { sheet: S.activeSheet, updates });
      S.dirty = res.dirty;
      updateStatus();
      log(color ? "Color set" : "Color cleared", "success");
    } catch (err) { log("Format error: " + err.message, "error"); }
  }
  draw();
}

// ── Keyboard handling ──
canvas.tabIndex = 0;

canvas.addEventListener("keydown", e => {
  if (S.editing) return;

  const shift = e.shiftKey;
  const ctrl = e.ctrlKey || e.metaKey;

  switch (e.key) {
    case "ArrowUp":    e.preventDefault(); moveCursor(-1, 0, shift); break;
    case "ArrowDown":  e.preventDefault(); moveCursor(1, 0, shift); break;
    case "ArrowLeft":  e.preventDefault(); moveCursor(0, -1, shift); break;
    case "ArrowRight": e.preventDefault(); moveCursor(0, 1, shift); break;
    case "Tab":
      e.preventDefault();
      moveCursor(0, shift ? -1 : 1, false);
      break;
    case "Enter":
      e.preventDefault();
      startEdit();
      break;
    case "Escape":
      break;
    case "Delete":
    case "Backspace":
      e.preventDefault();
      {
        const a = addr(S.curRow, S.curCol);
        delete S.cells[a];
        clearError(a);
        api("POST", "/sheet/cells", {
          sheet: S.activeSheet,
          edits: [{ addr: a, value: "" }]
        }).then(res => { S.dirty = res.dirty; updateStatus(); })
          .catch(err => log("Error: " + err.message, "error"));
        fbar.value = "";
        draw();
      }
      break;
    case "Home":
      e.preventDefault();
      S.curCol = 0; if (!shift) S.selCol = 0;
      if (ctrl) { S.curRow = 0; if (!shift) S.selRow = 0; }
      ensureVisible(S.curRow, S.curCol);
      updateCellInfo(); draw();
      break;
    case "End":
      e.preventDefault();
      S.curCol = S.nCols - 1; if (!shift) S.selCol = S.nCols - 1;
      ensureVisible(S.curRow, S.curCol);
      updateCellInfo(); draw();
      break;
    case "PageDown":
      e.preventDefault();
      if (ctrl) { switchSheetByOffset(1); break; }
      moveCursor(visibleRows() - 2, 0, shift);
      break;
    case "PageUp":
      e.preventDefault();
      if (ctrl) { switchSheetByOffset(-1); break; }
      moveCursor(-(visibleRows() - 2), 0, shift);
      break;
    default:
      // Ctrl+S = Save
      if (ctrl && e.key === "s") {
        e.preventDefault(); doSave(); break;
      }
      // Ctrl+Enter = Run
      if (ctrl && e.key === "Enter") {
        e.preventDefault(); doRun(); break;
      }
      // Ctrl+B = Toggle panel
      if (ctrl && e.key === "b") {
        e.preventDefault(); togglePanel(); break;
      }
      // Ctrl+C = Copy
      if (ctrl && e.key === "c") {
        e.preventDefault(); doCopy(); break;
      }
      // Ctrl+V = Paste
      if (ctrl && e.key === "v") {
        e.preventDefault(); doPaste(); break;
      }
      // Ctrl+1 = Color blue
      if (ctrl && e.key === "1") {
        e.preventDefault(); setColor("#4f7cff"); break;
      }
      // Ctrl+2 = Color red
      if (ctrl && e.key === "2") {
        e.preventDefault(); setColor("#ff5c5c"); break;
      }
      // Ctrl+0 = Clear color
      if (ctrl && e.key === "0") {
        e.preventDefault(); setColor(null); break;
      }
      // Ctrl+P = Toggle precedent highlight
      if (ctrl && e.key === "p") {
        e.preventDefault();
        S.showPrecedents = !S.showPrecedents;
        localStorage.setItem("fin123_showPrecedents", S.showPrecedents ? "1" : "0");
        log("Precedent highlight " + (S.showPrecedents ? "on" : "off"));
        draw();
        break;
      }
      // Ctrl+Shift+= = Insert row above cursor
      if (ctrl && shift && (e.key === "=" || e.key === "+")) {
        e.preventDefault();
        (async () => {
          try {
            const res = await api("POST", "/sheet/rows/insert", { sheet: S.activeSheet, row_idx: S.curRow, count: 1 });
            S.nRows = res.n_rows; S.nCols = res.n_cols; S.dirty = res.dirty;
            updateStatus(); await loadSheet(); draw();
            log("Inserted row", "success");
          } catch (err) { log("Error: " + err.message, "error"); }
        })();
        break;
      }
      // Ctrl+- = Delete current row
      if (ctrl && !shift && e.key === "-") {
        e.preventDefault();
        (async () => {
          try {
            const res = await api("POST", "/sheet/rows/delete", { sheet: S.activeSheet, row_idx: S.curRow, count: 1 });
            S.nRows = res.n_rows; S.nCols = res.n_cols; S.dirty = res.dirty;
            if (S.curRow >= S.nRows) S.curRow = S.nRows - 1;
            updateStatus(); await loadSheet(); draw();
            log("Deleted row", "success");
          } catch (err) { log("Error: " + err.message, "error"); }
        })();
        break;
      }
      // E = Toggle errors panel (when not editing)
      if (e.key === "e" && !ctrl && !e.altKey) {
        e.preventDefault(); showErrorsPanel(); break;
      }
      // ? = Keyboard help
      if (e.key === "?" && !ctrl && !e.altKey) {
        e.preventDefault(); toggleKbOverlay(); break;
      }
      // Type-to-edit (single printable char, not special keys)
      if (e.key.length === 1 && !ctrl && !e.altKey) {
        e.preventDefault();
        startEdit(e.key);
      }
  }
});

// In-cell editor keys
editor.addEventListener("keydown", e => {
  if (e.key === "Enter") { e.preventDefault(); commitEdit(); moveCursor(1, 0, false); }
  else if (e.key === "Escape") { cancelEdit(); }
  else if (e.key === "Tab") { e.preventDefault(); commitEdit(); moveCursor(0, e.shiftKey ? -1 : 1, false); }
});

// ── Mouse handling ──
canvas.addEventListener("mousedown", e => {
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;

  if (mx < HDR_W || my < HDR_H) return;

  const c = Math.floor((mx - HDR_W) / COL_W) + S.scrollCol;
  const r = Math.floor((my - HDR_H) / ROW_H) + S.scrollRow;
  if (r >= S.nRows || c >= S.nCols) return;

  if (S.editing) commitEdit();
  S.curRow = r; S.curCol = c;
  if (!e.shiftKey) { S.selRow = r; S.selCol = c; }
  updateCellInfo();
  draw();
  canvas.focus();
});

canvas.addEventListener("dblclick", e => {
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  if (mx < HDR_W || my < HDR_H) return;
  startEdit();
});

// Hover tracking
canvas.addEventListener("mousemove", e => {
  const rect = canvas.getBoundingClientRect();
  const my = e.clientY - rect.top;
  if (my < HDR_H) { S.hoverRow = -1; draw(); return; }
  const r = Math.floor((my - HDR_H) / ROW_H) + S.scrollRow;
  if (r !== S.hoverRow) {
    S.hoverRow = r;
    draw();
  }
});

canvas.addEventListener("mouseleave", () => {
  if (S.hoverRow >= 0) {
    S.hoverRow = -1;
    draw();
  }
});

// Scroll with wheel
canvas.addEventListener("wheel", e => {
  e.preventDefault();
  if (e.deltaY) {
    S.scrollRow = Math.max(0, Math.min(S.nRows - 1, S.scrollRow + Math.sign(e.deltaY) * 3));
  }
  if (e.deltaX) {
    S.scrollCol = Math.max(0, Math.min(S.nCols - 1, S.scrollCol + Math.sign(e.deltaX) * 3));
  }
  draw();
}, { passive: false });

// ── Context menu (row/col insert & delete) ──
const ctxMenu = document.getElementById("grid-context-menu");

function hideContextMenu() {
  if (ctxMenu) ctxMenu.classList.remove("visible");
}

canvas.addEventListener("contextmenu", e => {
  e.preventDefault();
  if (!ctxMenu) return;
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  if (mx < HDR_W || my < HDR_H) return;

  const c = Math.floor((mx - HDR_W) / COL_W) + S.scrollCol;
  const r = Math.floor((my - HDR_H) / ROW_H) + S.scrollRow;
  if (r >= S.nRows || c >= S.nCols) return;

  // Move cursor to right-clicked cell
  S.curRow = r; S.curCol = c;
  S.selRow = r; S.selCol = c;
  updateCellInfo();
  draw();

  // Position menu
  ctxMenu.style.left = Math.min(mx, canvas.width - 170) + "px";
  ctxMenu.style.top = Math.min(my, canvas.height - 120) + "px";
  ctxMenu.classList.add("visible");
});

document.addEventListener("click", hideContextMenu);
document.addEventListener("keydown", e => { if (e.key === "Escape") hideContextMenu(); });

if (ctxMenu) {
  ctxMenu.querySelectorAll(".ctx-item").forEach(item => {
    item.addEventListener("click", async () => {
      hideContextMenu();
      const action = item.dataset.action;
      try {
        let res;
        if (action === "insert-row") {
          res = await api("POST", "/sheet/rows/insert", { sheet: S.activeSheet, row_idx: S.curRow, count: 1 });
        } else if (action === "insert-col") {
          res = await api("POST", "/sheet/cols/insert", { sheet: S.activeSheet, col_idx: S.curCol, count: 1 });
        } else if (action === "delete-row") {
          res = await api("POST", "/sheet/rows/delete", { sheet: S.activeSheet, row_idx: S.curRow, count: 1 });
        } else if (action === "delete-col") {
          res = await api("POST", "/sheet/cols/delete", { sheet: S.activeSheet, col_idx: S.curCol, count: 1 });
        }
        if (res) {
          S.nRows = res.n_rows;
          S.nCols = res.n_cols;
          S.dirty = res.dirty;
          updateStatus();
          await loadSheet();
          draw();
          log(action.replace("-", " ") + " done", "success");
        }
      } catch (err) { log("Error: " + err.message, "error"); }
    });
  });
}

// ── Actions ──
async function doSave() {
  try {
    log("Committing...");
    saveSheetState();
    const res = await api("POST", "/commit");
    S.dirty = false;
    S.snapVer = res.snapshot_version;
    updateStatus();
    log("Committed " + res.snapshot_version, "success");
    loadSnapshots();
    loadStatus();
  } catch (err) { log("Commit error: " + err.message, "error"); }
}

async function doRun() {
  if (S.dirty) {
    log("Commit before building (uncommitted edits)", "error");
    return;
  }
  try {
    log("Building...");
    const res = await api("POST", "/build");
    S.lastRunId = res.run_id;
    updateStatus();
    log("Build complete: " + res.run_id, "success");
    loadScalars();
    loadRuns();
    loadChecks(res.run_id);
    loadIncidents(res.run_id);
    loadLogs();
    loadStatus();
  } catch (err) { log("Build error: " + err.message, "error"); }
}

async function doSync(tableName) {
  try {
    log("Syncing" + (tableName ? " " + tableName : "") + "...");
    const body = tableName ? { table_name: tableName } : {};
    const res = await api("POST", "/sync", body);
    const parts = [];
    if (res.synced.length) parts.push("synced: " + res.synced.join(", "));
    if (res.skipped.length) parts.push("skipped: " + res.skipped.join(", "));
    if (res.errors.length) parts.push("errors: " + res.errors.join(", "));
    log(parts.join("; ") || "No SQL tables", parts.some(p => p.startsWith("errors")) ? "error" : "success");
    loadDatasheets();
  } catch (err) { log("Sync error: " + err.message, "error"); }
}

async function doWorkflow() {
  const name = S.workflows[0];
  if (!name) { log("No workflows available", "error"); return; }
  try {
    log("Running workflow " + name + "...");
    const res = await api("POST", "/workflow/run", { workflow_name: name });
    log("Workflow done: " + res.artifact_name + " " + res.artifact_version, "success");
  } catch (err) { log("Workflow error: " + err.message, "error"); }
}

async function doFill() {
  const sr_ = selRect();
  if (sr_.r0 === sr_.r1 && sr_.c0 === sr_.c1) { log("Select a range first", "error"); return; }
  const src = S.cells[addr(S.curRow, S.curCol)];
  if (!src) { log("Active cell is empty", "error"); return; }
  const edits = [];
  for (let r = sr_.r0; r <= sr_.r1; r++) {
    for (let c = sr_.c0; c <= sr_.c1; c++) {
      if (r === S.curRow && c === S.curCol) continue;
      const a = addr(r, c);
      const raw = src.raw;
      S.cells[a] = { raw, display: raw };
      edits.push({
        addr: a,
        value: raw.startsWith("=") ? undefined : raw,
        formula: raw.startsWith("=") ? raw : undefined
      });
    }
  }
  if (edits.length) {
    try {
      const res = await api("POST", "/sheet/cells", { sheet: S.activeSheet, edits });
      S.dirty = res.dirty;
      if (res.errors && res.errors.length) addErrors(res.errors);
      updateStatus();
      log("Filled " + edits.length + " cell(s)", "success");
    } catch (err) { log("Fill error: " + err.message, "error"); }
  }
  draw();
}

// Menu bar buttons
document.getElementById("btn-save").addEventListener("click", doSave);
document.getElementById("btn-sync").addEventListener("click", () => doSync());
document.getElementById("btn-workflow").addEventListener("click", doWorkflow);
document.getElementById("btn-panel").addEventListener("click", togglePanel);
document.getElementById("btn-run-main").addEventListener("click", doRun);

// Side panel buttons
document.getElementById("btn-side-save").addEventListener("click", doSave);
document.getElementById("btn-side-run").addEventListener("click", doRun);
document.getElementById("btn-side-sync").addEventListener("click", () => doSync());
document.getElementById("btn-side-workflow").addEventListener("click", doWorkflow);
document.getElementById("btn-fill").addEventListener("click", doFill);

// ── Tabs ──
document.querySelectorAll(".tab-bar .tab").forEach(tab => {
  tab.addEventListener("click", () => {
    tab.parentElement.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    tab.classList.add("active");
    const section = tab.closest(".panel-section");
    section.querySelectorAll(".tab-content").forEach(tc => tc.classList.remove("active"));
    section.querySelector("#tab-" + tab.dataset.tab).classList.add("active");
    // Scroll clicked tab into view within the tab bar
    tab.scrollIntoView({ inline: "nearest", block: "nearest", behavior: "smooth" });
    // Start/stop log polling based on active tab
    if (tab.dataset.tab === "logs") {
      loadLogs();
      startLogPolling();
    } else {
      stopLogPolling();
    }
    // Refresh incidents when switching to checks/incidents tab
    if (tab.dataset.tab === "checks") {
      loadIncidents();
    }
    // Load worksheet specs when switching to worksheet tab
    if (tab.dataset.tab === "worksheet") {
      loadWorksheetSpecs();
    }
  });
});

// Horizontal scroll on tab bar via mouse wheel / trackpad
document.querySelectorAll(".tab-bar").forEach(bar => {
  bar.addEventListener("wheel", e => {
    if (bar.scrollWidth > bar.clientWidth) {
      e.preventDefault();
      bar.scrollLeft += (e.deltaY || e.deltaX);
    }
  }, { passive: false });
});

// ── Data loading ──
async function loadProject() {
  try {
    const info = await api("GET", "/project");
    S.nRows = 200; S.nCols = 40;
    S.dirty = info.dirty;
    S.snapVer = info.snapshot_version;
    S.lastRunId = info.last_run_id;
    S.outputTables = info.output_tables || [];
    S.workflows = info.workflows || [];
    S.sheets = info.sheets || ["Sheet1"];
    S.activeSheet = S.sheets[0] || "Sheet1";
    updateStatus();
    renderSheetTabs();
    // Check project mode for prod banner
    if (info.mode) updateProdBanner(info.mode);
  } catch (err) { log("Load error: " + err.message, "error"); }
}

async function loadSheet(sheetName) {
  const name = sheetName || S.activeSheet;
  try {
    const data = await api("GET", "/sheet?sheet=" + encodeURIComponent(name) + "&r0=0&c0=0&rows=" + S.nRows + "&cols=" + S.nCols);
    S.nRows = data.n_rows;
    S.nCols = data.n_cols;
    S.cells = {};
    S.fmt = {};
    for (const c of data.cells) {
      if (c.raw || c.display) {
        S.cells[c.addr] = { raw: c.raw, display: c.display };
      }
      if (c.fmt) {
        S.fmt[c.addr] = c.fmt;
      }
    }
    draw();
  } catch (err) { log("Sheet load error: " + err.message, "error"); }
}

async function loadScalars() {
  try {
    const data = await api("GET", "/outputs/scalars");
    const el = document.getElementById("scalar-list");
    el.innerHTML = "";
    const scalars = data.scalars || {};
    for (const [k, v] of Object.entries(scalars)) {
      const item = document.createElement("div");
      item.className = "item";
      item.innerHTML = `<span class="label">${esc(k)}</span><span class="val">${esc(String(v))}</span>`;
      el.appendChild(item);
    }
  } catch (_) {}
}

async function loadTableList() {
  const el = document.getElementById("table-list");
  el.innerHTML = "";
  for (const name of S.outputTables) {
    const btn = document.createElement("button");
    btn.className = "btn";
    btn.textContent = name;
    btn.addEventListener("click", () => loadTablePreview(name));
    el.appendChild(btn);
  }
}

// ── View sort/filter state ──
const _viewState = {
  tableName: null,
  sorts: [],       // [{column, descending}]
  filters: [],     // [{type, column, ...}]
  columns: [],
  rows: [],
  totalRows: 0,
};

async function loadTablePreview(name) {
  _viewState.tableName = name;
  _viewState.sorts = [];
  _viewState.filters = [];
  await _fetchTableView();
}

async function _fetchTableView() {
  const name = _viewState.tableName;
  if (!name) return;
  try {
    const hasTransforms = _viewState.sorts.length > 0 || _viewState.filters.length > 0;
    let data;
    if (hasTransforms) {
      data = await api("POST", "/outputs/table/view", {
        name, limit: 5000,
        sorts: _viewState.sorts,
        filters: _viewState.filters,
      });
    } else {
      data = await api("GET", "/outputs/table?name=" + encodeURIComponent(name) + "&limit=5000");
    }
    if (!data.columns || !data.rows) {
      document.getElementById("table-preview").innerHTML = "<p>No data</p>";
      return;
    }
    _viewState.columns = data.columns;
    _viewState.rows = data.rows;
    _viewState.totalRows = data.total_rows;
    _renderTableView();
  } catch (err) { log("Table preview error: " + err.message, "error"); }
}

function _renderTableView() {
  const el = document.getElementById("table-preview");
  const { columns, rows, totalRows, sorts, filters } = _viewState;

  // Build sort/filter lookup
  const sortMap = {};
  sorts.forEach((s, i) => { sortMap[s.column] = { desc: s.descending, idx: i }; });
  const filterCols = new Set(filters.map(f => f.column));

  // Toolbar chips
  let toolbar = "";
  if (sorts.length > 0 || filters.length > 0) {
    toolbar = '<div class="view-toolbar">';
    for (const s of sorts) {
      toolbar += `<span class="view-chip view-chip-sort" data-col="${esc(s.column)}">${esc(s.column)} ${s.descending ? "\u2193" : "\u2191"} <span class="view-chip-x" data-action="clear-sort" data-col="${esc(s.column)}">\u00d7</span></span>`;
    }
    for (const f of filters) {
      let label = f.column + ": ";
      if (f.type === "numeric") label += f.op + " " + f.value;
      else if (f.type === "between") label += f.low + "\u2013" + f.high;
      else if (f.type === "text") label += f.op + " " + f.value;
      else if (f.type === "value_list") label += f.values.length + " values";
      else if (f.type === "blanks") label += f.show_blanks ? "blanks" : "non-blanks";
      toolbar += `<span class="view-chip view-chip-filter" data-col="${esc(f.column)}">${esc(label)} <span class="view-chip-x" data-action="clear-filter" data-col="${esc(f.column)}">\u00d7</span></span>`;
    }
    toolbar += '<span class="view-chip view-chip-clear" data-action="clear-all">Clear All</span>';
    toolbar += "</div>";
  }

  // Table
  let html = toolbar + "<table><thead><tr>";
  for (const col of columns) {
    const srt = sortMap[col];
    const hasFilter = filterCols.has(col);
    let cls = "tv-th";
    if (srt) cls += " tv-th-sorted";
    if (hasFilter) cls += " tv-th-filtered";
    const arrow = srt ? (srt.desc ? " \u25bc" : " \u25b2") : "";
    const badge = hasFilter ? '<span class="tv-filter-badge">F</span>' : "";
    html += `<th class="${cls}" data-col="${esc(col)}">${esc(col)}${arrow}${badge}</th>`;
  }
  html += "</tr></thead><tbody>";
  const displayRows = rows.slice(0, 200);
  for (const row of displayRows) {
    html += "<tr>";
    for (const col of columns) html += "<td>" + esc(String(row[col] ?? "")) + "</td>";
    html += "</tr>";
  }
  html += "</tbody></table>";
  const showing = Math.min(displayRows.length, rows.length);
  html += `<p style="color:var(--fg-dim);font-size:10px;">${totalRows} total rows (showing ${showing})</p>`;
  el.innerHTML = html;

  // Attach header click/right-click handlers
  el.querySelectorAll("th.tv-th").forEach(th => {
    th.addEventListener("click", () => _onSortClick(th.dataset.col));
    th.addEventListener("contextmenu", (e) => { e.preventDefault(); _openFilterPanel(th.dataset.col, e); });
  });

  // Toolbar chip handlers
  el.querySelectorAll(".view-chip-x").forEach(x => {
    x.addEventListener("click", (e) => {
      e.stopPropagation();
      const action = x.dataset.action;
      const col = x.dataset.col;
      if (action === "clear-sort") { _viewState.sorts = _viewState.sorts.filter(s => s.column !== col); _fetchTableView(); }
      if (action === "clear-filter") { _viewState.filters = _viewState.filters.filter(f => f.column !== col); _fetchTableView(); }
    });
  });
  const clearAll = el.querySelector('[data-action="clear-all"]');
  if (clearAll) clearAll.addEventListener("click", () => { _viewState.sorts = []; _viewState.filters = []; _fetchTableView(); });
}

function _onSortClick(col) {
  const existing = _viewState.sorts.findIndex(s => s.column === col);
  if (existing === -1) {
    // Add ascending sort
    _viewState.sorts.push({ column: col, descending: false });
  } else if (!_viewState.sorts[existing].descending) {
    // Switch to descending
    _viewState.sorts[existing].descending = true;
  } else {
    // Remove sort
    _viewState.sorts.splice(existing, 1);
  }
  _fetchTableView();
}

function _openFilterPanel(col, event) {
  // Close any existing panel
  _closeFilterPanel();

  const panel = document.createElement("div");
  panel.className = "view-filter-panel";
  panel.id = "view-filter-panel";

  // Detect column type from data
  const vals = _viewState.rows.map(r => r[col]);
  const nonNull = vals.filter(v => v !== null && v !== undefined && v !== "");
  const isNumeric = nonNull.length > 0 && nonNull.every(v => typeof v === "number" || (typeof v === "string" && !isNaN(Number(v))));
  const uniqueVals = [...new Set(vals.map(v => v === null || v === undefined ? null : v))];
  const hasValues = uniqueVals.length <= 50;

  let html = `<div class="vfp-header">${esc(col)}</div>`;

  if (isNumeric) {
    html += `<div class="vfp-section">
      <label class="vfp-label">Numeric filter</label>
      <div class="vfp-row">
        <select id="vfp-num-op" class="vfp-select">
          <option value=">">&gt;</option>
          <option value=">=">&ge;</option>
          <option value="<">&lt;</option>
          <option value="<=">&le;</option>
          <option value="=">=</option>
          <option value="<>">&ne;</option>
        </select>
        <input id="vfp-num-val" type="number" class="vfp-input" placeholder="value" />
        <button class="btn vfp-apply" data-filter="numeric">Apply</button>
      </div>
    </div>`;
  }

  if (!isNumeric) {
    html += `<div class="vfp-section">
      <label class="vfp-label">Text filter</label>
      <div class="vfp-row">
        <select id="vfp-text-op" class="vfp-select">
          <option value="contains">Contains</option>
          <option value="starts_with">Starts with</option>
          <option value="ends_with">Ends with</option>
          <option value="equals">Equals</option>
        </select>
        <input id="vfp-text-val" type="text" class="vfp-input" placeholder="text" />
        <button class="btn vfp-apply" data-filter="text">Apply</button>
      </div>
    </div>`;
  }

  if (hasValues) {
    html += `<div class="vfp-section">
      <label class="vfp-label">Values (${uniqueVals.length})</label>
      <div class="vfp-values" id="vfp-values">`;
    for (const v of uniqueVals.sort()) {
      const display = v === null ? "(blank)" : String(v);
      html += `<label class="vfp-val-label"><input type="checkbox" value="${esc(String(v ?? "__null__"))}" checked /> ${esc(display)}</label>`;
    }
    html += `</div>
      <button class="btn vfp-apply" data-filter="value_list">Apply selected</button>
    </div>`;
  }

  html += `<div class="vfp-section">
    <label class="vfp-label">Blanks</label>
    <div class="vfp-row">
      <button class="btn vfp-apply" data-filter="blanks-only">Only blanks</button>
      <button class="btn vfp-apply" data-filter="non-blanks">Non-blanks</button>
    </div>
  </div>`;

  panel.innerHTML = html;

  // Position near the click
  panel.style.left = Math.min(event.clientX, window.innerWidth - 260) + "px";
  panel.style.top = Math.min(event.clientY, window.innerHeight - 300) + "px";
  document.body.appendChild(panel);

  // Attach apply handlers
  panel.querySelectorAll(".vfp-apply").forEach(btn => {
    btn.addEventListener("click", () => {
      const filterType = btn.dataset.filter;
      // Remove existing filter on this column
      _viewState.filters = _viewState.filters.filter(f => f.column !== col);

      if (filterType === "numeric") {
        const op = panel.querySelector("#vfp-num-op").value;
        const val = parseFloat(panel.querySelector("#vfp-num-val").value);
        if (!isNaN(val)) _viewState.filters.push({ type: "numeric", column: col, op, value: val });
      } else if (filterType === "text") {
        const op = panel.querySelector("#vfp-text-op").value;
        const val = panel.querySelector("#vfp-text-val").value;
        if (val) _viewState.filters.push({ type: "text", column: col, op, value: val, case_sensitive: false });
      } else if (filterType === "value_list") {
        const checks = panel.querySelectorAll("#vfp-values input:checked");
        const selected = [...checks].map(c => c.value === "__null__" ? null : (isNumeric ? Number(c.value) : c.value));
        if (selected.length < uniqueVals.length) _viewState.filters.push({ type: "value_list", column: col, values: selected });
      } else if (filterType === "blanks-only") {
        _viewState.filters.push({ type: "blanks", column: col, show_blanks: true });
      } else if (filterType === "non-blanks") {
        _viewState.filters.push({ type: "blanks", column: col, show_blanks: false });
      }

      _closeFilterPanel();
      _fetchTableView();
    });
  });

  // Close on outside click
  setTimeout(() => {
    document.addEventListener("click", _closeFilterPanelOutside);
  }, 0);
}

function _closeFilterPanel() {
  const p = document.getElementById("view-filter-panel");
  if (p) p.remove();
  document.removeEventListener("click", _closeFilterPanelOutside);
}

function _closeFilterPanelOutside(e) {
  const p = document.getElementById("view-filter-panel");
  if (p && !p.contains(e.target)) _closeFilterPanel();
}

async function loadRuns() {
  try {
    const runs = await api("GET", "/runs?limit=20");
    const el = document.getElementById("run-list");
    el.innerHTML = "";
    for (const run of runs) {
      const item = document.createElement("div");
      item.className = "item";
      item.innerHTML = `<span class="label">${esc(run.run_id)}</span><span class="val">${esc(run.timestamp?.substring(0,19) || "")}</span>`;
      el.appendChild(item);
    }
  } catch (_) {}
}

async function loadSnapshots() {
  try {
    const snaps = await api("GET", "/snapshots?limit=20");
    const el = document.getElementById("snap-list");
    el.innerHTML = "";
    for (const s of snaps) {
      const item = document.createElement("div");
      item.className = "item";
      item.innerHTML = `<span class="label">${esc(s.version)}</span>`;
      el.appendChild(item);
    }
  } catch (_) {}
}

async function loadDatasheets() {
  try {
    const sheets = await api("GET", "/datasheets");
    const el = document.getElementById("datasheet-list");
    if (!el) return;
    el.innerHTML = "";
    if (sheets.length === 0) {
      el.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No SQL datasheets</div>';
      return;
    }
    for (const ds of sheets) {
      const item = document.createElement("div");
      item.className = "ds-item";

      const dotCls = {fresh: "dot-ok", stale: "dot-stale", fail: "dot-fail", unknown: "dot-unknown"}[ds.staleness] || "dot-unknown";
      const dot = `<span class="dot ${dotCls}"></span>`;

      let info = ds.staleness;
      if (ds.last_rowcount !== null) info += " \u00B7 " + ds.last_rowcount + " rows";
      if (ds.last_sync_time) info += " \u00B7 " + ds.last_sync_time.substring(0, 16);

      item.innerHTML = `${dot}<span class="ds-name">${esc(ds.table_name)}</span><span class="ds-info">${esc(info)}</span><button class="ds-sync-btn">sync</button>`;
      item.querySelector(".ds-sync-btn").addEventListener("click", () => doSync(ds.table_name));
      el.appendChild(item);
    }
  } catch (_) {}
}

// ── Named ranges ──
async function loadNames() {
  try {
    const names = await api("GET", "/names");
    const el = document.getElementById("names-list");
    if (!el) return;
    el.innerHTML = "";
    const entries = Object.entries(names);
    if (entries.length === 0) {
      el.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No named ranges</div>';
      return;
    }
    for (const [name, defn] of entries) {
      const item = document.createElement("div");
      item.className = "name-item";
      item.innerHTML = `<span class="name-id">${esc(name)}</span><span class="name-range">${esc(defn.sheet)}!${esc(defn.start)}:${esc(defn.end)}</span><button class="name-del" title="Delete">\u00d7</button>`;
      item.querySelector(".name-del").addEventListener("click", () => deleteName(name));
      el.appendChild(item);
    }
  } catch (_) {}
}

async function addName() {
  const name = prompt("Name (identifier):");
  if (!name || !name.trim()) return;
  const sheet = prompt("Sheet name:", S.activeSheet);
  if (!sheet) return;
  const start = prompt("Start address (e.g. A1):");
  if (!start) return;
  const end = prompt("End address (e.g. A10):");
  if (!end) return;
  try {
    await api("POST", "/names", { name: name.trim(), sheet, start, end });
    S.dirty = true;
    updateStatus();
    loadNames();
    log("Named range '" + name.trim() + "' created", "success");
  } catch (err) { log("Error: " + err.message, "error"); }
}

async function deleteName(name) {
  if (!confirm("Delete named range '" + name + "'?")) return;
  try {
    await api("DELETE", "/names/" + encodeURIComponent(name));
    S.dirty = true;
    updateStatus();
    loadNames();
    log("Named range '" + name + "' deleted", "success");
  } catch (err) { log("Error: " + err.message, "error"); }
}

const btnAddName = document.getElementById("btn-add-name");
if (btnAddName) btnAddName.addEventListener("click", addName);

// ── Model / Version ──
async function loadModelInfo() {
  try {
    const info = await api("GET", "/model");
    S.readOnly = info.read_only;
    S.snapVer = info.current_model_version_id;
    updateReadOnlyUI();
  } catch (_) {}
}

async function loadModelVersions() {
  try {
    const versions = await api("GET", "/model/versions");
    S.modelVersions = versions;
    const sel = document.getElementById("version-select");
    if (!sel) return;
    sel.innerHTML = "";
    for (const v of versions.slice().reverse()) {
      const opt = document.createElement("option");
      opt.value = v.model_version_id;
      opt.textContent = v.model_version_id + (v.pinned ? " \u{1F4CC}" : "");
      sel.appendChild(opt);
    }
    if (S.snapVer) sel.value = S.snapVer;
  } catch (_) {}
}

async function selectVersion(v) {
  try {
    log("Loading version " + v + "...");
    const info = await api("POST", "/model/select", { version: v });
    S.readOnly = info.read_only;
    S.snapVer = info.current_model_version_id;
    updateReadOnlyUI();
    await loadSheet();
    updateCellInfo();
    draw();
    log("Loaded version " + v, "success");
  } catch (err) { log("Version error: " + err.message, "error"); }
}

function updateReadOnlyUI() {
  const banner = document.getElementById("readonly-banner");
  const verSpan = document.getElementById("readonly-ver");
  if (S.readOnly) {
    banner.style.display = "flex";
    verSpan.textContent = S.snapVer || "";
  } else {
    banner.style.display = "none";
  }
  // Disable mutation buttons when read-only
  const btns = ["btn-save", "btn-run-main", "btn-side-save", "btn-side-run"];
  for (const id of btns) {
    const el = document.getElementById(id);
    if (el) {
      el.style.opacity = S.readOnly ? "0.4" : "1";
      el.style.pointerEvents = S.readOnly ? "none" : "";
    }
  }
}

const versionSelect = document.getElementById("version-select");
if (versionSelect) {
  versionSelect.addEventListener("change", () => selectVersion(versionSelect.value));
}

const btnBackLatest = document.getElementById("btn-back-latest");
if (btnBackLatest) {
  btnBackLatest.addEventListener("click", async () => {
    if (S.modelVersions.length) {
      const latest = S.modelVersions[S.modelVersions.length - 1].model_version_id;
      await selectVersion(latest);
      const sel = document.getElementById("version-select");
      if (sel) sel.value = latest;
    }
  });
}

// ── Clear cache ──
const ccOverlay = document.getElementById("cc-overlay");
async function doClearCache() {
  try {
    log("Analyzing cache...");
    const summary = await api("POST", "/clear-cache", { dry_run: true });
    const el = document.getElementById("cc-summary");
    el.innerHTML = "";
    const stats = [
      ["Runs to delete", summary.runs_deleted],
      ["Artifact versions to delete", summary.artifact_versions_deleted],
      ["Sync runs to delete", summary.sync_runs_deleted],
      ["Model versions to delete", summary.model_versions_deleted],
      ["Hash cache", (summary.hash_cache_bytes || 0).toLocaleString() + " bytes"],
      ["Bytes to free", (summary.bytes_freed || 0).toLocaleString()],
    ];
    for (const [label, val] of stats) {
      const d = document.createElement("div");
      d.className = "cc-stat";
      d.innerHTML = `<span class="label">${esc(label)}</span><span class="val">${esc(String(val))}</span>`;
      el.appendChild(d);
    }
    ccOverlay.classList.add("visible");
  } catch (err) { log("Clear-cache error: " + err.message, "error"); }
}

document.getElementById("btn-clear-cache").addEventListener("click", doClearCache);
document.getElementById("cc-cancel").addEventListener("click", () => ccOverlay.classList.remove("visible"));
document.getElementById("cc-confirm").addEventListener("click", async () => {
  ccOverlay.classList.remove("visible");
  try {
    log("Clearing cache...");
    const summary = await api("POST", "/clear-cache", { dry_run: false });
    log("Cache cleared: " + (summary.bytes_freed || 0).toLocaleString() + " bytes freed", "success");
    loadRuns();
    loadSnapshots();
    loadModelVersions();
  } catch (err) { log("Clear-cache error: " + err.message, "error"); }
});
ccOverlay.addEventListener("click", e => { if (e.target === ccOverlay) ccOverlay.classList.remove("visible"); });

// ── Import XLSX upload ──
const importOverlay = document.getElementById("import-overlay");
const importFileInput = document.getElementById("import-file-input");
const importFileName = document.getElementById("import-file-name");
const importProjectName = document.getElementById("import-project-name");
const importDestPreview = document.getElementById("import-dest-preview");
const importSubmitBtn = document.getElementById("import-submit");

function openImportModal() {
  importFileInput.value = "";
  importFileName.textContent = "No file selected";
  importProjectName.value = "";
  importDestPreview.textContent = "~/Documents/fin123_projects/...";
  importSubmitBtn.disabled = true;
  importOverlay.classList.add("visible");
}

function closeImportModal() {
  importOverlay.classList.remove("visible");
}

document.getElementById("import-browse-btn").addEventListener("click", () => importFileInput.click());
importFileInput.addEventListener("change", () => {
  const file = importFileInput.files[0];
  if (!file) return;
  importFileName.textContent = file.name;
  // Auto-fill project name from stem
  const stem = file.name.replace(/\.xlsx$/i, "");
  const slug = stem.toLowerCase().replace(/\s+/g, "_").replace(/[^a-z0-9_-]/g, "_").replace(/_+/g, "_").replace(/^_|_$/g, "");
  importProjectName.value = slug;
  importDestPreview.textContent = "~/Documents/fin123_projects/" + (slug || "...");
  importSubmitBtn.disabled = false;
});

importProjectName.addEventListener("input", () => {
  const name = importProjectName.value.trim();
  importDestPreview.textContent = "~/Documents/fin123_projects/" + (name || "...");
});

async function doImportUpload() {
  const file = importFileInput.files[0];
  if (!file) return;
  const name = importProjectName.value.trim();

  importSubmitBtn.disabled = true;
  importSubmitBtn.textContent = "Importing...";
  log("Importing " + file.name + "...");

  try {
    const formData = new FormData();
    formData.append("file", file);
    if (name) formData.append("project_name", name);

    const resp = await fetch("/api/import/xlsx", { method: "POST", body: formData });
    const data = await resp.json();
    if (!resp.ok) {
      const msg = data.detail || JSON.stringify(data);
      log("Import error: " + msg, "error");
      return;
    }

    closeImportModal();
    const cells = data.report ? data.report.cells_imported : "?";
    log("Imported " + file.name + " → " + data.project_dir + " (" + cells + " cells)", "success");

    // Switch to Import tab and reload report
    const importTab = document.querySelector('[data-tab="import"]');
    if (importTab) importTab.click();
    await loadImportReport();
  } catch (err) {
    log("Import error: " + err.message, "error");
  } finally {
    importSubmitBtn.textContent = "Import";
    importSubmitBtn.disabled = !importFileInput.files[0];
  }
}

importSubmitBtn.addEventListener("click", doImportUpload);
document.getElementById("btn-import-xlsx").addEventListener("click", openImportModal);
document.getElementById("btn-import-xlsx-side").addEventListener("click", openImportModal);
document.getElementById("import-cancel").addEventListener("click", closeImportModal);
importOverlay.addEventListener("click", e => { if (e.target === importOverlay) closeImportModal(); });

// ── Import report ──
async function loadImportReport() {
  try {
    const report = await api("GET", "/import/report/latest");
    const el = document.getElementById("import-summary");
    if (!el) return;
    el.innerHTML = "";

    if (report.source) {
      const d = document.createElement("div");
      d.className = "import-stat";
      d.innerHTML = `<span class="label">Source</span><span class="val">${esc(report.source)}</span>`;
      el.appendChild(d);
    }

    const statsItems = [
      ["Cells imported", report.cells_imported],
      ["Formulas", report.formulas_imported],
      ["Colors", report.colors_imported],
    ];
    for (const [label, val] of statsItems) {
      if (val !== undefined) {
        const d = document.createElement("div");
        d.className = "import-stat";
        d.innerHTML = `<span class="label">${esc(label)}</span><span class="val">${esc(String(val))}</span>`;
        el.appendChild(d);
      }
    }

    if (report.sheets_imported && report.sheets_imported.length) {
      const tbl = document.createElement("table");
      tbl.style.cssText = "width:100%;border-collapse:collapse;margin-top:6px;font-size:10px;";
      let html = "<tr><th style='text-align:left;color:var(--accent);'>Sheet</th><th>Cells</th><th>Formulas</th></tr>";
      for (const s of report.sheets_imported) {
        html += `<tr><td style="color:var(--fg-muted)">${esc(s.name)}</td><td style="text-align:center">${s.cells}</td><td style="text-align:center">${s.formulas}</td></tr>`;
      }
      tbl.innerHTML = html;
      el.appendChild(tbl);
    }

    if (report.skipped_features && report.skipped_features.length) {
      const d = document.createElement("div");
      d.style.cssText = "margin-top:6px;color:var(--fg-dim);font-size:10px;";
      d.textContent = "Skipped: " + report.skipped_features.join(", ");
      el.appendChild(d);
    }

    // Classification summary (Phase 8)
    const clsSummary = report.classification_summary;
    if (clsSummary && clsSummary.total_formulas > 0) {
      const csDiv = document.createElement("div");
      csDiv.style.cssText = "margin-top:8px;";
      const csTitle = document.createElement("div");
      csTitle.style.cssText = "font-size:10px;color:var(--fg-dim);margin-bottom:4px;text-transform:uppercase;letter-spacing:0.5px;";
      csTitle.textContent = "Formula Classification";
      csDiv.appendChild(csTitle);

      const csItems = [
        ["Supported", clsSummary.supported, "var(--success)"],
        ["Parse errors", clsSummary.parse_errors, "var(--error)"],
        ["Unsupported functions", clsSummary.unsupported_functions, "var(--dirty)"],
        ["External links", clsSummary.external_links, "var(--dirty)"],
        ["Plugin formulas", clsSummary.plugin_formulas, "var(--fg-dim)"],
      ];
      for (const [label, count, color] of csItems) {
        if (count > 0) {
          const row = document.createElement("div");
          row.className = "import-stat";
          row.innerHTML = `<span class="label" style="color:${color}">${esc(label)}</span><span class="val">${count}</span>`;
          csDiv.appendChild(row);
        }
      }
      el.appendChild(csDiv);
    }

    // Top unsupported functions
    if (report.top_unsupported_functions && report.top_unsupported_functions.length) {
      const ufDiv = document.createElement("div");
      ufDiv.style.cssText = "margin-top:6px;font-size:10px;color:var(--fg-dim);";
      ufDiv.textContent = "Top unsupported: " + report.top_unsupported_functions.map(f => f.name + " (" + f.count + ")").join(", ");
      el.appendChild(ufDiv);
    }

    // Issues list with filter + quick actions (Phase 8)
    const classifications = report.formula_classifications;
    if (classifications && classifications.length) {
      const issues = classifications.filter(c => c.classification !== "supported");
      if (issues.length > 0) {
        const issueDiv = document.createElement("div");
        issueDiv.style.cssText = "margin-top:10px;";

        // Filter bar
        const filterBar = document.createElement("div");
        filterBar.className = "import-filter-bar";
        const filters = ["All", "parse_error", "unsupported_function", "external_link", "plugin_formula"];
        let activeFilter = "All";

        function renderIssueList() {
          const listEl = issueDiv.querySelector(".import-issue-list");
          if (listEl) listEl.remove();
          const list = document.createElement("div");
          list.className = "import-issue-list";
          list.style.cssText = "max-height:200px;overflow-y:auto;";

          const filtered = activeFilter === "All" ? issues : issues.filter(c => c.classification === activeFilter);
          const toShow = filtered.slice(0, 200);
          for (const item of toShow) {
            const row = document.createElement("div");
            row.className = "import-issue-item";
            const badge = {parse_error: "\u2716", unsupported_function: "\u26A0", external_link: "\ud83d\udd17", plugin_formula: "\u2699"}[item.classification] || "\u2022";
            const badgeCls = {parse_error: "health-error", unsupported_function: "health-warning", external_link: "health-warning", plugin_formula: ""}[item.classification] || "";
            row.innerHTML =
              `<span class="severity-icon ${badgeCls}">${badge}</span>` +
              `<span class="import-issue-loc" data-sheet="${esc(item.sheet)}" data-addr="${esc(item.addr)}">${esc(item.sheet)}!${esc(item.addr)}</span>` +
              `<span class="import-issue-formula" title="${esc(item.formula)}">${esc(item.formula.substring(0, 40))}</span>` +
              `<button class="btn import-detail-btn" title="Toggle details" style="font-size:9px;padding:0 4px;">&#x25B6;</button>` +
              `<button class="btn import-todo-btn" title="Mark TODO">TODO</button>` +
              `<button class="btn import-value-btn" title="Convert to value">Value</button>`;

            // Details toggle
            const detailBtn = row.querySelector(".import-detail-btn");
            let detailDiv = null;
            detailBtn.addEventListener("click", () => {
              if (detailDiv) {
                detailDiv.remove();
                detailDiv = null;
                detailBtn.innerHTML = "&#x25B6;";
                return;
              }
              detailBtn.innerHTML = "&#x25BC;";
              detailDiv = document.createElement("div");
              detailDiv.style.cssText = "font-size:9px;color:var(--fg-dim);padding:2px 0 4px 20px;white-space:pre-wrap;font-family:monospace;";
              let lines = [];
              if (item.non_ascii_chars) lines.push("non_ascii_chars: " + item.non_ascii_chars);
              if (item.sanitized_preview) lines.push("sanitized_preview: " + item.sanitized_preview);
              if (item.error_message) lines.push("parser_error: " + item.error_message);
              if (item.repr) lines.push("repr: " + item.repr);
              if (!lines.length) lines.push("(no additional diagnostics)");
              detailDiv.textContent = lines.join("\n");
              row.after(detailDiv);
            });

            row.querySelector(".import-issue-loc").addEventListener("click", () => {
              if (S.sheets.includes(item.sheet)) switchSheet(item.sheet);
              jumpToCell(item.addr);
            });
            row.querySelector(".import-todo-btn").addEventListener("click", async () => {
              try {
                await api("POST", "/import/review/todo", {sheet: item.sheet, addr: item.addr});
                log("Marked " + item.sheet + "!" + item.addr + " as TODO", "success");
                loadSheet();
              } catch (err) { log("Error: " + err.message, "error"); }
            });
            row.querySelector(".import-value-btn").addEventListener("click", async () => {
              try {
                await api("POST", "/import/review/convert-value", {sheet: item.sheet, addr: item.addr});
                log("Converted " + item.sheet + "!" + item.addr + " to value", "success");
                loadSheet();
                loadImportReport();
              } catch (err) { log("Error: " + err.message, "error"); }
            });
            list.appendChild(row);
          }
          if (filtered.length > 200) {
            const more = document.createElement("div");
            more.style.cssText = "color:var(--fg-dim);font-size:10px;padding:4px 0;";
            more.textContent = "... and " + (filtered.length - 200) + " more";
            list.appendChild(more);
          }
          issueDiv.appendChild(list);
        }

        for (const f of filters) {
          const chip = document.createElement("button");
          chip.className = "btn" + (f === activeFilter ? " active" : "");
          chip.textContent = f === "All" ? "All" : f.replace(/_/g, " ");
          chip.addEventListener("click", () => {
            activeFilter = f;
            filterBar.querySelectorAll(".btn").forEach(b => b.classList.remove("active"));
            chip.classList.add("active");
            renderIssueList();
          });
          filterBar.appendChild(chip);
        }
        issueDiv.appendChild(filterBar);
        renderIssueList();
        el.appendChild(issueDiv);
      }
    }

    // Download trace button — own row at bottom
    const traceRow = document.createElement("div");
    traceRow.style.cssText = "margin-top:8px;padding-top:6px;border-top:1px solid var(--gridline);";
    const traceBtn = document.createElement("button");
    traceBtn.className = "btn";
    traceBtn.style.cssText = "font-size:10px;width:100%;";
    traceBtn.textContent = "\u2b07 Download import trace JSON";
    traceBtn.addEventListener("click", () => {
      window.open("/api/import/trace/download/latest", "_blank");
    });
    traceRow.appendChild(traceBtn);
    el.appendChild(traceRow);
  } catch (_) {
    // 404 is expected if no import
    const el = document.getElementById("import-summary");
    if (el) el.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No import reports</div>';
  }
}

// ── Health ──
async function loadHealth() {
  try {
    const data = await api("GET", "/health");
    // Update health dot
    const dot = document.getElementById("health-dot");
    if (dot) {
      dot.className = "dot " + ({ok: "dot-ok", warn: "dot-warning", error: "dot-error"}[data.status] || "dot-ok");
    }

    // Populate health list
    const el = document.getElementById("health-list");
    if (!el) return;
    el.innerHTML = "";

    if (!data.issues || data.issues.length === 0) {
      el.innerHTML = '<div style="color:var(--success);font-size:10px;padding:4px 0;">All clear</div>';
      return;
    }

    for (const issue of data.issues) {
      const item = document.createElement("div");
      item.className = "health-item";
      const icon = {error: "\u2716", warning: "\u26A0", info: "\u2139"}[issue.severity] || "\u2022";
      const cls = "health-" + issue.severity;
      let targetText = "";
      let targetIsDict = false;
      if (issue.target && typeof issue.target === "object" && issue.target.sheet && issue.target.addr) {
        targetText = issue.target.sheet + "!" + issue.target.addr;
        targetIsDict = true;
      } else if (issue.target) {
        targetText = String(issue.target);
      }
      let html = `<span class="severity-icon ${cls}">${icon}</span><span class="health-msg">${esc(issue.message)}</span>`;
      if (targetText) {
        html += `<span class="health-target">${esc(targetText)}</span>`;
      }
      item.innerHTML = html;

      // Click target to navigate
      const targetEl = item.querySelector(".health-target");
      if (targetEl && targetText) {
        targetEl.addEventListener("click", () => {
          if (targetIsDict) {
            const sheet = issue.target.sheet;
            if (S.sheets.includes(sheet)) switchSheet(sheet);
            jumpToCell(issue.target.addr);
          } else if (issue.code && issue.code.startsWith("formula_")) {
            const t = targetText;
            const parts = t.split("!");
            if (parts.length === 2) {
              const sheet = parts[0];
              if (S.sheets.includes(sheet)) switchSheet(sheet);
              jumpToCell(parts[1]);
            } else {
              jumpToCell(t);
            }
          }
        });
      }
      el.appendChild(item);
    }
  } catch (_) {}
}

function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// ── Logs tab ──
let _logPollTimer = null;

async function loadLogs() {
  const scopeEl = document.getElementById("log-scope");
  const scopeIdEl = document.getElementById("log-scope-id");
  const listEl = document.getElementById("log-list");
  if (!scopeEl || !listEl) return;

  const scope = scopeEl.value;
  const scopeId = scopeIdEl.value.trim();

  try {
    let url = "/events/tail?scope=" + encodeURIComponent(scope) + "&n=200";
    if ((scope === "build" || scope === "run" || scope === "sync") && scopeId) {
      url += "&id=" + encodeURIComponent(scopeId);
    }
    const events = await api("GET", url);
    listEl.innerHTML = "";
    if (!events.length) {
      listEl.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No events</div>';
      return;
    }
    for (const evt of events) {
      const entry = document.createElement("div");
      entry.className = "log-entry";
      const ts = (evt.ts || "").substring(11, 19);
      const level = (evt.level || "").toLowerCase();
      const levelCls = "log-level log-level-" + level;
      const displayType = evt.display_type || evt.event_type || "";
      entry.innerHTML =
        `<span class="log-ts">${esc(ts)}</span> ` +
        `<span class="${levelCls}">${esc((evt.level || "").toUpperCase().substring(0, 4))}</span> ` +
        `<span class="log-type">${esc(displayType)}</span> ` +
        `<span class="log-msg">${esc(evt.message || "")}</span>`;
      listEl.appendChild(entry);
    }
  } catch (_) {}
}

function setupLogControls() {
  const scopeEl = document.getElementById("log-scope");
  const scopeIdEl = document.getElementById("log-scope-id");
  const refreshBtn = document.getElementById("btn-log-refresh");

  if (scopeEl) {
    scopeEl.addEventListener("change", () => {
      const needs = scopeEl.value === "build" || scopeEl.value === "run" || scopeEl.value === "sync";
      scopeIdEl.disabled = !needs;
      if (!needs) scopeIdEl.value = "";
      loadLogs();
    });
  }
  if (refreshBtn) refreshBtn.addEventListener("click", loadLogs);
}

function startLogPolling() {
  stopLogPolling();
  _logPollTimer = setInterval(loadLogs, 3000);
}

function stopLogPolling() {
  if (_logPollTimer) { clearInterval(_logPollTimer); _logPollTimer = null; }
}

// ── Checks tab ──
async function loadChecks(runId) {
  const rid = runId || S.lastRunId;
  const assertEl = document.getElementById("checks-assertions");
  const verifyEl = document.getElementById("checks-verify");
  const timingsEl = document.getElementById("checks-timings");
  if (!assertEl) return;

  if (!rid) {
    assertEl.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;">No builds yet</div>';
    verifyEl.innerHTML = "";
    timingsEl.innerHTML = "";
    return;
  }

  try {
    const data = await api("GET", "/build/checks?run_id=" + encodeURIComponent(rid));

    // Assertions
    assertEl.innerHTML = "<h4>Assertions</h4>";
    if (data.assertions && data.assertions.status) {
      const statusCls = {pass: "check-pass", fail: "check-fail", warn: "check-warn"}[data.assertions.status] || "check-none";
      assertEl.innerHTML += `<div class="check-item"><span class="check-icon ${statusCls}">${data.assertions.status === "pass" ? "\u2713" : "\u2717"}</span><span class="check-label">Status</span><span class="check-value ${statusCls}">${esc(data.assertions.status)}</span></div>`;
      if (data.assertions.failed_count > 0) {
        assertEl.innerHTML += `<div class="check-item"><span class="check-icon check-fail">\u2717</span><span class="check-label">Failed</span><span class="check-value">${data.assertions.failed_count}</span></div>`;
      }
      if (data.assertions.warn_count > 0) {
        assertEl.innerHTML += `<div class="check-item"><span class="check-icon check-warn">\u26A0</span><span class="check-label">Warnings</span><span class="check-value">${data.assertions.warn_count}</span></div>`;
      }
    } else {
      assertEl.innerHTML += '<div style="color:var(--fg-dim);font-size:10px;">No assertions</div>';
    }

    // Scenario info
    if (data.scenario_name) {
      assertEl.innerHTML += `<div class="check-item"><span class="check-icon" style="color:var(--accent);">S</span><span class="check-label">Scenario</span><span class="check-value">${esc(data.scenario_name)}</span></div>`;
    }
    if (data.overlay_hash) {
      assertEl.innerHTML += `<div class="check-item"><span class="check-icon" style="color:var(--fg-dim);">#</span><span class="check-label">Overlay hash</span><span class="check-value" style="font-size:9px;">${esc(data.overlay_hash.substring(0, 16))}...</span></div>`;
    }

    // Verify report
    verifyEl.innerHTML = "<h4>Verification</h4>";
    if (data.verify) {
      const vCls = data.verify.status === "pass" ? "check-pass" : "check-fail";
      verifyEl.innerHTML += `<div class="check-item"><span class="check-icon ${vCls}">${data.verify.status === "pass" ? "\u2713" : "\u2717"}</span><span class="check-label">Status</span><span class="check-value ${vCls}">${esc(data.verify.status)}</span></div>`;
      if (data.verify.failures && data.verify.failures.length) {
        for (const f of data.verify.failures) {
          verifyEl.innerHTML += `<div class="check-item"><span class="check-icon check-fail">\u2717</span><span class="check-label" title="${esc(f)}">${esc(f.substring(0, 60))}</span></div>`;
        }
      }
    } else {
      verifyEl.innerHTML += '<div style="color:var(--fg-dim);font-size:10px;">Not verified yet</div>';
    }

    // Lookup violations
    const violEl = document.getElementById("checks-violations");
    if (violEl) {
      violEl.innerHTML = "<h4>Lookup Violations</h4>";
      if (data.lookup_violations && data.lookup_violations.length) {
        for (const v of data.lookup_violations) {
          const msg = v.message || JSON.stringify(v.extra || {});
          violEl.innerHTML += `<div class="check-item"><span class="check-icon check-warn">\u26A0</span><span class="check-label">${esc(msg)}</span></div>`;
        }
      } else {
        violEl.innerHTML += '<div style="color:var(--fg-dim);font-size:10px;">No violations</div>';
      }
    }

    // Timings
    timingsEl.innerHTML = "<h4>Timings</h4>";
    if (data.timings_ms) {
      for (const [phase, ms] of Object.entries(data.timings_ms)) {
        timingsEl.innerHTML += `<div class="check-item"><span class="check-icon" style="color:var(--fg-dim);">\u23F1</span><span class="check-label">${esc(phase)}</span><span class="check-value">${Number(ms).toFixed(1)} ms</span></div>`;
      }
    } else {
      timingsEl.innerHTML += '<div style="color:var(--fg-dim);font-size:10px;">No timing data</div>';
    }
  } catch (_) {
    assertEl.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;">Could not load checks</div>';
    verifyEl.innerHTML = "";
    timingsEl.innerHTML = "";
  }
}

async function doVerifyRun() {
  const rid = S.lastRunId;
  if (!rid) { log("No builds to verify", "error"); return; }
  try {
    log("Verifying build " + rid + "...");
    const report = await api("POST", "/build/verify", { run_id: rid });
    if (report.status === "pass") {
      log("Verification passed: " + rid, "success");
    } else {
      log("Verification failed: " + (report.failures || []).length + " issue(s)", "error");
    }
    loadChecks(rid);
  } catch (err) { log("Verify error: " + err.message, "error"); }
}

const btnVerifyRun = document.getElementById("btn-verify-run");
if (btnVerifyRun) btnVerifyRun.addEventListener("click", doVerifyRun);

const btnRunVerify = document.getElementById("btn-run-verify");
if (btnRunVerify) btnRunVerify.addEventListener("click", doVerifyRun);

// ── Incidents tab ──
async function loadIncidents(runId) {
  const rid = runId || S.lastRunId;
  const contentEl = document.getElementById("checks-content");
  if (!contentEl) return;

  if (!rid) {
    contentEl.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No builds yet</div>';
    return;
  }

  try {
    const url = "/incidents" + (rid ? "?run_id=" + encodeURIComponent(rid) : "");
    const data = await api("GET", url);

    let html = "";
    // Summary bar
    html += '<div class="incidents-summary">';
    html += `<span class="cnt cnt-error">${data.counts.error} error(s)</span>`;
    html += `<span class="cnt cnt-warning">${data.counts.warning} warning(s)</span>`;
    html += `<span class="cnt cnt-info">${data.counts.info} info</span>`;
    html += '</div>';

    if (data.incidents.length === 0) {
      html += '<div style="color:var(--fg-dim);font-size:10px;padding:4px 0;">No incidents</div>';
    } else {
      for (const inc of data.incidents) {
        const sev = inc.severity || "info";
        const icon = sev === "error" ? "\u2717" : sev === "warning" ? "\u26A0" : "\u2139";
        html += `<div class="incident-card severity-${esc(sev)}">`;
        html += `<div class="incident-header">`;
        html += `<span class="incident-icon">${icon}</span>`;
        html += `<span class="incident-title">${esc(inc.title || inc.code || "")}</span>`;
        html += `</div>`;
        if (inc.detail) {
          html += `<div class="incident-detail">${esc(inc.detail)}</div>`;
        }
        if (inc.location) {
          html += `<div class="incident-loc" onclick="jumpToCell('${esc(inc.location)}')">${esc(inc.location)}</div>`;
        }
        if (inc.suggested_action) {
          html += `<div class="incident-action">${esc(inc.suggested_action)}</div>`;
        }
        html += '</div>';
      }
    }
    contentEl.innerHTML = html;
  } catch (_) {
    contentEl.innerHTML = '<div style="color:var(--fg-dim);font-size:10px;">Could not load incidents</div>';
  }
}

// ── Status Ribbon ──
async function loadStatus() {
  try {
    const data = await api("GET", "/status");
    const srDirty = document.getElementById("sr-dirty");
    const srDs = document.getElementById("sr-datasheets");
    const srBuild = document.getElementById("sr-build");
    const srVerify = document.getElementById("sr-verify");
    const srOpenTable = document.getElementById("sr-open-table");

    // Dirty / committed pill
    if (srDirty) {
      srDirty.textContent = data.project.dirty ? "UNCOMMITTED" : "COMMITTED";
      srDirty.className = "sr-pill sr-dirty " + (data.project.dirty ? "sr-state-dirty" : "sr-state-committed");
    }

    // Datasheets pill
    if (srDs) {
      const ds = data.datasheets;
      if (ds.total === 0) {
        srDs.textContent = "DATASHEETS: --";
        srDs.className = "sr-pill sr-ds sr-state-none";
      } else if (ds.summary_status === "fresh") {
        srDs.textContent = "DATASHEETS: " + ds.total + " fresh";
        srDs.className = "sr-pill sr-ds sr-state-fresh";
      } else {
        const parts = [];
        if (ds.counts.stale) parts.push(ds.counts.stale + " stale");
        if (ds.counts.fail) parts.push(ds.counts.fail + " fail");
        srDs.textContent = "DATASHEETS: " + parts.join(", ");
        srDs.className = "sr-pill sr-ds " + (ds.counts.fail ? "sr-state-fail" : "sr-state-stale");
      }
      // Click on datasheets pill opens datasheets section
      srDs.style.cursor = "pointer";
      srDs.onclick = () => {
        const dsTab = document.querySelector('[data-tab="scalars"]');
        if (dsTab) dsTab.click();
      };
    }

    // Build pill
    if (srBuild) {
      const b = data.build;
      if (!b.has_build) {
        srBuild.textContent = "LAST BUILD: --";
        srBuild.className = "sr-pill sr-build sr-state-none";
      } else {
        const ts = b.built_at ? b.built_at.substring(11, 19) : "";
        srBuild.textContent = "LAST BUILD: " + ts + (b.status === "ok" ? "" : " (" + b.status + ")");
        srBuild.className = "sr-pill sr-build " + (b.status === "ok" ? "sr-state-pass" : "sr-state-fail");
      }
    }

    // Verify pill
    if (srVerify) {
      const v = data.verify;
      if (v.status === "unknown") {
        srVerify.textContent = "VERIFY: --";
        srVerify.className = "sr-pill sr-verify sr-state-unknown";
      } else {
        srVerify.textContent = "VERIFY: " + v.status.toUpperCase();
        srVerify.className = "sr-pill sr-verify " + (v.status === "pass" ? "sr-state-pass" : "sr-state-fail");
      }
      // Click on verify pill opens incidents tab
      srVerify.style.cursor = "pointer";
      srVerify.onclick = () => {
        const checksTab = document.querySelector('[data-tab="checks"]');
        if (checksTab) checksTab.click();
      };
    }

    // Open Table button: show after a successful build
    if (srOpenTable) {
      if (data.build.has_build && data.build.status === "ok") {
        srOpenTable.style.display = "";
      } else {
        srOpenTable.style.display = "none";
      }
    }
  } catch (_) {}
}

// Poll status every 3 seconds
let _statusTimer = null;
function startStatusPolling() {
  if (_statusTimer) clearInterval(_statusTimer);
  _statusTimer = setInterval(loadStatus, 3000);
}

// ── Open Latest Table ──
async function openLatestTable(runId) {
  try {
    const url = "/run/latest/table" + (runId ? "?run_id=" + encodeURIComponent(runId) : "");
    const data = await api("GET", url);
    if (data.table_name) {
      // Switch to Tables tab and load preview
      const tablesTab = document.querySelector('[data-tab="tables"]');
      if (tablesTab) tablesTab.click();
      loadTablePreview(data.table_name);
      log("Opened table: " + data.table_name);
    }
  } catch (_) {}
}

// Open Table button handler
const srOpenTableBtn = document.getElementById("sr-open-table");
if (srOpenTableBtn) srOpenTableBtn.addEventListener("click", () => openLatestTable(S.lastRunId));

// ── Pipeline ──
async function doPipeline() {
  if (S.dirty) {
    log("Commit before running pipeline (uncommitted edits)", "error");
    return;
  }
  try {
    log("Running pipeline (Sync \u2192 Build \u2192 Verify)...");
    const res = await api("POST", "/pipeline/run");

    // Log per-step results
    for (const step of (res.steps || [])) {
      const sym = step.status === "ok" ? "\u2713" : step.status === "skipped" ? "\u2192" : "\u2717";
      log(sym + " " + step.step + ": " + step.status, step.status === "error" ? "error" : "");
    }

    if (res.run_id) {
      S.lastRunId = res.run_id;
    }
    updateStatus();

    if (res.status === "ok") {
      log("Pipeline complete: " + (res.run_id || ""), "success");
    } else {
      log("Pipeline failed: " + (res.error || ""), "error");
      // Switch to Incidents tab on failure
      const checksTab = document.querySelector('[data-tab="checks"]');
      if (checksTab) checksTab.click();
    }

    loadScalars();
    loadRuns();
    loadIncidents(res.run_id);
    loadChecks(res.run_id);
    loadDatasheets();
    loadLogs();
    loadStatus();
  } catch (err) { log("Pipeline error: " + err.message, "error"); }
}

// Pipeline button handlers
const btnPipeline = document.getElementById("btn-pipeline");
if (btnPipeline) btnPipeline.addEventListener("click", doPipeline);
const btnPipelineMenu = document.getElementById("btn-pipeline-menu");
if (btnPipelineMenu) btnPipelineMenu.addEventListener("click", doPipeline);

// ── Prod mode banner ──
function updateProdBanner(mode) {
  const banner = document.getElementById("prod-banner");
  if (banner) {
    if (mode === "prod") {
      banner.classList.add("visible");
    } else {
      banner.classList.remove("visible");
    }
  }
}

// ── Registry ──
async function loadRegistryStatus() {
  try {
    const data = await api("GET", "/registry/status");
    const el = document.getElementById("registry-status");
    if (!el) return;
    const dot = data.reachable ? "\u2705" : (data.enabled ? "\u26A0\uFE0F" : "\u2B55");
    el.innerHTML = `<div style="font-size:11px;padding:4px 0;">
      <div><span style="opacity:0.5">Backend:</span> ${esc(data.backend)}</div>
      <div><span style="opacity:0.5">Status:</span> ${dot} ${data.reachable ? "connected" : (data.enabled ? "unreachable" : "disabled")}</div>
      <div><span style="opacity:0.5">Store runs:</span> ${data.store_runs ? "yes" : "no"}</div>
    </div>`;
  } catch (_) {}
}

async function registryPush() {
  try {
    const data = await api("POST", "/registry/push");
    if (data.pushed && data.pushed.length) {
      showToast("Pushed: " + data.pushed.join(", "));
    } else {
      showToast("Nothing pushed");
    }
    await loadRegistryStatus();
  } catch (e) {
    showToast("Push failed: " + (e.message || e));
  }
}

document.getElementById("btn-registry-push")?.addEventListener("click", registryPush);

// ── Global keyboard shortcuts ──
window.addEventListener("keydown", e => {
  // Ctrl+S = Save (global)
  if ((e.ctrlKey || e.metaKey) && e.key === "s") {
    e.preventDefault();
    doSave();
  }
  // Ctrl+B = Toggle panel (global)
  if ((e.ctrlKey || e.metaKey) && e.key === "b") {
    e.preventDefault();
    togglePanel();
  }
  // Ctrl+O = Import XLSX
  if ((e.ctrlKey || e.metaKey) && e.key === "o") {
    e.preventDefault();
    openImportModal();
  }
  // Escape closes import overlay
  if (e.key === "Escape" && importOverlay.classList.contains("visible")) {
    e.preventDefault();
    closeImportModal();
    return;
  }
  // ? or Esc closes keyboard overlay
  if (S.kbOverlay) {
    if (e.key === "?" || e.key === "Escape") {
      e.preventDefault();
      toggleKbOverlay();
    }
  }
});

// Click on overlay background closes it
kbOverlay.addEventListener("click", e => {
  if (e.target === kbOverlay) toggleKbOverlay();
});

// ── Worksheet tab ──

let _wsSpecsLoaded = false;

async function loadWorksheetSpecs() {
  const specSel = document.getElementById("ws-spec-select");
  const tableSel = document.getElementById("ws-table-select");
  if (!specSel || !tableSel) return;

  // Populate table selector from already-loaded output tables
  tableSel.innerHTML = "";
  if (S.outputTables && S.outputTables.length > 0) {
    for (const t of S.outputTables) {
      const opt = document.createElement("option");
      opt.value = t;
      opt.textContent = t;
      tableSel.appendChild(opt);
    }
  } else {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "(no tables)";
    tableSel.appendChild(opt);
  }

  // Load specs (only on first visit or if not yet loaded)
  if (_wsSpecsLoaded) return;
  try {
    const specs = await api("GET", "/worksheet/specs");
    specSel.innerHTML = "";
    if (specs.length === 0) {
      const opt = document.createElement("option");
      opt.value = "";
      opt.textContent = "(no specs)";
      specSel.appendChild(opt);
    } else {
      for (const s of specs) {
        const opt = document.createElement("option");
        opt.value = s.file;
        opt.textContent = s.name + (s.title ? " — " + s.title : "");
        specSel.appendChild(opt);
      }
    }
    _wsSpecsLoaded = true;
  } catch (err) {
    log("Worksheet specs: " + err.message, "error");
  }
}

document.getElementById("btn-ws-compile").addEventListener("click", async () => {
  const specFile = document.getElementById("ws-spec-select").value;
  const tableName = document.getElementById("ws-table-select").value;
  const mount = document.getElementById("ws-viewer-mount");

  if (!specFile || !tableName) {
    log("Select a spec and table first", "error");
    return;
  }

  mount.innerHTML = '<div style="color:var(--fg-dim);padding:8px;">Compiling...</div>';

  try {
    const result = await api("POST", "/worksheet/compile", {
      spec_file: specFile,
      table_name: tableName,
    });
    if (window.WorksheetViewer) {
      WorksheetViewer.render(mount, result);
    } else {
      mount.innerHTML = '<pre style="font-size:10px;overflow:auto;max-height:400px;">' +
        JSON.stringify(result, null, 2).replace(/</g, "&lt;") + '</pre>';
    }
    log("Worksheet compiled", "success");
  } catch (err) {
    mount.innerHTML = '<div style="color:var(--error);padding:8px;">' +
      err.message.replace(/</g, "&lt;") + '</div>';
    log("Worksheet compile error", "error");
  }
});

// ═══════════════════════════════════════════════
// Mode System
// ═══════════════════════════════════════════════

function setMode(mode) {
  if (!["spreadsheet", "system", "terminal"].includes(mode)) return;
  S.uiMode = mode;
  localStorage.setItem("fin123_uiMode", mode);
  document.body.className = document.body.className.replace(/mode-\S+/g, "").trim();
  document.body.classList.add("mode-" + mode);
  document.querySelectorAll(".mode-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  });
  setTimeout(() => resizeCanvas(), 20);
  if (mode === "terminal") {
    setTimeout(() => document.getElementById("terminal-input").focus(), 50);
  }
}

document.querySelectorAll(".mode-btn").forEach(btn => {
  btn.addEventListener("click", () => setMode(btn.dataset.mode));
});

// ═══════════════════════════════════════════════
// Terminal Shell
// ═══════════════════════════════════════════════

const termOutput = document.getElementById("terminal-output");
const termInput  = document.getElementById("terminal-input");
const termToggle = document.getElementById("terminal-toggle");
const termPanel  = document.getElementById("terminal-panel");

const termHistory = [];
let termHistIdx = -1;

// Scenarios are now persisted via backend API (/api/scenarios)

function termEsc(s) {
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

function termAppend(html) {
  const div = document.createElement("div");
  div.className = "term-block";
  div.innerHTML = html;
  termOutput.appendChild(div);
  termOutput.scrollTop = termOutput.scrollHeight;
}

function termClear() { termOutput.innerHTML = ""; }

function termEcho(cmd) {
  termAppend('<div class="term-echo">' + termEsc(cmd) + '</div>');
}

function termError(msg) {
  termAppend('<div class="term-error">' + termEsc(msg) + '</div>');
}

function termSuccess(msg) {
  termAppend('<div class="term-success">' + termEsc(msg) + '</div>');
}

function termText(msg) {
  termAppend('<div class="term-text">' + termEsc(msg) + '</div>');
}

function termStatus(title, rows) {
  let html = '<div class="term-status"><div class="term-status-title">' + termEsc(title) + '</div>';
  for (const [k, v] of rows) {
    html += '<div class="term-status-row"><span class="term-status-key">' + termEsc(k) + '</span><span class="term-status-val">' + termEsc(v) + '</span></div>';
  }
  html += '</div>';
  termAppend(html);
}

function termCard(title, rows) {
  let html = '<div class="term-card"><div class="term-card-title">' + termEsc(title) + '</div>';
  for (const [k, v] of rows) {
    html += '<div class="term-card-row"><span class="term-card-key">' + termEsc(k) + '</span><span class="term-card-val">' + termEsc(v) + '</span></div>';
  }
  html += '</div>';
  termAppend(html);
}

function termTable(headers, rows) {
  let html = '<table class="term-table"><thead><tr>';
  for (const h of headers) html += '<th>' + termEsc(h) + '</th>';
  html += '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>';
    for (const cell of row) html += '<td>' + termEsc(cell) + '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  termAppend(html);
}

function termSection(title) {
  termAppend('<div class="term-section">' + termEsc(title) + '</div>');
}

function _fmtVal(v) {
  if (v == null || v === "" || v === "—") return "—";
  const s = String(v);
  const n = Number(v);
  if (!isNaN(n) && s !== "" && isFinite(n)) {
    if (Number.isInteger(n) && Math.abs(n) < 1e15) return n.toLocaleString("en-US");
    if (Math.abs(n) >= 1000) return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (Math.abs(n) < 0.01 && n !== 0) return n.toExponential(2);
    return n.toLocaleString("en-US", { maximumFractionDigits: 4 });
  }
  return s;
}

function _fmtTs(ts) {
  if (!ts) return "—";
  return ts.substring(0, 19).replace("T", " ");
}

function _fmtRunId(id) {
  if (!id) return "—";
  return '<span class="term-run-id">' + termEsc(id) + '</span>';
}

// Live progress block (replaceable)
let _progressEl = null;
function termProgress(current, total, detail) {
  const pct = Math.round((current / total) * 100);
  const barW = Math.round((current / total) * 100);
  const html =
    '<span class="term-progress-bar"><span class="term-progress-fill" style="width:' + barW + '%;"></span></span>' +
    current + '/' + total + ' (' + pct + '%)' +
    (detail ? '  ' + termEsc(detail) : '');
  if (_progressEl && _progressEl.parentNode === termOutput) {
    _progressEl.innerHTML = html;
  } else {
    _progressEl = document.createElement("div");
    _progressEl.className = "term-block term-progress";
    _progressEl.innerHTML = html;
    termOutput.appendChild(_progressEl);
  }
  termOutput.scrollTop = termOutput.scrollHeight;
}
function termProgressDone() {
  if (_progressEl && _progressEl.parentNode) {
    _progressEl.parentNode.removeChild(_progressEl);
  }
  _progressEl = null;
}

// Preferred output key ordering heuristic
const _PREFERRED_OUTPUTS = [
  "value_per_share", "enterprise_value", "implied_upside",
  "equity_value", "net_income", "ebitda", "free_cash_flow",
  "irr", "npv", "total_return",
];
function _selectOutputKeys(allKeys, selected, cap) {
  if (selected && selected.length > 0) {
    return selected.filter(k => allKeys.has(k));
  }
  const ordered = [];
  for (const pk of _PREFERRED_OUTPUTS) {
    if (allKeys.has(pk)) ordered.push(pk);
  }
  for (const k of allKeys) {
    if (!ordered.includes(k)) ordered.push(k);
  }
  return ordered.slice(0, cap || 6);
}

// Last truncatable AI content (for show full last)
let _lastAiContent = null;

const _TRUNCATE_LINES = 8;

// ── Command Registry ──

const CMD_REGISTRY = {};

function registerCmd(name, meta) {
  CMD_REGISTRY[name] = meta;
}

function parseCommand(input) {
  const trimmed = input.trim();
  if (!trimmed) return null;
  // Handle set <name> = <value> specially
  const setMatch = trimmed.match(/^set\s+(\S+)\s*=\s*(.+)$/i);
  if (setMatch) return { name: "set", args: [setMatch[1], setMatch[2].trim()] };
  const parts = trimmed.split(/\s+/);
  const name = parts[0].toLowerCase();
  return { name, args: parts.slice(1) };
}

async function execCommand(input) {
  const parsed = parseCommand(input);
  if (!parsed) return;
  termEcho(input);
  // Check for compound command names (e.g. "scenario save", "show input")
  const compoundName = parsed.name + (parsed.args.length ? " " + parsed.args[0].toLowerCase() : "");
  const compound3 = compoundName + (parsed.args.length > 1 ? " " + parsed.args[1].toLowerCase() : "");

  let cmd = CMD_REGISTRY[parsed.name];
  let cmdArgs = parsed.args;

  // Try compound 2-word match
  if (CMD_REGISTRY[compoundName]) {
    cmd = CMD_REGISTRY[compoundName];
    cmdArgs = parsed.args.slice(1);
  }

  if (!cmd) {
    termError("Unknown command: " + parsed.name + ". Type 'help' for available commands.");
    return;
  }
  try {
    await cmd.handler(cmdArgs);
  } catch (err) {
    termError("Error: " + err.message);
  }
}

// Terminal input handling
termInput.addEventListener("keydown", e => {
  if (e.key === "Enter") {
    const val = termInput.value.trim();
    if (val) {
      termHistory.push(val);
      termHistIdx = termHistory.length;
      execCommand(val);
      termInput.value = "";
    }
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    if (termHistIdx > 0) {
      termHistIdx--;
      termInput.value = termHistory[termHistIdx];
    }
  } else if (e.key === "ArrowDown") {
    e.preventDefault();
    if (termHistIdx < termHistory.length - 1) {
      termHistIdx++;
      termInput.value = termHistory[termHistIdx];
    } else {
      termHistIdx = termHistory.length;
      termInput.value = "";
    }
  }
});

// Terminal collapse toggle
termToggle.addEventListener("click", () => {
  termPanel.classList.toggle("collapsed");
});

// ── Phase 1 Commands ──

registerCmd("help", {
  description: "Show available commands or help for a specific command",
  usage: "help [command]",
  group: "general",
  handler(args) {
    if (args.length > 0) {
      const name = args.join(" ").toLowerCase();
      const cmd = CMD_REGISTRY[name];
      if (!cmd) { termError("Unknown command: " + name); return; }
      let html = '<div class="term-card">';
      html += '<div class="term-card-title">' + termEsc(name) + '</div>';
      html += '<div class="term-card-row"><span class="term-card-key">Usage</span><span class="term-card-val">' + termEsc(cmd.usage || name) + '</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">Description</span><span class="term-card-val">' + termEsc(cmd.description) + '</span></div>';
      html += '</div>';
      termAppend(html);
      return;
    }
    const groups = {};
    for (const [name, cmd] of Object.entries(CMD_REGISTRY)) {
      if (cmd.hidden) continue;
      const g = cmd.group || "other";
      if (!groups[g]) groups[g] = [];
      groups[g].push([name, cmd.description]);
    }
    const order = ["general", "inspect", "runner", "scenario", "sweep", "ai", "draft"];
    let html = '';
    for (const g of order) {
      if (!groups[g]) continue;
      html += '<div class="term-help-group"><div class="term-help-group-title">' + termEsc(g) + '</div>';
      for (const [n, d] of groups[g]) {
        html += '<div class="term-help-row"><span class="term-help-cmd">' + termEsc(n) + '</span><span class="term-help-desc">' + termEsc(d) + '</span></div>';
      }
      html += '</div>';
    }
    termAppend(html);
  }
});

registerCmd("clear", {
  description: "Clear terminal output",
  usage: "clear",
  group: "general",
  handler() { termClear(); }
});

registerCmd("mode", {
  description: "Show or switch the current UI mode",
  usage: "mode [spreadsheet|system|terminal]",
  group: "general",
  handler(args) {
    if (args.length > 0) {
      const m = args[0].toLowerCase();
      if (["spreadsheet", "system", "terminal"].includes(m)) {
        setMode(m);
        termSuccess("Switched to " + m + " mode");
      } else {
        termError("Unknown mode: " + m + ". Options: spreadsheet, system, terminal");
      }
      return;
    }
    termStatus("Current Mode", [["mode", S.uiMode]]);
  }
});

registerCmd("status", {
  description: "Show workbook and session status",
  usage: "status",
  group: "inspect",
  async handler() {
    try {
      const info = await api("GET", "/project");
      const rows = [
        ["project", info.project_dir || "unknown"],
        ["dirty", info.dirty ? "uncommitted" : "committed"],
        ["snapshot", info.snapshot_version || "none"],
        ["last build", info.last_run_id || "none"],
        ["active sheet", S.activeSheet],
        ["sheets", (info.sheets || []).join(", ")],
        ["mode", S.uiMode],
      ];
      if (info.mode) rows.push(["project mode", info.mode]);
      termStatus("Workbook Status", rows);
    } catch (err) { termError("Failed to load status: " + err.message); }
  }
});

registerCmd("inputs", {
  description: "List editable workbook parameters/inputs",
  usage: "inputs",
  group: "inspect",
  async handler() {
    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      const keys = Object.keys(params);
      if (keys.length === 0) {
        termText("No parameters defined in workbook.");
        return;
      }
      const rows = keys.map(k => [k, String(params[k])]);
      termTable(["Parameter", "Value"], rows);
    } catch (err) { termError("Failed to load inputs: " + err.message); }
  }
});

registerCmd("outputs", {
  description: "List key output scalars from the latest build",
  usage: "outputs",
  group: "inspect",
  async handler() {
    try {
      const data = await api("GET", "/outputs/scalars");
      const scalars = data.scalars || {};
      const keys = Object.keys(scalars);
      if (keys.length === 0) {
        termText("No outputs available. Use 'commit' to build first.");
        return;
      }
      const rows = keys.map(k => [k, String(scalars[k])]);
      termTable(["Output", "Value"], rows);
    } catch (err) { termError("Failed to load outputs: " + err.message); }
  }
});

registerCmd("show input", {
  description: "Show value and metadata for a named input parameter",
  usage: "show input <name>",
  group: "inspect",
  async handler(args) {
    if (args.length === 0) { termError("Usage: show input <name>"); return; }
    const name = args[0];
    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      if (!(name in params)) {
        termError("Input '" + name + "' not found. Use 'inputs' to list available parameters.");
        return;
      }
      const val = params[name];
      const rows = [
        ["name", name],
        ["value", String(val)],
        ["type", typeof val],
      ];
      termCard("Input: " + name, rows);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("show output", {
  description: "Show value and metadata for a named output scalar",
  usage: "show output <name>",
  group: "inspect",
  async handler(args) {
    if (args.length === 0) { termError("Usage: show output <name>"); return; }
    const name = args[0];
    try {
      const data = await api("GET", "/outputs/scalars");
      const scalars = data.scalars || {};
      if (!(name in scalars)) {
        termError("Output '" + name + "' not found. Use 'outputs' to list available outputs.");
        return;
      }
      const val = scalars[name];
      const rows = [
        ["name", name],
        ["value", String(val)],
        ["type", typeof val],
      ];
      if (data.run_id) rows.push(["run_id", data.run_id]);
      termCard("Output: " + name, rows);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

// ── Runner Commands ──

registerCmd("set", {
  description: "Set a named input parameter value",
  usage: "set <name> = <value>",
  group: "runner",
  async handler(args) {
    if (args.length < 2) { termError("Usage: set <name> = <value>"); return; }
    const name = args[0];
    const rawVal = args[1];
    try {
      const res = await api("POST", "/params/update", { name, value: rawVal });
      await loadSheet();
      draw();
      S.dirty = res.dirty;
      updateStatus();
      termStatus("Set Parameter", [
        ["parameter", name],
        ["old value", String(res.old_value)],
        ["new value", String(res.new_value)],
        ["state", "uncommitted"],
      ]);
    } catch (err) { termError("Failed to set: " + err.message); }
  }
});

registerCmd("reset", {
  description: "Reset a parameter to its workbook default",
  usage: "reset <name>",
  group: "runner",
  async handler(args) {
    if (args.length === 0) { termError("Usage: reset <name>"); return; }
    const name = args[0];
    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      if (!(name in params)) {
        termError("'" + name + "' is not a parameter. Use 'inputs' to list parameters.");
        return;
      }
      termText("'" + name + "' current default: " + params[name] + ". To restore, use: set " + name + " = " + params[name]);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

// Helper: commit + build the workbook, return structured result
async function _doCommitBuild() {
  // If dirty, save snapshot first
  if (S.dirty) {
    const cres = await api("POST", "/commit");
    S.dirty = false;
    S.snapVer = cres.snapshot_version;
    updateStatus();
  }

  const res = await api("POST", "/build");
  S.lastRunId = res.run_id;
  updateStatus();
  loadScalars();
  loadRuns();
  loadChecks(res.run_id);
  loadIncidents(res.run_id);
  loadLogs();
  loadStatus();

  const scalars = await api("GET", "/outputs/scalars");
  const scalarData = scalars.scalars || {};
  const tableInfo = res.tables || {};
  const nScalars = Object.keys(scalarData).length;
  const nTables = Object.keys(tableInfo).length;

  return {
    run_id: res.run_id,
    snapshot: res.snapshot_version || S.snapVer,
    scalars: scalarData,
    n_scalars: nScalars,
    n_tables: nTables,
    timestamp: new Date().toISOString(),
  };
}

registerCmd("commit", {
  description: "Persist current state and build the workbook (primary deterministic action)",
  usage: "commit [--name <scenario_name>]",
  group: "runner",
  async handler(args) {
    // Parse --name flag
    let scenarioName = null;
    for (let i = 0; i < args.length; i++) {
      if (args[i] === "--name" && i + 1 < args.length) {
        scenarioName = args[i + 1];
        i++;
      }
    }

    termText("Committing...");
    try {
      const result = await _doCommitBuild();

      termStatus("COMMIT", [
        ["status", "success"],
        ["build id", result.run_id],
        ["snapshot", result.snapshot || "—"],
        ["timestamp", result.timestamp],
        ["outputs updated", result.n_scalars + " scalars, " + result.n_tables + " tables"],
      ]);

      if (result.n_scalars > 0) {
        const rows = Object.entries(result.scalars).map(([k, v]) => [k, String(v)]);
        termTable(["Output", "Value"], rows);
      }

      // If --name was given, persist as scenario
      if (scenarioName) {
        const info = await api("GET", "/project");
        await api("POST", "/scenarios", {
          name: scenarioName,
          inputs: info.params || {},
          outputs: result.scalars,
          run_id: result.run_id,
        });
        termSuccess("Saved as scenario: " + scenarioName);
      }
    } catch (err) { termError("Commit error: " + err.message); }
  }
});

// Hidden backward-compatible alias for `run`
registerCmd("run", {
  description: "Alias for commit (deprecated — use 'commit' instead)",
  usage: "run [--name <scenario_name>]",
  group: "runner",
  hidden: true,
  async handler(args) {
    termText("Note: 'run' is deprecated. Use 'commit' instead.");
    await CMD_REGISTRY["commit"].handler(args);
  }
});

// ── Scenario Commands (persistent) ──

registerCmd("scenario save", {
  description: "Save current inputs and outputs as a persistent named scenario",
  usage: "scenario save <name>",
  group: "scenario",
  async handler(args) {
    if (args.length === 0) { termError("Usage: scenario save <name>"); return; }
    const name = args[0];
    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      let outputs = {};
      try {
        const data = await api("GET", "/outputs/scalars");
        outputs = data.scalars || {};
      } catch (_) {}
      await api("POST", "/scenarios", {
        name,
        inputs: params,
        outputs,
        run_id: S.lastRunId,
      });
      termStatus("Scenario Saved", [
        ["name", name],
        ["inputs", Object.keys(params).length + " parameters"],
        ["outputs", Object.keys(outputs).length + " scalars"],
        ["build id", S.lastRunId || "—"],
      ]);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("scenario load", {
  description: "Load a saved scenario's inputs into the workbook",
  usage: "scenario load <name>",
  group: "scenario",
  async handler(args) {
    if (args.length === 0) { termError("Usage: scenario load <name>"); return; }
    const name = args[0];
    try {
      const sc = await api("GET", "/scenarios/" + encodeURIComponent(name));
      termText("Loading scenario '" + name + "'...");
      const info = await api("GET", "/project");
      const currentParams = info.params || {};
      let changed = 0;

      for (const [k, v] of Object.entries(sc.inputs || {})) {
        if (k in currentParams && String(currentParams[k]) !== String(v)) {
          try {
            await api("POST", "/params/update", { name: k, value: v });
            changed++;
          } catch (_) {}
        }
      }
      S.dirty = true;
      updateStatus();
      await loadSheet();
      draw();
      termStatus("Scenario Loaded", [
        ["scenario", name],
        ["linked build", sc.run_id || "—"],
        ["parameters applied", String(changed)],
        ["state", "uncommitted — run 'commit' to build"],
      ]);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("scenario list", {
  description: "List all saved scenarios",
  usage: "scenario list",
  group: "scenario",
  async handler() {
    try {
      const scenarios = await api("GET", "/scenarios");
      if (scenarios.length === 0) {
        termText("No scenarios saved. Use 'scenario save <name>' to save one.");
        return;
      }
      termSection("Saved Scenarios (" + scenarios.length + ")");
      let html = '<table class="term-table"><thead><tr>';
      html += '<th>Scenario</th><th>Updated</th><th>Build ID</th><th>Key Outputs</th>';
      html += '</tr></thead><tbody>';
      for (const sc of scenarios) {
        const outs = sc.outputs || {};
        const outKeys = Object.keys(outs);
        const preview = outKeys.slice(0, 2).map(k => k + "=" + _fmtVal(outs[k])).join(", ");
        const more = outKeys.length > 2 ? " +" + (outKeys.length - 2) + " more" : "";
        html += '<tr>';
        html += '<td><span class="term-scenario-badge">' + termEsc(sc.name) + '</span></td>';
        html += '<td>' + termEsc(_fmtTs(sc.updated_at || sc.created_at)) + '</td>';
        html += '<td>' + _fmtRunId(sc.run_id) + '</td>';
        html += '<td style="color:var(--fg-dim);font-size:9px;">' + termEsc(preview + more) + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
      termAppend(html);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("scenario show", {
  description: "Show details for a saved scenario",
  usage: "scenario show <name>",
  group: "scenario",
  async handler(args) {
    if (args.length === 0) { termError("Usage: scenario show <name>"); return; }
    const name = args[0];
    try {
      const sc = await api("GET", "/scenarios/" + encodeURIComponent(name));
      // Identity card
      let html = '<div class="term-card">';
      html += '<div class="term-card-title"><span class="term-scenario-badge">' + termEsc(sc.name) + '</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">created</span><span class="term-card-val">' + termEsc(_fmtTs(sc.created_at)) + '</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">updated</span><span class="term-card-val">' + termEsc(_fmtTs(sc.updated_at)) + '</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">build id</span><span class="term-card-val">' + _fmtRunId(sc.run_id) + '</span></div>';
      if (sc.notes) html += '<div class="term-card-row"><span class="term-card-key">notes</span><span class="term-card-val">' + termEsc(sc.notes) + '</span></div>';
      html += '</div>';
      termAppend(html);

      // Inputs section
      const inputs = sc.inputs || {};
      const inKeys = Object.keys(inputs);
      if (inKeys.length > 0) {
        termSection("Inputs (" + inKeys.length + ")");
        let thtml = '<table class="term-table"><thead><tr><th>Parameter</th><th style="text-align:right;">Value</th></tr></thead><tbody>';
        for (const k of inKeys) {
          thtml += '<tr><td>' + termEsc(k) + '</td><td class="num-cell" style="text-align:right;">' + termEsc(_fmtVal(inputs[k])) + '</td></tr>';
        }
        thtml += '</tbody></table>';
        termAppend(thtml);
      }

      // Outputs section
      const outputs = sc.outputs || {};
      const outKeys = Object.keys(outputs);
      if (outKeys.length > 0) {
        termSection("Outputs (" + outKeys.length + ")");
        let thtml = '<table class="term-table"><thead><tr><th>Output</th><th style="text-align:right;">Value</th></tr></thead><tbody>';
        for (const k of outKeys) {
          thtml += '<tr><td>' + termEsc(k) + '</td><td class="num-cell" style="text-align:right;">' + termEsc(_fmtVal(outputs[k])) + '</td></tr>';
        }
        thtml += '</tbody></table>';
        termAppend(thtml);
      }
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("scenario delete", {
  description: "Delete a saved scenario",
  usage: "scenario delete <name>",
  group: "scenario",
  async handler(args) {
    if (args.length === 0) { termError("Usage: scenario delete <name>"); return; }
    const name = args[0];
    try {
      await api("DELETE", "/scenarios/" + encodeURIComponent(name));
      termSuccess("Scenario '" + name + "' deleted.");
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("compare", {
  description: "Compare two saved scenarios (inputs and outputs)",
  usage: "compare <scenario_a> <scenario_b>",
  group: "scenario",
  async handler(args) {
    if (args.length < 2) { termError("Usage: compare <scenario_a> <scenario_b>"); return; }
    const nameA = args[0], nameB = args[1];
    let scA, scB;
    try { scA = await api("GET", "/scenarios/" + encodeURIComponent(nameA)); }
    catch (_) { termError("Scenario '" + nameA + "' not found."); return; }
    try { scB = await api("GET", "/scenarios/" + encodeURIComponent(nameB)); }
    catch (_) { termError("Scenario '" + nameB + "' not found."); return; }

    // Input diffs
    const allInputKeys = new Set([...Object.keys(scA.inputs || {}), ...Object.keys(scB.inputs || {})]);
    const inputDiffs = [];
    for (const k of allInputKeys) {
      const a = (scA.inputs || {})[k];
      const b = (scB.inputs || {})[k];
      if (String(a ?? "—") !== String(b ?? "—")) inputDiffs.push([k, a, b]);
    }

    // Output diffs
    const allOutputKeys = new Set([...Object.keys(scA.outputs || {}), ...Object.keys(scB.outputs || {})]);
    const outputDiffs = [];
    for (const k of allOutputKeys) {
      const a = (scA.outputs || {})[k];
      const b = (scB.outputs || {})[k];
      if (String(a ?? "—") !== String(b ?? "—")) outputDiffs.push([k, a, b]);
    }

    // Build structured compare output
    let html = '<div class="term-compare">';

    // Header
    html += '<div class="term-compare-header">';
    html += '<span class="term-compare-title">COMPARE</span>';
    html += '<span class="term-scenario-badge">' + termEsc(nameA) + '</span>';
    html += '<span class="term-compare-meta">vs</span>';
    html += '<span class="term-scenario-badge">' + termEsc(nameB) + '</span>';
    if (scA.run_id || scB.run_id) {
      html += '<span class="term-compare-meta" style="margin-left:auto;">' +
        (scA.run_id ? termEsc(scA.run_id.substring(0, 15)) : "—") + ' → ' +
        (scB.run_id ? termEsc(scB.run_id.substring(0, 15)) : "—") + '</span>';
    }
    html += '</div>';

    // Inputs changed
    html += '<div class="term-compare-section">';
    html += '<div class="term-compare-section-title">Inputs Changed (' + inputDiffs.length + ')</div>';
    if (inputDiffs.length > 0) {
      html += '<table class="term-diff-table"><thead><tr><th>Parameter</th><th>' + termEsc(nameA) + '</th><th></th><th>' + termEsc(nameB) + '</th></tr></thead><tbody>';
      for (const [k, a, b] of inputDiffs) {
        html += '<tr><td>' + termEsc(k) + '</td>';
        html += '<td class="val-old">' + termEsc(_fmtVal(a)) + '</td>';
        html += '<td class="val-arrow">→</td>';
        html += '<td class="val-new">' + termEsc(_fmtVal(b)) + '</td></tr>';
      }
      html += '</tbody></table>';
    } else {
      html += '<div class="term-no-diff">No differences</div>';
    }
    html += '</div>';

    // Outputs changed
    html += '<div class="term-compare-section">';
    html += '<div class="term-compare-section-title">Outputs Changed (' + outputDiffs.length + ')</div>';
    if (outputDiffs.length > 0) {
      html += '<table class="term-diff-table"><thead><tr><th>Output</th><th>' + termEsc(nameA) + '</th><th></th><th>' + termEsc(nameB) + '</th></tr></thead><tbody>';
      for (const [k, a, b] of outputDiffs) {
        html += '<tr><td>' + termEsc(k) + '</td>';
        html += '<td class="val-old">' + termEsc(_fmtVal(a)) + '</td>';
        html += '<td class="val-arrow">→</td>';
        html += '<td class="val-new">' + termEsc(_fmtVal(b)) + '</td></tr>';
      }
      html += '</tbody></table>';
    } else {
      html += '<div class="term-no-diff">No differences</div>';
    }
    html += '</div>';

    // Summary
    const totalInputs = allInputKeys.size;
    const unchangedInputs = totalInputs - inputDiffs.length;
    if (unchangedInputs > 0) {
      html += '<div class="term-compare-meta" style="padding-top:4px;">' + unchangedInputs + ' input(s) unchanged</div>';
    }

    html += '</div>';
    termAppend(html);
  }
});

// ── Sweep Commands ──

function _parseRange(token) {
  // Parse range(start, stop, step) → array of numbers
  const m = token.match(/^range\(\s*([^,]+),\s*([^,]+),\s*([^)]+)\s*\)$/);
  if (!m) return null;
  const start = parseFloat(m[1]);
  const stop = parseFloat(m[2]);
  const step = parseFloat(m[3]);
  if (isNaN(start) || isNaN(stop) || isNaN(step) || step === 0) return null;
  if ((step > 0 && start > stop) || (step < 0 && start < stop)) return null;
  const vals = [];
  // Inclusive of endpoints: iterate while value <= stop (for positive step)
  for (let v = start; step > 0 ? v <= stop + step * 1e-9 : v >= stop + step * 1e-9; v += step) {
    vals.push(Math.round(v * 1e10) / 1e10); // avoid floating point drift
    if (vals.length > 200) break; // safety limit
  }
  return vals;
}

registerCmd("sweep", {
  description: "Run a one-dimensional parameter sweep using the deterministic pipeline",
  usage: "sweep <input> <v1> <v2> ... [--outputs key1 key2] | sweep <input> range(start, stop, step)",
  group: "sweep",
  async handler(args) {
    if (args.length < 2) {
      termError("Usage: sweep <input> <v1> <v2> ... [--outputs key1 key2]");
      return;
    }
    const inputName = args[0];

    // Parse --outputs flag
    let selectedOutputs = null;
    let valueArgs = args.slice(1);
    const outputsIdx = valueArgs.indexOf("--outputs");
    if (outputsIdx >= 0) {
      selectedOutputs = valueArgs.slice(outputsIdx + 1);
      valueArgs = valueArgs.slice(0, outputsIdx);
    }

    // Parse values
    let values;
    const rest = valueArgs.join(" ");
    const rangeVals = _parseRange(rest);
    if (rangeVals) {
      values = rangeVals;
    } else {
      values = valueArgs.map(v => {
        const n = parseFloat(v);
        return isNaN(n) ? v : n;
      });
    }

    if (values.length === 0) { termError("No sweep values provided."); return; }

    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      if (!(inputName in params)) {
        termError("'" + inputName + "' is not a declared parameter. Use 'inputs' to list parameters.");
        return;
      }
      const originalValue = params[inputName];
      const t0 = performance.now();

      // Start block
      termStatus("SWEEP STARTED", [
        ["parameter", inputName],
        ["points", String(values.length)],
        ["range", _fmtVal(values[0]) + " → " + _fmtVal(values[values.length - 1])],
        ...(selectedOutputs ? [["outputs", selectedOutputs.join(", ")]] : []),
      ]);

      const results = [];
      let allOutputKeys = new Set();

      for (let i = 0; i < values.length; i++) {
        const val = values[i];
        termProgress(i, values.length, inputName + " = " + _fmtVal(val));
        try {
          await api("POST", "/params/update", { name: inputName, value: val });
          const buildResult = await _doCommitBuild();
          for (const k of Object.keys(buildResult.scalars)) allOutputKeys.add(k);
          results.push({
            value: val,
            outputs: buildResult.scalars,
            run_id: buildResult.run_id,
            status: "success",
          });
        } catch (err) {
          results.push({
            value: val,
            outputs: {},
            run_id: null,
            status: "error",
            error: err.message,
          });
        }
      }
      termProgressDone();

      // Restore
      try {
        await api("POST", "/params/update", { name: inputName, value: originalValue });
        await loadSheet();
        draw();
      } catch (_) {}

      const durationMs = Math.round(performance.now() - t0);
      const successes = results.filter(r => r.status === "success").length;
      const failures = results.length - successes;
      const outputKeys = _selectOutputKeys(allOutputKeys, selectedOutputs, 6);

      // Result table
      termSection("Results");
      _renderSweepTable(inputName, outputKeys, results);

      // Persist
      const sweepId = "sweep_" + inputName + "_" + Date.now();
      const sweepData = {
        sweep_id: sweepId,
        input: inputName,
        values: values,
        results: results,
        output_keys: Array.from(allOutputKeys),
        selected_outputs: selectedOutputs,
        created_at: new Date().toISOString(),
        duration_ms: durationMs,
        success_count: successes,
        failure_count: failures,
      };
      try {
        await api("POST", "/sweeps", { sweep_id: sweepId, data: sweepData });
      } catch (_) {}

      // Completion block
      const statusLabel = failures > 0 ? "SWEEP COMPLETE WITH ERRORS" : "SWEEP COMPLETE";
      const rows = [
        ["sweep id", sweepId],
        ["points", successes + " succeeded" + (failures ? ", " + failures + " failed" : "")],
        ["duration", (durationMs / 1000).toFixed(1) + "s"],
      ];
      if (failures > 0) {
        const failedVals = results.filter(r => r.status !== "success").map(r => _fmtVal(r.value)).join(", ");
        rows.push(["failed values", failedVals]);
      }
      termStatus(statusLabel, rows);
      termText("show sweep " + sweepId + "  |  export sweep " + sweepId);

    } catch (err) { termError("Sweep error: " + err.message); }
  }
});

// Shared sweep table renderer
function _renderSweepTable(inputName, outputKeys, results) {
  let html = '<table class="term-table"><thead><tr>';
  html += '<th>' + termEsc(inputName) + '</th>';
  for (const k of outputKeys) html += '<th style="text-align:right;">' + termEsc(k) + '</th>';
  html += '<th>Build ID</th></tr></thead><tbody>';
  for (const r of results) {
    const failed = r.status !== "success";
    html += '<tr>';
    html += '<td style="font-weight:500;">' + termEsc(_fmtVal(r.value)) + '</td>';
    for (const k of outputKeys) {
      const v = (r.outputs || {})[k];
      html += '<td class="num-cell" style="text-align:right;">' + (failed ? '<span style="color:var(--error);">—</span>' : termEsc(_fmtVal(v))) + '</td>';
    }
    html += '<td>' + (r.run_id ? _fmtRunId(r.run_id) : '<span style="color:var(--error);">' + termEsc(r.status || "error") + '</span>') + '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  termAppend(html);
}

registerCmd("sweeps", {
  description: "List saved sweep results",
  usage: "sweeps",
  group: "sweep",
  async handler() {
    try {
      const list = await api("GET", "/sweeps");
      if (list.length === 0) {
        termText("No sweeps saved. Use 'sweep <input> <values...>' to run one.");
        return;
      }
      termSection("Saved Sweeps (" + list.length + ")");
      let html = '<table class="term-table"><thead><tr>';
      html += '<th>Sweep ID</th><th>Parameter</th><th>Points</th><th>Created</th>';
      html += '</tr></thead><tbody>';
      for (const s of list) {
        html += '<tr>';
        html += '<td style="font-weight:500;color:var(--fg);">' + termEsc(s.sweep_id) + '</td>';
        html += '<td>' + termEsc(s.input || "—") + '</td>';
        html += '<td class="num-cell">' + (s.points || 0) + '</td>';
        html += '<td>' + termEsc(_fmtTs(s.created_at)) + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
      termAppend(html);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("show sweep", {
  description: "Show a saved sweep result",
  usage: "show sweep <id>",
  group: "sweep",
  async handler(args) {
    if (args.length === 0) { termError("Usage: show sweep <id>"); return; }
    const id = args[0];
    try {
      const data = await api("GET", "/sweeps/" + encodeURIComponent(id));
      const results = data.results || [];
      const outputKeys = data.output_keys || [];

      // Identity card
      termCard("Sweep: " + (data.sweep_id || id), [
        ["parameter", data.input || "—"],
        ["points", String(results.length)],
        ["range", results.length ? _fmtVal(results[0].value) + " → " + _fmtVal(results[results.length - 1].value) : "—"],
        ["created", _fmtTs(data.created_at)],
        ["status", results.every(r => r.status === "success") ? "all succeeded" : "some errors"],
      ]);

      // Result table (cap displayed outputs at 8)
      let displayKeys = outputKeys;
      if (displayKeys.length > 8) {
        displayKeys = displayKeys.slice(0, 8);
        termText("Showing " + displayKeys.length + " of " + outputKeys.length + " outputs. Use 'export sweep " + id + "' for full data.");
      }

      termSection("Results");
      _renderSweepTable(data.input || "input", displayKeys, results);

      termText("Export: export sweep " + (data.sweep_id || id));
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("export sweep", {
  description: "Export a sweep result to CSV",
  usage: "export sweep <id>",
  group: "sweep",
  async handler(args) {
    if (args.length === 0) { termError("Usage: export sweep <id>"); return; }
    const id = args[0];
    try {
      const url = "/api/sweeps/" + encodeURIComponent(id) + "/csv";
      const resp = await fetch(url);
      if (!resp.ok) {
        const text = await resp.text();
        termError("Export failed: " + text);
        return;
      }
      const blob = await resp.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = id + ".csv";
      a.click();
      URL.revokeObjectURL(a.href);
      termStatus("Export Complete", [
        ["file", id + ".csv"],
        ["size", (blob.size / 1024).toFixed(1) + " KB"],
        ["format", "CSV (all outputs included)"],
      ]);
    } catch (err) { termError("Export failed: " + err.message); }
  }
});

// ── Grid Commands ──

const _GRID_MAX_CELLS = 100;

function _renderGridMatrix(inputX, valuesX, inputY, valuesY, cells, displayOutput) {
  // Build lookup: "x|y" → display_value
  const lookup = {};
  for (const c of cells) {
    lookup[c.x + "|" + c.y] = c;
  }

  let html = '<table class="term-grid-matrix"><thead><tr>';
  html += '<th>' + termEsc(inputX) + ' \\ ' + termEsc(inputY) + '</th>';
  for (const y of valuesY) html += '<th>' + termEsc(_fmtVal(y)) + '</th>';
  html += '</tr></thead><tbody>';
  for (const x of valuesX) {
    html += '<tr><td>' + termEsc(_fmtVal(x)) + '</td>';
    for (const y of valuesY) {
      const c = lookup[x + "|" + y];
      if (!c || c.status !== "success") {
        html += '<td class="grid-err">ERR</td>';
      } else {
        html += '<td>' + termEsc(_fmtVal(c.display_value)) + '</td>';
      }
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  termAppend(html);
}

registerCmd("grid", {
  description: "Run a 2D parameter grid sweep with one displayed output",
  usage: "grid <inputX> <valsX...> vs <inputY> <valsY...> --output <name>",
  group: "sweep",
  async handler(args) {
    // Parse: <inputX> <vals...> vs <inputY> <vals...> --output <name>
    const vsIdx = args.indexOf("vs");
    if (vsIdx < 2) {
      termError("Usage: grid <inputX> <valsX...> vs <inputY> <valsY...> --output <name>");
      return;
    }

    const inputX = args[0];
    let xArgs = args.slice(1, vsIdx);
    let yArgs = args.slice(vsIdx + 1);

    // Parse --output flag from yArgs
    const outIdx = yArgs.indexOf("--output");
    if (outIdx < 0 || outIdx >= yArgs.length - 1) {
      termError("--output <name> is required. Example: grid revenue_growth 0.04 0.08 vs wacc 0.08 0.10 --output value_per_share");
      return;
    }
    const displayOutput = yArgs[outIdx + 1];
    const inputY = yArgs[0];
    let yValArgs = yArgs.slice(1, outIdx);

    // Parse X values
    let valuesX;
    const xRest = xArgs.join(" ");
    const xRange = _parseRange(xRest);
    valuesX = xRange || xArgs.map(v => { const n = parseFloat(v); return isNaN(n) ? v : n; });

    // Parse Y values
    let valuesY;
    const yRest = yValArgs.join(" ");
    const yRange = _parseRange(yRest);
    valuesY = yRange || yValArgs.map(v => { const n = parseFloat(v); return isNaN(n) ? v : n; });

    if (valuesX.length === 0 || valuesY.length === 0) {
      termError("Both axes need at least one value.");
      return;
    }

    const totalCells = valuesX.length * valuesY.length;
    if (totalCells > _GRID_MAX_CELLS) {
      termError("Grid too large: " + totalCells + " cells exceeds maximum of " + _GRID_MAX_CELLS + ". Reduce values.");
      return;
    }

    try {
      const info = await api("GET", "/project");
      const params = info.params || {};
      if (!(inputX in params)) { termError("'" + inputX + "' is not a declared parameter."); return; }
      if (!(inputY in params)) { termError("'" + inputY + "' is not a declared parameter."); return; }
      if (inputX === inputY) { termError("X and Y inputs must be different parameters."); return; }
      const origX = params[inputX];
      const origY = params[inputY];
      const t0 = performance.now();

      // Start block
      termStatus("GRID STARTED", [
        ["X", inputX + " (" + valuesX.length + " values)"],
        ["Y", inputY + " (" + valuesY.length + " values)"],
        ["output", displayOutput],
        ["points", String(totalCells)],
      ]);

      const cells = [];
      let done = 0;

      for (const x of valuesX) {
        for (const y of valuesY) {
          done++;
          termProgress(done, totalCells, inputX + "=" + _fmtVal(x) + ", " + inputY + "=" + _fmtVal(y));
          try {
            await api("POST", "/params/update", { name: inputX, value: x });
            await api("POST", "/params/update", { name: inputY, value: y });
            const buildResult = await _doCommitBuild();
            const dispVal = buildResult.scalars[displayOutput];
            cells.push({
              x: x, y: y,
              run_id: buildResult.run_id,
              status: "success",
              display_value: dispVal != null ? dispVal : null,
              outputs: buildResult.scalars,
            });
          } catch (err) {
            cells.push({
              x: x, y: y,
              run_id: null,
              status: "error",
              display_value: null,
              error: err.message,
            });
          }
        }
      }
      termProgressDone();

      // Restore
      try {
        await api("POST", "/params/update", { name: inputX, value: origX });
        await api("POST", "/params/update", { name: inputY, value: origY });
        await loadSheet();
        draw();
      } catch (_) {}

      const durationMs = Math.round(performance.now() - t0);
      const successes = cells.filter(c => c.status === "success").length;
      const failures = cells.length - successes;

      // Matrix render
      termSection(displayOutput);
      _renderGridMatrix(inputX, valuesX, inputY, valuesY, cells, displayOutput);

      // Persist
      const gridId = "grid_" + inputX + "_" + inputY + "_" + Date.now();
      const gridData = {
        grid_id: gridId,
        input_x: inputX,
        values_x: valuesX,
        input_y: inputY,
        values_y: valuesY,
        display_output: displayOutput,
        cells: cells,
        created_at: new Date().toISOString(),
        duration_ms: durationMs,
        success_count: successes,
        failure_count: failures,
      };
      try {
        await api("POST", "/grids", { grid_id: gridId, data: gridData });
      } catch (_) {}

      // Completion
      const statusLabel = failures > 0 ? "GRID COMPLETE WITH ERRORS" : "GRID COMPLETE";
      termStatus(statusLabel, [
        ["grid id", gridId],
        ["points", successes + " succeeded" + (failures ? ", " + failures + " failed" : "")],
        ["duration", (durationMs / 1000).toFixed(1) + "s"],
        ["output", displayOutput],
      ]);
      termText("show grid " + gridId + "  |  export grid " + gridId);

    } catch (err) { termError("Grid error: " + err.message); }
  }
});

registerCmd("grids", {
  description: "List saved grid sweep results",
  usage: "grids",
  group: "sweep",
  async handler() {
    try {
      const list = await api("GET", "/grids");
      if (list.length === 0) {
        termText("No grids saved. Use 'grid <inputX> <valsX> vs <inputY> <valsY> --output <name>' to run one.");
        return;
      }
      termSection("Saved Grids (" + list.length + ")");
      let html = '<table class="term-table"><thead><tr>';
      html += '<th>Grid ID</th><th>X</th><th>Y</th><th>Output</th><th>Points</th><th>Created</th>';
      html += '</tr></thead><tbody>';
      for (const g of list) {
        html += '<tr>';
        html += '<td style="font-weight:500;color:var(--fg);">' + termEsc(g.grid_id) + '</td>';
        html += '<td>' + termEsc(g.input_x || "—") + '</td>';
        html += '<td>' + termEsc(g.input_y || "—") + '</td>';
        html += '<td>' + termEsc(g.display_output || "—") + '</td>';
        html += '<td class="num-cell">' + (g.points || 0) + '</td>';
        html += '<td>' + termEsc(_fmtTs(g.created_at)) + '</td>';
        html += '</tr>';
      }
      html += '</tbody></table>';
      termAppend(html);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("show grid", {
  description: "Show a saved grid result",
  usage: "show grid <id>",
  group: "sweep",
  async handler(args) {
    if (args.length === 0) { termError("Usage: show grid <id>"); return; }
    const id = args[0];
    try {
      const data = await api("GET", "/grids/" + encodeURIComponent(id));
      const cells = data.cells || [];
      const successes = cells.filter(c => c.status === "success").length;
      const failures = cells.length - successes;

      termCard("Grid: " + (data.grid_id || id), [
        ["X", (data.input_x || "—") + " (" + (data.values_x || []).length + " values)"],
        ["Y", (data.input_y || "—") + " (" + (data.values_y || []).length + " values)"],
        ["output", data.display_output || "—"],
        ["points", successes + " succeeded" + (failures ? ", " + failures + " failed" : "")],
        ["created", _fmtTs(data.created_at)],
      ]);

      termSection(data.display_output || "Results");
      _renderGridMatrix(data.input_x, data.values_x || [], data.input_y, data.values_y || [], cells, data.display_output);

      termText("Export: export grid " + (data.grid_id || id));
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("export grid", {
  description: "Export a grid result to CSV",
  usage: "export grid <id>",
  group: "sweep",
  async handler(args) {
    if (args.length === 0) { termError("Usage: export grid <id>"); return; }
    const id = args[0];
    try {
      const url = "/api/grids/" + encodeURIComponent(id) + "/csv";
      const resp = await fetch(url);
      if (!resp.ok) {
        const text = await resp.text();
        termError("Export failed: " + text);
        return;
      }
      const blob = await resp.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = id + ".csv";
      a.click();
      URL.revokeObjectURL(a.href);
      termStatus("Export Complete", [
        ["file", id + ".csv"],
        ["size", (blob.size / 1024).toFixed(1) + " KB"],
        ["format", "CSV (tidy long-form)"],
      ]);
    } catch (err) { termError("Export failed: " + err.message); }
  }
});

// ═══════════════════════════════════════════════
// AI Workbench Commands
// ═══════════════════════════════════════════════

registerCmd("ai explain", {
  description: "Explain a formula or output using AI",
  usage: "ai explain formula <ref> | ai explain output <name>",
  group: "ai",
  async handler(args) {
    if (args.length < 2) {
      termError("Usage: ai explain formula <ref> | ai explain output <name>");
      return;
    }
    const kind = args[0].toLowerCase();
    const target = args[1];

    if (kind !== "formula" && kind !== "output") {
      termError("Usage: ai explain formula <ref> | ai explain output <name>");
      return;
    }

    termText("Asking AI to explain " + kind + " '" + target + "'...");
    try {
      const body = { kind };
      if (kind === "formula") {
        body.ref = target;
      } else {
        body.name = target;
      }
      const result = await api("POST", "/ai/explain", body);
      if (!result.ok) {
        termError(result.error || "Explain failed");
        return;
      }
      const content = result.content || "(no explanation returned)";
      const contentLines = content.split("\n");
      const truncated = contentLines.length > _TRUNCATE_LINES;
      const displayContent = truncated ? contentLines.slice(0, _TRUNCATE_LINES).join("\n") : content;
      _lastAiContent = { content, kind, target, provider: result.provider, model: result.model };

      let html = '<div class="term-ai-explain">';
      html += '<div class="term-ai-header">';
      html += '<span class="term-ai-label">AI Explain</span>';
      html += '<span class="term-ai-target">' + termEsc(kind) + ': ' + termEsc(target) + '</span>';
      html += '<span class="term-ai-meta">' + termEsc(result.provider || "") + ' / ' + termEsc(result.model || "") + '</span>';
      html += '</div>';
      html += '<div class="term-ai-body">' + termEsc(displayContent) + '</div>';
      if (truncated) {
        html += '<div class="term-ai-footer">truncated (' + contentLines.length + ' lines) — use <strong>show full last</strong> to expand</div>';
      } else {
        html += '<div class="term-ai-footer">AI-generated — verify independently</div>';
      }
      html += '</div>';
      termAppend(html);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("ai draft addin", {
  description: "Draft an add-in plugin using AI",
  usage: 'ai draft addin "<description>"',
  group: "ai",
  async handler(args) {
    if (args.length === 0) {
      termError('Usage: ai draft addin "<description>"');
      return;
    }
    const description = args.join(" ");
    termText("Generating add-in draft: " + description + "...");
    try {
      const result = await api("POST", "/ai/draft", { description });
      if (!result.ok) {
        termError(result.error || "Draft generation failed");
        return;
      }
      const did = result.draft_id;
      let html = '<div class="term-ai-explain">';
      html += '<div class="term-ai-header">';
      html += '<span class="term-ai-label">Draft Created</span>';
      html += '<span class="term-draft-badge">' + termEsc(did) + '</span>';
      html += '<span class="term-ai-meta">' + termEsc(result.provider || "") + ' / ' + termEsc(result.model || "") + '</span>';
      html += '</div>';
      html += '<div class="term-card-row"><span class="term-card-key">code</span><span class="term-card-val">' + (result.detected_code_lines || "?") + ' lines</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">type</span><span class="term-card-val">scalar_plugin</span></div>';
      html += '</div>';
      termAppend(html);
      // Next steps
      let ns = '<div class="term-next-steps">';
      ns += '<div class="term-step"><span class="term-step-cmd">draft show ' + termEsc(did) + '</span> <span class="term-step-desc">— review generated code</span></div>';
      ns += '<div class="term-step"><span class="term-step-cmd">validate draft ' + termEsc(did) + '</span> <span class="term-step-desc">— run policy validation</span></div>';
      ns += '<div class="term-step"><span class="term-step-cmd">apply draft ' + termEsc(did) + '</span> <span class="term-step-desc">— apply to plugins/ (after validation)</span></div>';
      ns += '</div>';
      termAppend(ns);
      termAppend('<div class="term-ai-footer" style="margin-top:2px;">AI-generated code — review and validate before applying</div>');
    } catch (err) { termError("Failed: " + err.message); }
  }
});

// Shared draft list renderer
async function _renderDraftList() {
  const drafts = await api("GET", "/drafts");
  if (drafts.length === 0) {
    termText("No drafts. Use 'ai draft addin \"<description>\"' to create one.");
    return;
  }
  termSection("Drafts (" + drafts.length + ")");
  let html = '<table class="term-table"><thead><tr>';
  html += '<th>Draft</th><th>Status</th><th>Type</th><th>Updated</th><th>Prompt</th>';
  html += '</tr></thead><tbody>';
  for (const d of drafts) {
    const statusCls = "status-" + (d.status || "draft");
    const promptPreview = (d.prompt || "").substring(0, 35) + ((d.prompt || "").length > 35 ? "..." : "");
    html += '<tr>';
    html += '<td><span class="term-draft-badge">' + termEsc(d.draft_id) + '</span></td>';
    html += '<td><span class="term-status-badge ' + statusCls + '">' + termEsc(d.status || "draft") + '</span></td>';
    html += '<td style="color:var(--fg-dim);">' + termEsc(d.artifact_type || "—") + '</td>';
    html += '<td>' + termEsc(_fmtTs(d.updated_at || d.created_at)) + '</td>';
    html += '<td style="color:var(--fg-dim);font-size:9px;">' + termEsc(promptPreview) + '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  termAppend(html);
}

registerCmd("draft list", {
  description: "List all draft artifacts",
  usage: "draft list",
  group: "draft",
  async handler() {
    try { await _renderDraftList(); }
    catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("ai list drafts", {
  description: "List all draft artifacts (alias for draft list)",
  usage: "ai list drafts",
  group: "ai",
  hidden: true,
  async handler() {
    try { await _renderDraftList(); }
    catch (err) { termError("Failed: " + err.message); }
  }
});

// Shared validation renderer
function _renderValidation(v) {
  const errors = v.errors || v.violations || [];
  const warnings = v.warnings || [];
  termSection("Validation");
  let vhtml = '<div class="term-validation-result ' + (v.valid ? "valid" : "invalid") + '">';
  vhtml += '<div class="term-validation-header">';
  vhtml += '<span class="term-validation-status ' + (v.valid ? "pass" : "fail") + '">' + (v.valid ? "PASS" : "FAIL") + '</span>';
  if (v.detected_type) vhtml += '<span style="color:var(--fg-dim);font-size:9px;">type: ' + termEsc(v.detected_type) + '</span>';
  if (v.registered_names && v.registered_names.length) {
    vhtml += '<span style="color:var(--fg-dim);font-size:9px;">registers: ' + termEsc(v.registered_names.join(", ")) + '</span>';
  }
  vhtml += '</div>';
  for (const e of errors) {
    vhtml += '<div class="term-violation-item viol-error"><span class="viol-field">' + termEsc(e.field || "?") + '</span> <span class="viol-reason">' + termEsc(e.reason || "?") + '</span></div>';
  }
  for (const w of warnings) {
    vhtml += '<div class="term-violation-item viol-warning"><span class="viol-field">' + termEsc(w.field || "?") + '</span> <span class="viol-reason">' + termEsc(w.reason || "?") + '</span></div>';
  }
  vhtml += '</div>';
  termAppend(vhtml);
}

registerCmd("draft show", {
  description: "Show a draft artifact's metadata and code",
  usage: "draft show <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft show <id>"); return; }
    const id = args[0];
    try {
      const d = await api("GET", "/drafts/" + encodeURIComponent(id));
      const statusCls = "status-" + (d.status || "draft");

      // Identity card
      let html = '<div class="term-card">';
      html += '<div class="term-card-title"><span class="term-draft-badge">' + termEsc(d.draft_id) + '</span> <span class="term-status-badge ' + statusCls + '">' + termEsc(d.status || "draft") + '</span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">type</span><span class="term-card-val">' + termEsc(d.artifact_type || "—") + '</span></div>';
      if (d.model || d.provider) {
        html += '<div class="term-card-row"><span class="term-card-key">model</span><span class="term-card-val">' + termEsc((d.provider || "") + (d.provider && d.model ? " / " : "") + (d.model || "")) + '</span></div>';
      }
      html += '<div class="term-card-row"><span class="term-card-key">created</span><span class="term-card-val">' + termEsc(_fmtTs(d.created_at)) + '</span></div>';
      if (d.derived_from) {
        html += '<div class="term-card-row"><span class="term-card-key">revision of</span><span class="term-card-val"><span class="term-draft-badge">' + termEsc(d.derived_from) + '</span></span></div>';
      }
      if (d.applied_path) {
        html += '<div class="term-card-row"><span class="term-card-key">applied to</span><span class="term-card-val">' + termEsc(d.applied_path) + '</span></div>';
      }
      html += '</div>';
      termAppend(html);

      // Prompt
      if (d.prompt) {
        termSection("Prompt");
        termAppend('<div class="term-text" style="font-style:italic;">' + termEsc(d.prompt) + '</div>');
      }

      // Validation result
      if (d.validation) {
        _renderValidation(d.validation);
      }

      // Code (truncated preview)
      if (d.code) {
        const codeLines = d.code.split("\n");
        const maxLines = 40;
        const codeTruncated = codeLines.length > maxLines;
        const displayCode = codeTruncated ? codeLines.slice(0, maxLines).join("\n") : d.code;
        termSection("Code" + (codeTruncated ? " (first " + maxLines + " of " + codeLines.length + " lines)" : ""));
        termAppend('<pre class="term-code-block">' + termEsc(displayCode) + '</pre>');
        if (codeTruncated) {
          termText("Use 'draft show full " + id + "' to see complete code");
        }
      }
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("draft diff", {
  description: "Show the generated code for a draft",
  usage: "draft diff <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft diff <id>"); return; }
    const id = args[0];
    try {
      const d = await api("GET", "/drafts/" + encodeURIComponent(id));
      const statusCls = "status-" + (d.status || "draft");
      termAppend('<div class="term-diff-header"><span class="term-draft-badge">' + termEsc(d.draft_id) + '</span> <span class="term-status-badge ' + statusCls + '">' + termEsc(d.status || "draft") + '</span> <span style="color:var(--fg-dim);font-size:9px;">' + termEsc(d.artifact_type || "plugin") + '</span></div>');
      if (d.code) {
        termAppend('<pre class="term-code-block">' + termEsc(d.code) + '</pre>');
      } else {
        termText("(no code in draft)");
      }
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("draft delete", {
  description: "Delete a draft artifact",
  usage: "draft delete <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft delete <id>"); return; }
    const id = args[0];
    try {
      await api("DELETE", "/drafts/" + encodeURIComponent(id));
      termSuccess("Draft '" + id + "' deleted.");
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("validate draft", {
  description: "Run policy validation on a draft artifact",
  usage: "validate draft <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: validate draft <id>"); return; }
    const id = args[0];
    try {
      const result = await api("POST", "/drafts/" + encodeURIComponent(id) + "/validate");
      termAppend('<div style="margin-bottom:2px;"><span class="term-draft-badge">' + termEsc(id) + '</span></div>');
      _renderValidation(result);

      if (result.valid) {
        let ns = '<div class="term-next-steps">';
        ns += '<div class="term-step"><span class="term-step-cmd">apply draft ' + termEsc(id) + '</span> <span class="term-step-desc">— copy to plugins/ directory</span></div>';
        ns += '<div class="term-step"><span class="term-step-cmd">commit</span> <span class="term-step-desc">— build workbook with new plugin</span></div>';
        ns += '</div>';
        termAppend(ns);
      }
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("apply draft", {
  description: "Apply a validated draft to the plugins/ directory",
  usage: "apply draft <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: apply draft <id>"); return; }
    const id = args[0];
    try {
      const result = await api("POST", "/drafts/" + encodeURIComponent(id) + "/apply");
      S.dirty = true;
      updateStatus();
      termStatus("Draft Applied", [
        ["draft", id],
        ["applied to", result.applied_path || "—"],
        ["status", "applied — workbook marked dirty"],
      ]);
      let ns = '<div class="term-next-steps">';
      ns += '<div class="term-step"><span class="term-step-cmd">commit</span> <span class="term-step-desc">— build workbook with new plugin</span></div>';
      ns += '</div>';
      termAppend(ns);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("reject draft", {
  description: "Reject a draft artifact (marks as rejected, does not delete)",
  usage: "reject draft <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: reject draft <id>"); return; }
    const id = args[0];
    try {
      await api("POST", "/drafts/" + encodeURIComponent(id) + "/reject");
      termSuccess("Draft '" + id + "' rejected.");
    } catch (err) { termError("Failed: " + err.message); }
  }
});

// ── Iteration + recall commands ──

registerCmd("ai revise draft", {
  description: "Create a revised draft by asking AI to modify an existing one",
  usage: 'ai revise draft <id> "<instruction>"',
  group: "ai",
  async handler(args) {
    if (args.length < 2) {
      termError('Usage: ai revise draft <id> "<instruction>"');
      return;
    }
    const parentId = args[0];
    const instruction = args.slice(1).join(" ");
    termText("Revising " + parentId + ": " + instruction + "...");
    try {
      const result = await api("POST", "/ai/revise", { draft_id: parentId, instruction });
      if (!result.ok) {
        termError(result.error || "Revision failed");
        return;
      }
      const did = result.draft_id;
      let html = '<div class="term-ai-explain">';
      html += '<div class="term-ai-header">';
      html += '<span class="term-ai-label">Draft Revised</span>';
      html += '<span class="term-draft-badge">' + termEsc(did) + '</span>';
      html += '<span class="term-ai-meta">' + termEsc(result.provider || "") + ' / ' + termEsc(result.model || "") + '</span>';
      html += '</div>';
      html += '<div class="term-card-row"><span class="term-card-key">revision of</span><span class="term-card-val"><span class="term-draft-badge">' + termEsc(parentId) + '</span></span></div>';
      html += '<div class="term-card-row"><span class="term-card-key">code</span><span class="term-card-val">' + (result.detected_code_lines || "?") + ' lines</span></div>';
      html += '</div>';
      termAppend(html);
      let ns = '<div class="term-next-steps">';
      ns += '<div class="term-step"><span class="term-step-cmd">draft show ' + termEsc(did) + '</span> <span class="term-step-desc">— review revised code</span></div>';
      ns += '<div class="term-step"><span class="term-step-cmd">validate draft ' + termEsc(did) + '</span> <span class="term-step-desc">— run policy validation</span></div>';
      ns += '<div class="term-step"><span class="term-step-cmd">draft lineage ' + termEsc(did) + '</span> <span class="term-step-desc">— view revision chain</span></div>';
      ns += '</div>';
      termAppend(ns);
      termAppend('<div class="term-ai-footer" style="margin-top:2px;">AI-generated revision — review and validate before applying</div>');
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("draft lineage", {
  description: "Show the revision chain for a draft",
  usage: "draft lineage <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft lineage <id>"); return; }
    const id = args[0];
    try {
      // Load all drafts and build the chain
      const drafts = await api("GET", "/drafts");
      const byId = {};
      for (const d of drafts) byId[d.draft_id] = d;

      // Walk backward from target to root
      const chain = [];
      let current = id;
      const seen = new Set();
      while (current && byId[current] && !seen.has(current)) {
        seen.add(current);
        chain.unshift(current);
        current = byId[current].derived_from;
      }

      if (chain.length === 0) {
        termError("Draft '" + id + "' not found.");
        return;
      }

      // Also walk forward from the target to find descendants
      const children = {};
      for (const d of drafts) {
        if (d.derived_from) {
          if (!children[d.derived_from]) children[d.derived_from] = [];
          children[d.derived_from].push(d.draft_id);
        }
      }
      let tip = id;
      while (children[tip] && children[tip].length > 0) {
        tip = children[tip][0]; // follow first child
        if (seen.has(tip)) break;
        seen.add(tip);
        chain.push(tip);
      }

      termSection("Draft Lineage");
      let html = '<div style="padding:4px 0;">';
      for (let i = 0; i < chain.length; i++) {
        const cid = chain[i];
        const d = byId[cid];
        const isCurrent = cid === id;
        const statusCls = "status-" + (d ? (d.status || "draft") : "draft");
        html += '<span class="term-draft-badge" style="' + (isCurrent ? 'outline:2px solid var(--accent);outline-offset:1px;' : '') + '">' + termEsc(cid) + '</span>';
        if (d) html += ' <span class="term-status-badge ' + statusCls + '">' + termEsc(d.status || "draft") + '</span>';
        if (i < chain.length - 1) html += ' <span style="color:var(--fg-dim);margin:0 4px;">→</span> ';
      }
      html += '</div>';
      termAppend(html);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("draft show full", {
  description: "Show a draft's complete code without truncation",
  usage: "draft show full <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft show full <id>"); return; }
    const id = args[0];
    try {
      const d = await api("GET", "/drafts/" + encodeURIComponent(id));
      if (d.code) {
        const lines = d.code.split("\n").length;
        termSection("Full Code: " + d.draft_id + " (" + lines + " lines)");
        termAppend('<pre class="term-code-block" style="max-height:none;">' + termEsc(d.code) + '</pre>');
      } else {
        termText("(no code in draft)");
      }
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("draft validation", {
  description: "Recall the stored validation result for a draft (does not rerun validation)",
  usage: "draft validation <id>",
  group: "draft",
  async handler(args) {
    if (args.length === 0) { termError("Usage: draft validation <id>"); return; }
    const id = args[0];
    try {
      const d = await api("GET", "/drafts/" + encodeURIComponent(id));
      if (!d.validation) {
        termText("No validation result stored for " + id + ". Run 'validate draft " + id + "' first.");
        return;
      }
      termAppend('<div style="margin-bottom:2px;"><span class="term-draft-badge">' + termEsc(id) + '</span> <span style="color:var(--fg-dim);font-size:9px;">stored validation result</span></div>');
      _renderValidation(d.validation);
    } catch (err) { termError("Failed: " + err.message); }
  }
});

registerCmd("show full last", {
  description: "Show the full content of the last truncated AI explanation",
  usage: "show full last",
  group: "ai",
  handler() {
    if (!_lastAiContent) {
      termText("No recent AI explanation to expand.");
      return;
    }
    const c = _lastAiContent;
    let html = '<div class="term-ai-explain">';
    html += '<div class="term-ai-header">';
    html += '<span class="term-ai-label">AI Explain (full)</span>';
    html += '<span class="term-ai-target">' + termEsc(c.kind || "") + ': ' + termEsc(c.target || "") + '</span>';
    html += '<span class="term-ai-meta">' + termEsc(c.provider || "") + ' / ' + termEsc(c.model || "") + '</span>';
    html += '</div>';
    html += '<div class="term-ai-body">' + termEsc(c.content) + '</div>';
    html += '<div class="term-ai-footer">AI-generated — verify independently</div>';
    html += '</div>';
    termAppend(html);
  }
});

// ── Init ──
window.addEventListener("resize", resizeCanvas);

(async function init() {
  // Apply saved mode
  setMode(S.uiMode);

  // Force immediate resize (no debounce on first paint)
  const container = document.getElementById("grid-container");
  canvas.width = container.clientWidth;
  canvas.height = container.clientHeight;

  await loadProject();
  await loadSheet();
  await loadScalars();
  await loadRuns();
  await loadSnapshots();
  await loadTableList();
  await loadDatasheets();
  await loadNames();
  await loadModelInfo();
  await loadModelVersions();
  await loadImportReport();
  await loadHealth();
  await loadRegistryStatus();
  setupLogControls();
  loadLogs();
  loadChecks();
  loadIncidents();
  loadStatus();
  startStatusPolling();
  renderErrors();
  updateCellInfo();
  canvas.focus();
  draw();
})();
