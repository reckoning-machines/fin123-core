/**
 * worksheet_viewer.js -- DOM renderer for CompiledWorksheet artifacts.
 *
 * Read-only. No sorting. No filtering. No editing. No charts.
 * No framework. No build step. No external dependencies.
 *
 * API:
 *   WorksheetViewer.render(containerElement, compiledWorksheetObject)
 *
 * The caller is responsible for JSON.parse(). This module receives
 * the parsed CompiledWorksheet object directly.
 */

/* global window, document */

var WorksheetViewer = (function () {
  "use strict";

  // ────────────────────────────────────────────────────────────────
  // Display format helpers
  // ────────────────────────────────────────────────────────────────

  /**
   * Apply a display_format spec to a raw value.
   * Returns a formatted string, or null if no format applies.
   */
  function applyDisplayFormat(value, displayFormat) {
    if (value == null || displayFormat == null) return null;
    var type = displayFormat.type;
    var places = displayFormat.places;

    if (type === "decimal" || type === "currency") {
      var num = Number(value);
      if (isNaN(num)) return null;
      var formatted = places != null ? num.toFixed(places) : String(num);
      // Add thousands separators
      var parts = formatted.split(".");
      parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");
      formatted = parts.join(".");
      if (type === "currency") {
        var symbol = displayFormat.symbol || "$";
        // Handle negative currency
        if (num < 0) {
          return "(" + symbol + formatted.replace("-", "") + ")";
        }
        return symbol + formatted;
      }
      return formatted;
    }

    if (type === "percent") {
      var pNum = Number(value);
      if (isNaN(pNum)) return null;
      var pVal = pNum * 100;
      return (places != null ? pVal.toFixed(places) : String(pVal)) + "%";
    }

    if (type === "integer") {
      var iNum = Number(value);
      if (isNaN(iNum)) return null;
      var iStr = Math.round(iNum).toString();
      return iStr.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    if (type === "date") {
      // Use date_format hint if present, otherwise pass through
      return String(value);
    }

    if (type === "text") {
      return String(value);
    }

    return null;
  }

  // ────────────────────────────────────────────────────────────────
  // DOM helpers
  // ────────────────────────────────────────────────────────────────

  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    if (attrs) {
      for (var key in attrs) {
        if (!attrs.hasOwnProperty(key)) continue;
        if (key === "textContent") {
          node.textContent = attrs[key];
        } else if (key === "className") {
          node.className = attrs[key];
        } else if (key === "innerHTML") {
          node.innerHTML = attrs[key];
        } else {
          node.setAttribute(key, attrs[key]);
        }
      }
    }
    if (children) {
      for (var i = 0; i < children.length; i++) {
        if (typeof children[i] === "string") {
          node.appendChild(document.createTextNode(children[i]));
        } else if (children[i]) {
          node.appendChild(children[i]);
        }
      }
    }
    return node;
  }

  function text(str) {
    return document.createTextNode(str);
  }

  // ────────────────────────────────────────────────────────────────
  // Escape for safe text display
  // ────────────────────────────────────────────────────────────────

  function escapeText(val) {
    if (val == null) return "";
    return String(val);
  }

  // ────────────────────────────────────────────────────────────────
  // Check if a value is an inline error object
  // ────────────────────────────────────────────────────────────────

  function isErrorValue(val) {
    return val != null && typeof val === "object" && val.error != null;
  }

  // ────────────────────────────────────────────────────────────────
  // Build column lookup maps
  // ────────────────────────────────────────────────────────────────

  function buildColumnMap(columns) {
    var map = {};
    for (var i = 0; i < columns.length; i++) {
      map[columns[i].name] = columns[i];
    }
    return map;
  }

  function buildSortMap(sorts) {
    var map = {};
    if (!sorts) return map;
    for (var i = 0; i < sorts.length; i++) {
      map[sorts[i].column] = sorts[i].descending ? "desc" : "asc";
    }
    return map;
  }

  // ────────────────────────────────────────────────────────────────
  // Grouped header computation
  // ────────────────────────────────────────────────────────────────

  /**
   * Compute the grouped header row cells.
   * Returns array of { label, colspan } objects covering all output columns.
   * Ungrouped columns get label="" and colspan=1.
   */
  function computeGroupedHeaders(columns, headerGroups) {
    if (!headerGroups || headerGroups.length === 0) return null;

    // Map each column name to its group label
    var colToGroup = {};
    for (var g = 0; g < headerGroups.length; g++) {
      var group = headerGroups[g];
      for (var c = 0; c < group.columns.length; c++) {
        colToGroup[group.columns[c]] = group.label;
      }
    }

    // Walk columns in order, merging consecutive same-group columns
    var cells = [];
    var i = 0;
    while (i < columns.length) {
      var colName = columns[i].name;
      var groupLabel = colToGroup[colName] || "";
      var span = 1;

      // Merge consecutive columns with the same group
      while (
        i + span < columns.length &&
        (colToGroup[columns[i + span].name] || "") === groupLabel
      ) {
        span++;
      }

      cells.push({ label: groupLabel, colspan: span });
      i += span;
    }

    return cells;
  }

  // ────────────────────────────────────────────────────────────────
  // Render: error summary banner
  // ────────────────────────────────────────────────────────────────

  function renderErrorSummary(ws) {
    if (!ws.error_summary) return null;

    var summary = ws.error_summary;
    var container = el("div", {
      className: "ws-error-summary",
      role: "alert",
      "aria-live": "polite",
    });

    var title = el("div", { className: "ws-error-summary-title" }, [
      text(summary.total_errors + " error" + (summary.total_errors !== 1 ? "s" : "") + " in worksheet"),
    ]);
    container.appendChild(title);

    if (summary.by_column) {
      var parts = [];
      for (var col in summary.by_column) {
        if (summary.by_column.hasOwnProperty(col)) {
          parts.push(col + ": " + summary.by_column[col]);
        }
      }
      if (parts.length > 0) {
        var detail = el("div", { className: "ws-error-summary-detail" }, [
          text("By column: " + parts.join(", ")),
        ]);
        container.appendChild(detail);
      }
    }

    return container;
  }

  // ────────────────────────────────────────────────────────────────
  // Render: table
  // ────────────────────────────────────────────────────────────────

  function renderTable(ws) {
    var columns = ws.columns || [];
    var rows = ws.rows || [];
    var flags = ws.flags || [];
    var colMap = buildColumnMap(columns);
    var sortMap = buildSortMap(ws.sorts);
    var hasFlags = false;

    // Check if any row has flags
    for (var fi = 0; fi < flags.length; fi++) {
      if (flags[fi] && flags[fi].length > 0) {
        hasFlags = true;
        break;
      }
    }

    var groupedHeaders = computeGroupedHeaders(columns, ws.header_groups);

    var table = el("table", { className: "ws-table", role: "table" });

    // ── Thead ──
    var thead = el("thead");

    // Grouped header row
    if (groupedHeaders) {
      var groupRow = el("tr", { className: "ws-header-group-row" });
      if (hasFlags) {
        groupRow.appendChild(el("th", { className: "ws-group-spacer", rowspan: "1" }));
      }
      for (var gi = 0; gi < groupedHeaders.length; gi++) {
        var gh = groupedHeaders[gi];
        var ghAttrs = { scope: "colgroup", colspan: String(gh.colspan) };
        if (!gh.label) {
          ghAttrs.className = "ws-group-spacer";
        }
        groupRow.appendChild(el("th", ghAttrs, [text(gh.label)]));
      }
      thead.appendChild(groupRow);
    }

    // Column header row
    var headerRow = el("tr", { className: "ws-header-row" });
    if (hasFlags) {
      var flagHeader = el("th", { scope: "col", "aria-label": "Flags" });
      headerRow.appendChild(flagHeader);
    }
    for (var ci = 0; ci < columns.length; ci++) {
      var col = columns[ci];
      var thAttrs = {
        scope: "col",
        "data-column": col.name,
        "data-type": col.column_type,
      };

      var sortDir = sortMap[col.name];
      if (sortDir) {
        thAttrs["aria-sort"] = sortDir === "asc" ? "ascending" : "descending";
      }

      var th = el("th", thAttrs, [text(col.label)]);

      // Sort indicator
      if (sortDir) {
        var arrow = sortDir === "asc" ? "\u25B2" : "\u25BC";
        th.appendChild(el("span", { className: "ws-sort-indicator", "aria-hidden": "true" }, [text(arrow)]));
      }

      headerRow.appendChild(th);
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // ── Tbody ──
    var tbody = el("tbody");

    for (var ri = 0; ri < rows.length; ri++) {
      var row = rows[ri];
      var rowFlags = flags[ri] || [];
      var tr = el("tr", { className: "ws-row", "data-row-index": String(ri) });

      // Flag cell
      if (hasFlags) {
        var flagCell = el("td", { className: "ws-flag-cell" });
        for (var fli = 0; fli < rowFlags.length; fli++) {
          var flag = rowFlags[fli];
          var flagTitle = flag.name;
          if (flag.message) flagTitle += ": " + flag.message;
          var flagDot = el("span", {
            className: "ws-flag ws-flag--" + flag.severity,
            title: flagTitle,
            role: "img",
            "aria-label": flag.severity + " flag: " + flagTitle,
          });
          flagCell.appendChild(flagDot);
        }
        tr.appendChild(flagCell);
      }

      // Data cells — pass cell-level metadata if ws.cell_meta is present.
      // Expected shape:
      //   ws.cell_meta[rowIndex][colName] = {
      //     semantic_role: "label" | "input" | "formula" | "lookup" | "meta",
      //     key_output:    boolean,
      //     editable:      boolean
      //   }
      // All fields optional. Overrides column-level and heuristic inference.
      var rowMeta = (ws.cell_meta && ws.cell_meta[ri]) ? ws.cell_meta[ri] : null;
      for (var di = 0; di < columns.length; di++) {
        var colDef = columns[di];
        var cellValue = row[colDef.name];
        var cellMeta = rowMeta ? (rowMeta[colDef.name] || null) : null;
        var td = renderCell(cellValue, colDef, cellMeta);
        tr.appendChild(td);
      }

      tbody.appendChild(tr);
    }

    table.appendChild(tbody);

    // ── Set sticky top offsets ──
    // After building the table, we need to set top offsets for stacking.
    // Group row = top:0, header row = top:<group row height>.
    // Actual heights are set after DOM insertion via a post-render step.

    return { table: table, hasGroupRow: !!groupedHeaders };
  }

  // ────────────────────────────────────────────────────────────────
  // Heuristic helpers (isolated fallbacks — each narrowly named)
  //
  // These are temporary inference rules used when upstream metadata
  // (source_kind, key_output, editable) is not yet emitted.
  // Each is self-contained so it can be removed once the compile
  // pipeline provides explicit semantic fields.
  // ────────────────────────────────────────────────────────────────

  /** Temporary: column-name pattern → label role. */
  var _LABEL_NAME_RE = /^(name|label|ticker|symbol|description|category|sector|id|item|metric|line_item)$/i;

  function inferLabelRoleFromName(colDef) {
    var t = colDef.column_type;
    if (t === "str" || t === "object" || t === "string") return true;
    return _LABEL_NAME_RE.test(colDef.name);
  }

  /** Temporary: column-name pattern → key-output emphasis. */
  var _KEY_OUTPUT_NAME_RE = /^(value_per_share|implied_upside|nav_per_share|total_return|irr|npv|wacc|enterprise_value|equity_value|share_price|target_price|fair_value)$/i;

  function inferKeyOutputFromName(colName) {
    return _KEY_OUTPUT_NAME_RE.test(colName);
  }

  /**
   * Temporary: infer semantic role from display_format shape.
   * Used only when no explicit source_kind is available.
   *   text/date format → "lookup" (reference fetch origin)
   *   numeric format   → "formula" (computed value)
   */
  var _LOOKUP_FMT_TYPES = /^(text|date)$/;

  function inferSemanticRoleFromDisplayFormat(fmt) {
    if (!fmt || !fmt.type) return null;
    return _LOOKUP_FMT_TYPES.test(fmt.type) ? "lookup" : "formula";
  }

  /**
   * Value-type inference.
   * Uses column_type for boolean/date, then rendered text for number/text.
   * Handles null/error for contract completeness even though the renderer
   * typically handles those before reaching the classifier.
   */
  var _NUMERIC_RE = /^[\(\-]?\$?[\d,]+\.?\d*%?\)?$/;

  function inferValueType(rawValue, colDef, displayText) {
    if (rawValue == null) return "blank";
    if (isErrorValue(rawValue)) return "error";
    var ct = colDef.column_type;
    if (ct === "bool") return "boolean";
    if (ct === "date" || ct === "datetime") return "date";
    var trimmed = displayText ? displayText.trim() : "";
    if (!trimmed) return "blank";
    return _NUMERIC_RE.test(trimmed) ? "number" : "text";
  }

  /** Density inference from rendered text length. */
  function inferDensityHint(displayText) {
    var len = displayText ? displayText.trim().length : 0;
    if (len >= 14) return "dense";
    if (len >= 10) return "medium";
    return "normal";
  }

  // ────────────────────────────────────────────────────────────────
  // Normalized semantic contract
  //
  // Produces a stable descriptor shape:
  //   { semantic_role, key_output, editable, value_type, density_hint }
  //
  // semantic_role: "label" | "input" | "formula" | "lookup" | "meta" | null
  //   "meta" is reserved for upstream use (e.g. provenance rows);
  //   not yet emitted by any heuristic — included so the vocabulary
  //   is stable when upstream begins tagging cells.
  //
  // editable: not yet mapped to CSS classes. Preserved in the
  //   normalized contract for future renderer affordances (e.g.
  //   editable-cell highlight, input cursor change).
  //
  // Precedence (highest → lowest):
  //   1. Cell-level metadata   (cellMeta.semantic_role, etc.)
  //   2. Column-level metadata (colDef.source_kind, colDef.key_output)
  //   3. Heuristic fallbacks   (name patterns, display_format inference)
  //   4. Value type            (boolean/date from column_type, then
  //                             number/text/blank from rendered text)
  // ────────────────────────────────────────────────────────────────

  function normalizeCellSemantics(rawValue, cellMeta, colDef, displayText) {
    var sem = {
      semantic_role: null,
      key_output:    false,
      editable:      false,
      value_type:    "text",   // "text" | "number" | "date" | "boolean" | "error" | "blank"
      density_hint:  "normal"  // "normal" | "medium" | "dense"
    };

    // ── 1. Cell-level metadata (highest priority) ──
    if (cellMeta) {
      if (cellMeta.semantic_role) sem.semantic_role = cellMeta.semantic_role;
      if (cellMeta.key_output === true) sem.key_output = true;
      if (cellMeta.editable === true)   sem.editable = true;
    }

    // ── 2. Column-level metadata ──
    if (!sem.semantic_role && colDef.source_kind) {
      sem.semantic_role = colDef.source_kind;
    }
    if (!sem.key_output && colDef.key_output === true) {
      sem.key_output = true;
    }
    if (!sem.editable && colDef.editable === true) {
      sem.editable = true;
    }

    // ── 3. Heuristic fallbacks (only when no metadata resolved role) ──
    if (!sem.semantic_role) {
      if (inferLabelRoleFromName(colDef)) {
        sem.semantic_role = "label";
      } else if (!colDef.display_format && (colDef.column_type === "float64" || colDef.column_type === "int64")) {
        sem.semantic_role = "input";
      } else {
        sem.semantic_role = inferSemanticRoleFromDisplayFormat(colDef.display_format);
      }
    }
    if (!sem.key_output) {
      sem.key_output = inferKeyOutputFromName(colDef.name);
    }

    // ── 4. Value type (column_type then rendered text) and density ──
    sem.value_type = inferValueType(rawValue, colDef, displayText);
    sem.density_hint = inferDensityHint(displayText);

    return sem;
  }

  /**
   * Map a normalized semantic descriptor to CSS class suffixes.
   * Returns array of strings (without "ws-cell--" prefix).
   *
   * Not mapped to classes (intentionally):
   *   sem.editable  — preserved for future renderer affordances
   *                   (e.g. editable-cell border, cursor style)
   *   sem.semantic_role === "meta" — reserved vocabulary, not yet
   *                   emitted upstream; will need a CSS rule when used
   */
  function classesFromSemantics(sem) {
    var classes = [];

    // Source role (mutually exclusive)
    if (sem.semantic_role) classes.push(sem.semantic_role);

    // Value type (independent of role)
    if (sem.value_type === "number")  classes.push("numeric");
    else if (sem.value_type === "text")    classes.push("text");
    else if (sem.value_type === "blank")   classes.push("blank");

    // Emphasis (independent of role)
    if (sem.key_output) classes.push("key-output");

    // Density
    if (sem.density_hint === "dense")  classes.push("dense");
    else if (sem.density_hint === "medium") classes.push("dense-md");

    return classes;
  }

  // ────────────────────────────────────────────────────────────────
  // Render: individual cell
  // ────────────────────────────────────────────────────────────────

  function renderCell(value, colDef, cellMeta) {
    var attrs = { "data-column": colDef.name };

    // Null
    if (value == null) {
      attrs.className = "ws-cell ws-cell--null ws-cell--" + colDef.column_type;
      return el("td", attrs, [text("\u2014")]);
    }

    // Error object
    if (isErrorValue(value)) {
      attrs.className = "ws-cell ws-cell--error";
      attrs.role = "status";
      attrs["aria-label"] = "Error: " + value.error;
      return el("td", attrs, [text(value.error)]);
    }

    // Format display value
    var displayText;
    if (colDef.display_format) {
      displayText = applyDisplayFormat(value, colDef.display_format);
    }
    if (displayText == null) {
      displayText = defaultFormat(value, colDef.column_type);
    }

    // Normalize semantics → CSS classes
    var sem = normalizeCellSemantics(value, cellMeta || null, colDef, displayText);
    var semClasses = classesFromSemantics(sem);
    var classList = "ws-cell ws-cell--" + colDef.column_type;
    for (var i = 0; i < semClasses.length; i++) {
      classList += " ws-cell--" + semClasses[i];
    }
    attrs.className = classList;
    return el("td", attrs, [text(displayText)]);
  }

  // ────────────────────────────────────────────────────────────────
  // Numeric formatting helpers
  // ────────────────────────────────────────────────────────────────

  /** Strip floating-point noise before formatting. */
  function cleanFloat(n) {
    return parseFloat(n.toFixed(10));
  }

  /**
   * Format a numeric value with magnitude-appropriate precision and grouping.
   * Purely shape-driven — no semantic awareness.
   *
   * Rules:
   *   integer-like (abs(n - round(n)) < 1e-9) → 0 decimals
   *   abs >= 1000 → max 2 decimals, comma grouping
   *   abs >= 1    → max 3 decimals, no grouping
   *   abs < 1     → max 4 decimals, no grouping
   *   trailing zeros trimmed automatically by toLocaleString
   */
  function formatNumber(value) {
    if (value == null) return "";
    if (typeof value !== "number" || !Number.isFinite(value)) return String(value);

    var n = cleanFloat(value);
    var abs = Math.abs(n);
    var isIntegerLike = Math.abs(n - Math.round(n)) < 1e-9;
    var useGrouping = abs >= 1000;

    if (isIntegerLike) {
      return n.toLocaleString("en-US", {
        useGrouping: useGrouping,
        maximumFractionDigits: 0
      });
    }

    var maxFrac;
    if (abs >= 1000) maxFrac = 2;
    else if (abs >= 1) maxFrac = 3;
    else maxFrac = 4;

    return n.toLocaleString("en-US", {
      useGrouping: useGrouping,
      minimumFractionDigits: 0,
      maximumFractionDigits: maxFrac
    });
  }

  // ────────────────────────────────────────────────────────────────
  // Default formatting by column type
  // ────────────────────────────────────────────────────────────────

  function defaultFormat(value, columnType) {
    if (columnType === "bool") {
      return value ? "TRUE" : "FALSE";
    }
    if (columnType === "float64" || columnType === "int64") {
      var n = Number(value);
      if (!isNaN(n)) {
        return formatNumber(n);
      }
    }
    return escapeText(value);
  }

  // ────────────────────────────────────────────────────────────────
  // Render: provenance footer
  // ────────────────────────────────────────────────────────────────

  function renderProvenance(ws) {
    if (!ws.provenance) return null;

    var prov = ws.provenance;
    var vt = prov.view_table || {};

    var details = el("details", { className: "ws-provenance" });
    var summary = el("summary", {}, [text("Provenance")]);
    details.appendChild(summary);

    var dl = el("dl", { className: "ws-provenance-details" });

    var fields = [
      ["Source", vt.source_label || ""],
      ["Row key", vt.row_key || "(none)"],
      ["Input rows", String(vt.input_row_count || 0)],
      ["Compiled", prov.compiled_at || ""],
      ["Version", prov.fin123_version || ""],
      ["Spec", prov.spec_name || ""],
      ["Output rows", String(prov.row_count || 0)],
      ["Output columns", String(prov.column_count || 0)],
    ];

    for (var i = 0; i < fields.length; i++) {
      dl.appendChild(el("dt", {}, [text(fields[i][0])]));
      dl.appendChild(el("dd", {}, [text(fields[i][1])]));
    }

    details.appendChild(dl);
    return details;
  }

  // ────────────────────────────────────────────────────────────────
  // Post-render: fix sticky header offsets
  // ────────────────────────────────────────────────────────────────

  function fixStickyOffsets(table, hasGroupRow) {
    if (!hasGroupRow) {
      // Single header row: top: 0
      var headerCells = table.querySelectorAll(".ws-header-row th");
      for (var i = 0; i < headerCells.length; i++) {
        headerCells[i].style.top = "0";
      }
      return;
    }

    // Group row at top: 0
    var groupCells = table.querySelectorAll(".ws-header-group-row th");
    for (var g = 0; g < groupCells.length; g++) {
      groupCells[g].style.top = "0";
    }

    // Measure group row height after render
    var groupRow = table.querySelector(".ws-header-group-row");
    if (!groupRow) return;
    var groupHeight = groupRow.offsetHeight;

    // Column headers below group row
    var colHeaders = table.querySelectorAll(".ws-header-row th");
    for (var c = 0; c < colHeaders.length; c++) {
      colHeaders[c].style.top = groupHeight + "px";
    }
  }

  // ────────────────────────────────────────────────────────────────
  // Main render function
  // ────────────────────────────────────────────────────────────────

  function render(container, ws) {
    if (!container || !ws) return;

    // Clear container
    while (container.firstChild) {
      container.removeChild(container.firstChild);
    }

    // Root wrapper
    var viewer = el("div", {
      className: "ws-viewer",
      role: "region",
      "aria-label": ws.title || ws.name || "Worksheet",
    });

    // Title
    if (ws.title) {
      viewer.appendChild(el("div", { className: "ws-title" }, [text(ws.title)]));
    }

    // Error summary
    var errorBanner = renderErrorSummary(ws);
    if (errorBanner) {
      viewer.appendChild(errorBanner);
    }

    // Table
    var tableResult = renderTable(ws);
    var tableContainer = el("div", { className: "ws-table-container" });
    tableContainer.appendChild(tableResult.table);
    viewer.appendChild(tableContainer);

    // Provenance
    var provenance = renderProvenance(ws);
    if (provenance) {
      viewer.appendChild(provenance);
    }

    container.appendChild(viewer);

    // Post-render: fix sticky offsets (must happen after DOM insertion)
    fixStickyOffsets(tableResult.table, tableResult.hasGroupRow);
  }

  // ────────────────────────────────────────────────────────────────
  // Public API
  // ────────────────────────────────────────────────────────────────

  return {
    render: render,
  };
})();

// Attach to window for non-module environments
if (typeof window !== "undefined") {
  window.WorksheetViewer = WorksheetViewer;
}
