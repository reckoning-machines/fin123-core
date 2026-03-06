/**
 * DOM assertion tests for worksheet_viewer.js
 *
 * Self-contained Node.js test using jsdom-like minimal shim.
 * Run: node tests/test_worksheet_viewer_dom.js
 * Exit code 0 = all pass, 1 = failure.
 *
 * No npm dependencies — uses only Node builtins + the viewer source.
 */

"use strict";

const { JSDOM } = (() => {
  try {
    return require("jsdom");
  } catch (e) {
    // If jsdom isn't installed, use a lighter approach via vm + built-in
    return { JSDOM: null };
  }
})();

const fs = require("fs");
const path = require("path");
const vm = require("vm");

// ── Minimal DOM environment ──

function createDOMEnv() {
  if (JSDOM) {
    const dom = new JSDOM("<!DOCTYPE html><html><body><div id='root'></div></body></html>");
    return { window: dom.window, document: dom.window.document };
  }

  // Fallback: build a minimal DOM shim in-process
  // This is a subset sufficient for worksheet_viewer.js
  const elements = [];

  function createTextNode(text) {
    return { nodeType: 3, textContent: String(text), _tag: "#text", childNodes: [], outerHTML: escapeHTML(String(text)) };
  }

  function escapeHTML(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function createElement(tag) {
    const el = {
      nodeType: 1,
      tagName: tag.toUpperCase(),
      _tag: tag.toLowerCase(),
      attributes: {},
      childNodes: [],
      className: "",
      textContent: "",
      innerHTML: "",
      style: {},
      _parent: null,

      setAttribute(k, v) {
        this.attributes[k] = String(v);
      },
      getAttribute(k) {
        return this.attributes[k] || null;
      },
      get firstChild() {
        return this.childNodes.length > 0 ? this.childNodes[0] : null;
      },
      appendChild(child) {
        this.childNodes.push(child);
        child._parent = this;
        return child;
      },
      removeChild(child) {
        const idx = this.childNodes.indexOf(child);
        if (idx !== -1) this.childNodes.splice(idx, 1);
        return child;
      },
      querySelectorAll(selector) {
        return querySelectorAllImpl(this, selector);
      },
      querySelector(selector) {
        const results = this.querySelectorAll(selector);
        return results.length > 0 ? results[0] : null;
      },
      get offsetHeight() { return 30; }, // Stub for sticky offset calc

      get outerHTML() {
        let html = "<" + this._tag;
        if (this.className) html += ' class="' + escapeHTML(this.className) + '"';
        for (const [k, v] of Object.entries(this.attributes)) {
          html += " " + k + '="' + escapeHTML(v) + '"';
        }
        html += ">";
        if (this.childNodes.length > 0) {
          for (const child of this.childNodes) {
            html += child.outerHTML || escapeHTML(child.textContent);
          }
        } else if (this.textContent) {
          html += escapeHTML(this.textContent);
        } else if (this.innerHTML) {
          html += this.innerHTML;
        }
        const voidTags = new Set(["br", "hr", "img", "input"]);
        if (!voidTags.has(this._tag)) {
          html += "</" + this._tag + ">";
        }
        return html;
      },
    };
    elements.push(el);
    return el;
  }

  // Minimal querySelectorAll supporting class and tag selectors
  function querySelectorAllImpl(root, selector) {
    const results = [];
    function walk(node) {
      if (node.nodeType !== 1) return;
      if (matchesSelector(node, selector)) results.push(node);
      for (const child of (node.childNodes || [])) {
        walk(child);
      }
    }
    walk(root);
    return results;
  }

  function matchesSelector(node, selector) {
    // Support: .class, tag, tag.class, .class1.class2
    const parts = selector.split(",").map(s => s.trim());
    for (const part of parts) {
      if (matchesSingle(node, part)) return true;
    }
    return false;
  }

  function matchesSingle(node, sel) {
    // Simple: tag.class or .class or tag
    const m = sel.match(/^(\w+)?(\.[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*)?$/);
    if (!m) return false;
    const tag = m[1];
    const classes = m[2] ? m[2].split(".").filter(Boolean) : [];
    if (tag && node._tag !== tag.toLowerCase()) return false;
    const nodeClasses = (node.className || "").split(/\s+/);
    for (const cls of classes) {
      if (nodeClasses.indexOf(cls) === -1) return false;
    }
    return true;
  }

  const doc = {
    createElement: createElement,
    createTextNode: createTextNode,
  };

  const rootEl = createElement("div");
  rootEl.setAttribute("id", "root");

  return { window: { WorksheetViewer: null }, document: doc, root: rootEl };
}

// ── Load the viewer script ──

function loadViewer(env) {
  const viewerPath = path.join(__dirname, "..", "src", "fin123", "ui", "static", "worksheet_viewer.js");
  const code = fs.readFileSync(viewerPath, "utf-8");

  const context = vm.createContext({
    window: env.window,
    document: env.document,
    console: console,
  });

  vm.runInContext(code, context);
  return context.window.WorksheetViewer || context.WorksheetViewer;
}

// ── Test fixtures ──

function makeBasicWorksheet() {
  return {
    name: "test_ws",
    title: "Test Worksheet",
    columns: [
      { name: "ticker", label: "Ticker", column_type: "string", display_format: null, source: "ticker", expression: null },
      { name: "revenue", label: "Revenue", column_type: "float64", display_format: { type: "currency", symbol: "$", places: 2 }, source: "revenue", expression: null },
      { name: "margin", label: "Margin", column_type: "float64", display_format: { type: "percent", places: 1 }, source: null, expression: "revenue / cost" },
    ],
    sorts: [{ column: "revenue", descending: true }],
    header_groups: [
      { label: "Financials", columns: ["revenue", "margin"] },
    ],
    rows: [
      { ticker: "AAPL", revenue: 100000.5, margin: 0.35 },
      { ticker: "MSFT", revenue: 80000.0, margin: 0.42 },
      { ticker: "GOOG", revenue: 60000.0, margin: null },
    ],
    flags: [
      [],
      [{ name: "high_margin", severity: "info", message: "Margin above 40%" }],
      [{ name: "missing_data", severity: "warning", message: "Null margin" }],
    ],
    provenance: {
      view_table: { source_label: "demo run", row_key: "ticker", input_row_count: 3, input_columns: ["ticker", "revenue", "cost"] },
      compiled_at: "2025-06-15T12:00:00+00:00",
      fin123_version: "0.1.0",
      spec_name: "test_ws",
      row_count: 3,
      column_count: 3,
      columns: {
        ticker: { type: "source", source_column: "ticker", expression: null },
        revenue: { type: "source", source_column: "revenue", expression: null },
        margin: { type: "derived", source_column: null, expression: "revenue / cost" },
      },
    },
    error_summary: null,
  };
}

function makeErrorWorksheet() {
  return {
    name: "err_ws",
    title: null,
    columns: [
      { name: "x", label: "X", column_type: "float64", display_format: null },
      { name: "ratio", label: "Ratio", column_type: "float64", display_format: { type: "decimal", places: 2 } },
    ],
    sorts: [],
    header_groups: [],
    rows: [
      { x: 10, ratio: 2.5 },
      { x: 20, ratio: { error: "#DIV/0!" } },
    ],
    flags: [[], []],
    provenance: {
      view_table: { source_label: "test", row_key: null, input_row_count: 2, input_columns: ["x", "y"] },
      compiled_at: "2025-06-15T12:00:00+00:00",
      fin123_version: "0.1.0",
      spec_name: "err_ws",
      row_count: 2,
      column_count: 2,
      columns: {},
    },
    error_summary: { total_errors: 1, by_column: { ratio: 1 } },
  };
}

// ── Test runner ──

let passed = 0;
let failed = 0;
const failures = [];

function assert(condition, message) {
  if (condition) {
    passed++;
  } else {
    failed++;
    failures.push(message);
    console.error("  FAIL: " + message);
  }
}

function assertIncludes(html, needle, message) {
  assert(html.indexOf(needle) !== -1, message + " — expected to find: " + JSON.stringify(needle));
}

function assertNotIncludes(html, needle, message) {
  assert(html.indexOf(needle) === -1, message + " — expected NOT to find: " + JSON.stringify(needle));
}

// ── Tests ──

function runTests() {
  const env = createDOMEnv();
  const viewer = loadViewer(env);

  assert(viewer != null, "WorksheetViewer loaded");
  assert(typeof viewer.render === "function", "WorksheetViewer.render is a function");

  // ── Test: basic render ──
  console.log("  Test: basic render");
  {
    const container = env.document.createElement("div");
    const ws = makeBasicWorksheet();
    viewer.render(container, ws);
    const html = container.outerHTML;

    // Semantic table
    assertIncludes(html, "<table", "renders a <table>");
    assertIncludes(html, "role=\"table\"", "table has role=table");
    assertIncludes(html, "role=\"region\"", "viewer root has role=region");

    // Title
    assertIncludes(html, "Test Worksheet", "renders title");
    assertIncludes(html, "ws-title", "title has ws-title class");

    // Column headers
    assertIncludes(html, "Ticker", "renders Ticker header");
    assertIncludes(html, "Revenue", "renders Revenue header");
    assertIncludes(html, "Margin", "renders Margin header");
    assertIncludes(html, "scope=\"col\"", "column headers have scope=col");

    // Grouped headers
    assertIncludes(html, "Financials", "renders grouped header label");
    assertIncludes(html, "ws-header-group-row", "has group header row class");
    assertIncludes(html, "scope=\"colgroup\"", "group headers have scope=colgroup");

    // Data rows
    assertIncludes(html, "AAPL", "renders AAPL row");
    assertIncludes(html, "MSFT", "renders MSFT row");
    assertIncludes(html, "GOOG", "renders GOOG row");

    // Currency formatting
    assertIncludes(html, "$100,000.50", "formats currency with symbol, commas, places");
    assertIncludes(html, "$80,000.00", "formats second currency value");

    // Percent formatting
    assertIncludes(html, "35.0%", "formats percent with places");
    assertIncludes(html, "42.0%", "formats second percent value");

    // Sort indicator
    assertIncludes(html, "aria-sort=\"descending\"", "revenue column has aria-sort descending");
    assertIncludes(html, "ws-sort-indicator", "sort indicator rendered");

    // Null cell
    assertIncludes(html, "ws-cell--null", "null cell has null class");

    // Flags
    assertIncludes(html, "ws-flag--info", "info flag rendered");
    assertIncludes(html, "ws-flag--warning", "warning flag rendered");
    assertIncludes(html, "ws-flag-cell", "flag cell column present");

    // Provenance
    assertIncludes(html, "ws-provenance", "provenance section rendered");
    assertIncludes(html, "<details", "provenance uses <details> element");
    assertIncludes(html, "<summary", "provenance uses <summary> element");
    assertIncludes(html, "demo run", "provenance shows source_label");
    assertIncludes(html, "0.1.0", "provenance shows version");
    assertIncludes(html, "test_ws", "provenance shows spec_name");

    // No error summary (none in this fixture)
    assertNotIncludes(html, "ws-error-summary", "no error summary when no errors");
  }

  // ── Test: error rendering ──
  console.log("  Test: error rendering");
  {
    const container = env.document.createElement("div");
    const ws = makeErrorWorksheet();
    viewer.render(container, ws);
    const html = container.outerHTML;

    // Error summary banner
    assertIncludes(html, "ws-error-summary", "error summary banner rendered");
    assertIncludes(html, "1 error", "error summary shows count");
    assertIncludes(html, "role=\"alert\"", "error summary has role=alert");
    assertIncludes(html, "ratio: 1", "error summary shows by-column breakdown");

    // Inline error cell
    assertIncludes(html, "#DIV/0!", "inline error value rendered");
    assertIncludes(html, "ws-cell--error", "error cell has error class");
    assertIncludes(html, "role=\"status\"", "error cell has role=status");
    assertIncludes(html, "aria-label=\"Error: #DIV/0!\"", "error cell has aria-label");

    // Normal value with display format
    assertIncludes(html, "2.50", "normal decimal value formatted");
  }

  // ── Test: no title ──
  console.log("  Test: no title");
  {
    const container = env.document.createElement("div");
    const ws = makeErrorWorksheet(); // has title: null
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertNotIncludes(html, "ws-title", "no title element when title is null");
  }

  // ── Test: no header groups ──
  console.log("  Test: no header groups");
  {
    const container = env.document.createElement("div");
    const ws = makeErrorWorksheet(); // has header_groups: []
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertNotIncludes(html, "ws-header-group-row", "no group row when no header groups");
  }

  // ── Test: no flags ──
  console.log("  Test: no flags");
  {
    const container = env.document.createElement("div");
    const ws = makeErrorWorksheet(); // flags are all empty arrays
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertNotIncludes(html, "ws-flag-cell", "no flag column when no flags triggered");
  }

  // ── Test: no client-side sort or filter ──
  console.log("  Test: no sort/filter functionality");
  {
    const viewerKeys = Object.keys(viewer);
    assert(viewerKeys.length === 1 && viewerKeys[0] === "render",
      "WorksheetViewer exposes only render() — no sort/filter methods");
  }

  // ── Test: container cleared on re-render ──
  console.log("  Test: container cleared on re-render");
  {
    const container = env.document.createElement("div");
    const ws = makeBasicWorksheet();
    viewer.render(container, ws);
    viewer.render(container, makeErrorWorksheet());
    const html = container.outerHTML;
    // Should only have the error worksheet content, not both
    assertNotIncludes(html, "Test Worksheet", "previous render cleared on re-render");
    assertIncludes(html, "#DIV/0!", "new render content present");
  }

  // ── Test: integer display format ──
  console.log("  Test: integer display format");
  {
    const container = env.document.createElement("div");
    const ws = {
      name: "int_test",
      title: null,
      columns: [
        { name: "count", label: "Count", column_type: "int64", display_format: { type: "integer" } },
      ],
      sorts: [],
      header_groups: [],
      rows: [{ count: 12345 }],
      flags: [[]],
      provenance: {
        view_table: { source_label: "t", row_key: null, input_row_count: 1, input_columns: ["count"] },
        compiled_at: "", fin123_version: "0.1.0", spec_name: "t",
        row_count: 1, column_count: 1, columns: {},
      },
      error_summary: null,
    };
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertIncludes(html, "12,345", "integer format adds thousands separator");
  }

  // ── Test: bool formatting ──
  console.log("  Test: bool formatting");
  {
    const container = env.document.createElement("div");
    const ws = {
      name: "bool_test",
      title: null,
      columns: [
        { name: "active", label: "Active", column_type: "bool", display_format: null },
      ],
      sorts: [],
      header_groups: [],
      rows: [{ active: true }, { active: false }],
      flags: [[], []],
      provenance: {
        view_table: { source_label: "t", row_key: null, input_row_count: 2, input_columns: ["active"] },
        compiled_at: "", fin123_version: "0.1.0", spec_name: "t",
        row_count: 2, column_count: 1, columns: {},
      },
      error_summary: null,
    };
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertIncludes(html, "TRUE", "true bool renders as TRUE");
    assertIncludes(html, "FALSE", "false bool renders as FALSE");
  }

  // ── Test: negative currency formatting ──
  console.log("  Test: negative currency");
  {
    const container = env.document.createElement("div");
    const ws = {
      name: "neg_test",
      title: null,
      columns: [
        { name: "val", label: "Val", column_type: "float64", display_format: { type: "currency", symbol: "$", places: 2 } },
      ],
      sorts: [],
      header_groups: [],
      rows: [{ val: -1500.5 }],
      flags: [[]],
      provenance: {
        view_table: { source_label: "t", row_key: null, input_row_count: 1, input_columns: ["val"] },
        compiled_at: "", fin123_version: "0.1.0", spec_name: "t",
        row_count: 1, column_count: 1, columns: {},
      },
      error_summary: null,
    };
    viewer.render(container, ws);
    const html = container.outerHTML;
    assertIncludes(html, "($1,500.50)", "negative currency uses parentheses");
  }

  // ── Summary ──
  console.log("");
  console.log("Results: " + passed + " passed, " + failed + " failed");
  if (failures.length > 0) {
    console.log("Failures:");
    for (const f of failures) {
      console.log("  - " + f);
    }
    process.exit(1);
  }
}

runTests();
