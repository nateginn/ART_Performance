"use strict";

// Client-side viewer over the sanitized aggregate bundle already produced by
// publish_exporter.py. This file fetches exactly three same-origin files —
// data/bundle.json, data/manifest.json, schema_constants.json — all already
// scanner-approved or schema-derived before they reach this directory (see
// publish_scanner.py / publish_site_constants.py). There is no server-side
// logic here: integrity verification, filtering, facility summation, and CSV
// export all happen in the browser against data that was already safe to
// publish in full.
//
// Before anything is rendered, the fetched bundle is (1) checked against
// schema_constants.json's enum lists and field types (catches a malformed or
// schema-drifted bundle before it can crash a formatter or render NaN), and
// (2) re-checksummed against manifest.json's declared SHA-256 per report
// type (catches a bundle.json/manifest.json pair from two different
// publications — e.g. a redeploy that lands between the two fetches). Either
// failure aborts rendering and shows an error banner instead of guessing.

const COMBINED_FACILITY = "Combined";

const state = {
  bundle: null,
  manifest: null,
  constants: null,
  knownFacilities: [],
  rollupAllProviders: null,
  rollupAllCategories: null,
  periodType: "month",
  period: null,
  facility: COMBINED_FACILITY,
  financialMonth: null,
};

function fmtMoney(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v < 0 ? "-" : "";
  return sign + "$" + Math.abs(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtCount(v) {
  if (v === null || v === undefined) return "—";
  return v.toLocaleString();
}

function fmtPct(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v.toFixed(1) + "%";
}

function round2(v) {
  return Math.round(v * 100) / 100;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// --- data loading ------------------------------------------------------

async function loadJson(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`${path} — HTTP ${res.status}`);
  }
  return res.json();
}

function reportProblems(app, label, problems) {
  finishWithError(app,
    `${label}:\n` + problems.slice(0, 10).join("\n") +
    (problems.length > 10 ? `\n…and ${problems.length - 10} more` : ""));
}

async function init() {
  const app = document.getElementById("app");
  let bundle, manifest, constants;
  try {
    [bundle, manifest, constants] = await Promise.all([
      loadJson("data/bundle.json"),
      loadJson("data/manifest.json"),
      loadJson("schema_constants.json"),
    ]);
  } catch (err) {
    finishWithError(app,
      "Could not load report data (" + err.message + "). This page must be " +
      "served alongside data/bundle.json, data/manifest.json, and schema_constants.json.");
    return;
  }

  // Validated in strict order — each stage assumes the previous one already
  // passed, so a malformed schema_constants.json (say) can't produce a
  // confusing cascade of bundle-shape errors caused by its own missing data.
  const constantsProblems = validateConstants(constants);
  if (constantsProblems.length) {
    reportProblems(app, "schema_constants.json failed validation and cannot be used", constantsProblems);
    return;
  }

  const manifestProblems = validateManifestShape(manifest, constants);
  if (manifestProblems.length) {
    reportProblems(app, "manifest.json failed validation and cannot be displayed", manifestProblems);
    return;
  }

  const shapeProblems = validateBundleShape(bundle, constants);
  if (shapeProblems.length) {
    reportProblems(app, "Report data failed validation and cannot be displayed", shapeProblems);
    return;
  }

  let integrityProblems;
  try {
    integrityProblems = await verifyBundleIntegrity(bundle, manifest, constants);
  } catch (err) {
    finishWithError(app, "Could not verify report data integrity (" + err.message + ").");
    return;
  }
  if (integrityProblems.length) {
    reportProblems(app,
      "Report data failed integrity verification and cannot be displayed " +
      "(bundle.json and manifest.json may be from two different publications)",
      integrityProblems);
    return;
  }

  state.bundle = bundle;
  state.manifest = manifest;
  state.constants = constants;
  state.knownFacilities = constants.allowed_facility_values.filter((f) => f !== COMBINED_FACILITY);
  state.rollupAllProviders = constants.rollup_all_providers;
  state.rollupAllCategories = constants.rollup_all_categories;

  app.setAttribute("aria-busy", "false");
  renderBundleMeta();
  populateFacilitySelect();
  populatePeriodSelect();
  populateFinancialMonthSelect();
  wireControls();
  renderAll();
}

function finishWithError(app, message) {
  app.setAttribute("aria-busy", "false");
  const banner = document.getElementById("load-error");
  banner.hidden = false;
  banner.textContent = message;
}

// --- integrity verification ---------------------------------------------
//
// Reproduces publish_manifest.checksum_report()'s exact canonical form
// (json.dumps(rows, sort_keys=True, separators=(",", ":")), ensure_ascii
// default True) so a browser-computed SHA-256 matches the Python-computed
// one bundled in manifest.json byte-for-byte. Field "kind" (from
// schema_constants.report_field_kinds) decides int-vs-float formatting,
// since JSON itself doesn't distinguish them but Python's json.dumps does
// (an amount of exactly 200 serializes as "200.0", a count as "200").

function pyJsonString(s) {
  let out = '"';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    const code = s.charCodeAt(i);
    if (ch === '"') out += '\\"';
    else if (ch === "\\") out += "\\\\";
    else if (ch === "\b") out += "\\b";
    else if (ch === "\f") out += "\\f";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (ch === "\t") out += "\\t";
    else if (code < 0x20 || code > 0x7e) out += "\\u" + code.toString(16).padStart(4, "0");
    else out += ch;
  }
  return out + '"';
}

// Python's float repr and JS's Number.prototype.toString() both use a
// shortest-round-trip algorithm and agree digit-for-digit within this range,
// but they switch to exponential notation at DIFFERENT magnitudes (Python's
// repr around 1e16, JS's around 1e21) — a value in that gap would format
// identically in neither language's "obviously safe" zone and could produce
// a false checksum mismatch (or worse, a false match) if extrapolated
// blindly. Real KPI figures (dollar amounts, percentages bounded by the
// schema to [-1000, 10000]) never approach these bounds, so anything outside
// a deliberately conservative safe window is treated as unverifiable and
// fails closed rather than guessing.
const SAFE_FLOAT_MAX_MAGNITUDE = 1e12;
const SAFE_FLOAT_MIN_NONZERO_MAGNITUDE = 1e-4;

function pyFloatRepr(v) {
  if (!Number.isFinite(v)) throw new Error("non-finite amount cannot be checksummed");
  if (v !== 0 && Math.abs(v) < SAFE_FLOAT_MIN_NONZERO_MAGNITUDE) {
    throw new Error("value magnitude too small to verify canonically");
  }
  if (Math.abs(v) >= SAFE_FLOAT_MAX_MAGNITUDE) {
    throw new Error("value magnitude too large to verify canonically");
  }
  if (Object.is(v, -0)) return "-0.0";
  let s = v.toString();
  if (!s.includes(".") && !s.includes("e") && !s.includes("E")) s += ".0";
  return s;
}

const INT_KINDS = new Set(["count", "small_group_count"]);
const FLOAT_KINDS = new Set(["amount", "rate_pct"]);

function canonicalValueJson(kind, value) {
  const t = typeof value;
  if (t === "string") return pyJsonString(value);
  if (t === "boolean") return value ? "true" : "false";
  if (t === "number") {
    if (INT_KINDS.has(kind)) {
      if (!Number.isInteger(value)) throw new Error(`expected an integer for kind '${kind}'`);
      return String(value);
    }
    if (FLOAT_KINDS.has(kind)) return pyFloatRepr(value);
    return Number.isInteger(value) ? String(value) : pyFloatRepr(value);
  }
  throw new Error(`unsupported value type '${t}' during checksum canonicalization`);
}

function canonicalReportJson(rows, fieldKinds) {
  const rowsJson = rows.map((row) => {
    const keys = Object.keys(row).sort();
    const body = keys.map((k) => pyJsonString(k) + ":" + canonicalValueJson(fieldKinds[k], row[k])).join(",");
    return "{" + body + "}";
  });
  return "[" + rowsJson.join(",") + "]";
}

async function sha256Hex(str) {
  const bytes = new TextEncoder().encode(str);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function verifyBundleIntegrity(bundle, manifest, constants) {
  const problems = [];
  const declared = new Map((manifest.reports || []).map((r) => [r.report_type, r]));
  const fieldKindsByType = constants.report_field_kinds || {};

  for (const [reportType, fieldKinds] of Object.entries(fieldKindsByType)) {
    const rows = bundle[reportType];
    if (!Array.isArray(rows)) {
      problems.push(`bundle.${reportType} is missing or not an array`);
      continue;
    }
    const decl = declared.get(reportType);
    if (!decl) {
      problems.push(`manifest has no entry for report type '${reportType}'`);
      continue;
    }
    if (rows.length !== decl.row_count) {
      problems.push(`${reportType}: row count ${rows.length} does not match manifest (${decl.row_count})`);
      continue;
    }
    let canonical;
    try {
      canonical = canonicalReportJson(rows, fieldKinds);
    } catch (err) {
      problems.push(`${reportType}: could not verify checksum (${err.message})`);
      continue;
    }
    const actual = await sha256Hex(canonical);
    if (actual !== decl.sha256) {
      problems.push(`${reportType}: checksum mismatch`);
    }
  }
  return problems;
}

// --- constants / manifest / shape validation ------------------------------
//
// Validated in this order from init(): schema_constants.json itself, then
// manifest.json's structure, then every bundle row's fields (including
// rejecting any field not declared for that report type — defense-in-depth
// against an unexpected or PHI-bearing extra field surviving into a
// checksum-valid bundle, e.g. from a future exporter regression that also
// happens to compute its checksum over the same, already-wrong bundle).

const REQUIRED_CONSTANT_ARRAYS = [
  "allowed_facility_values", "canonical_categories", "allowed_payer_groups",
  "allowed_procedure_groups", "allowed_freshness_verdicts", "allowed_table_names",
  "allowed_anomaly_metrics", "allowed_anomaly_severities", "period_types",
];
const REQUIRED_CONSTANT_STRINGS = [
  "schema_version", "manifest_schema_version", "rollup_all_providers", "rollup_all_categories",
];
// The complete set of report types publish_schema.REPORT_SCHEMAS declares.
// JS can't import the Python module, so this is a literal, reviewed
// duplication of the report-type NAMES only (same acceptable-duplication
// pattern already used for the rollup sentinels — see the Risks section) —
// needed because report_field_kinds' own key set can't validate its own
// completeness: iterating "whatever keys happen to be present" can never
// notice a key is silently absent. Without this, a truncated
// schema_constants.json (missing e.g. "anomaly_summary") would pass
// validateConstants() and then validateBundleShape()/validateManifestShape()
// would silently never check that report type at all, since both derive
// their "known report types" list from report_field_kinds' own keys.
const REQUIRED_REPORT_TYPES = [
  "visits_kpi", "financial_summary", "payer_procedure_summary",
  "anomaly_summary", "freshness",
];

function validateConstants(constants) {
  const problems = [];
  if (!constants || typeof constants !== "object") {
    return ["schema_constants.json is not an object"];
  }
  for (const key of REQUIRED_CONSTANT_ARRAYS) {
    const v = constants[key];
    if (!Array.isArray(v) || v.length === 0 || !v.every((x) => typeof x === "string")) {
      problems.push(`schema_constants.${key} is missing or not a non-empty array of strings`);
    }
  }
  for (const key of REQUIRED_CONSTANT_STRINGS) {
    if (typeof constants[key] !== "string" || !constants[key]) {
      problems.push(`schema_constants.${key} is missing or not a non-empty string`);
    }
  }
  const fieldKinds = constants.report_field_kinds;
  if (!fieldKinds || typeof fieldKinds !== "object" || Array.isArray(fieldKinds)) {
    problems.push("schema_constants.report_field_kinds is missing or not an object");
  } else {
    for (const reportType of REQUIRED_REPORT_TYPES) {
      if (!(reportType in fieldKinds)) {
        problems.push(`schema_constants.report_field_kinds is missing required report type '${reportType}'`);
      }
    }
    for (const [reportType, fields] of Object.entries(fieldKinds)) {
      if (!fields || typeof fields !== "object" || Array.isArray(fields)) {
        problems.push(`schema_constants.report_field_kinds.${reportType} is not an object`);
        continue;
      }
      for (const [field, kind] of Object.entries(fields)) {
        if (typeof kind !== "string" || !kind) {
          problems.push(`schema_constants.report_field_kinds.${reportType}.${field} has an invalid kind`);
        }
      }
    }
  }
  return problems;
}

const CHECKSUM_HEX_RE = /^[0-9a-f]{64}$/;
const REPORTING_PERIOD_RE = /^(\d{4}-(?:0[1-9]|1[0-2]))\.\.(\d{4}-(?:0[1-9]|1[0-2]))$/;
const TIMESTAMP_DETAIL_RE = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$/;

// Regex alone can't catch "2026-13-40T25:70:70" (wrong digit counts pass a
// shape-only check). Mirrors publish_schema._kind_timestamp: validate shape,
// then round-trip through a real Date to reject invalid calendar/clock
// values the same way Python's datetime.fromisoformat() would raise on them.
function isValidTimestamp(value) {
  if (typeof value !== "string") return false;
  const m = TIMESTAMP_DETAIL_RE.exec(value);
  if (!m) return false;
  const year = Number(m[1]), month = Number(m[2]), day = Number(m[3]);
  const hour = Number(m[4]), minute = Number(m[5]), second = Number(m[6]);
  if (month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59) return false;
  const dt = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  return dt.getUTCFullYear() === year && dt.getUTCMonth() === month - 1 && dt.getUTCDate() === day
    && dt.getUTCHours() === hour && dt.getUTCMinutes() === minute && dt.getUTCSeconds() === second;
}

function validateManifestShape(manifest, constants) {
  const problems = [];
  if (!manifest || typeof manifest !== "object") return ["manifest.json is not an object"];

  if (manifest.schema_version !== constants.manifest_schema_version) {
    problems.push(`manifest.schema_version '${manifest.schema_version}' does not match expected '${constants.manifest_schema_version}'`);
  }
  if (manifest.bundle_schema_version !== constants.schema_version) {
    problems.push(`manifest.bundle_schema_version '${manifest.bundle_schema_version}' does not match this page's '${constants.schema_version}'`);
  }
  if (!isValidTimestamp(manifest.generated_at)) {
    problems.push("manifest.generated_at is missing or not a valid timestamp");
  }
  const periodMatch = typeof manifest.reporting_period === "string"
    ? REPORTING_PERIOD_RE.exec(manifest.reporting_period) : null;
  if (!periodMatch) {
    problems.push("manifest.reporting_period is missing or malformed");
  } else if (periodMatch[1] > periodMatch[2]) {
    problems.push(`manifest.reporting_period start '${periodMatch[1]}' is after end '${periodMatch[2]}'`);
  }

  const knownReportTypes = Object.keys(constants.report_field_kinds || {});
  if (!Array.isArray(manifest.reports)) {
    problems.push("manifest.reports is missing or not an array");
  } else {
    const seen = new Set();
    for (const r of manifest.reports) {
      if (!r || typeof r !== "object") { problems.push("manifest.reports contains a non-object entry"); continue; }
      if (typeof r.report_type !== "string" || !knownReportTypes.includes(r.report_type)) {
        problems.push(`manifest.reports has an unrecognized report_type '${r.report_type}'`);
      } else {
        seen.add(r.report_type);
      }
      if (typeof r.row_count !== "number" || !Number.isInteger(r.row_count) || r.row_count < 0) {
        problems.push(`manifest.reports['${r.report_type}'].row_count is invalid`);
      }
      if (typeof r.sha256 !== "string" || !CHECKSUM_HEX_RE.test(r.sha256)) {
        problems.push(`manifest.reports['${r.report_type}'].sha256 is not a valid checksum`);
      }
    }
    for (const known of knownReportTypes) {
      if (!seen.has(known)) problems.push(`manifest.reports is missing a declaration for '${known}'`);
    }
  }

  if (!Array.isArray(manifest.source_table_freshness)) {
    problems.push("manifest.source_table_freshness is missing or not an array");
  } else {
    for (const r of manifest.source_table_freshness) {
      if (!r || typeof r !== "object") { problems.push("source_table_freshness contains a non-object entry"); continue; }
      if (typeof r.table_name !== "string" || !constants.allowed_table_names.includes(r.table_name)) {
        problems.push(`source_table_freshness has an unrecognized table_name '${r.table_name}'`);
      }
      if (typeof r.verdict !== "string" || !constants.allowed_freshness_verdicts.includes(r.verdict)) {
        problems.push(`source_table_freshness['${r.table_name}'].verdict is invalid`);
      }
      if (typeof r.age_days !== "number" || !Number.isFinite(r.age_days)) {
        problems.push(`source_table_freshness['${r.table_name}'].age_days is invalid`);
      }
    }
  }

  return problems;
}

const DAY_PERIOD_RE = /^\d{4}-\d{2}-\d{2}$/;
const WEEK_PERIOD_RE = /^(\d{4})-W(\d{2})$/;
const MONTH_PERIOD_RE = /^\d{4}-(0[1-9]|1[0-2])$/;

// Löhr's formula for whether an ISO-8601 year has 53 weeks rather than 52 —
// mirrors what Python's date.fromisocalendar(year, 53, 1) raises ValueError
// on. Needed because most years do NOT have a week 53 (e.g. 2026 has only
// 52); a bare "week <= 53" check would accept a nonexistent "2026-W53".
function isoYearHasWeek53(year) {
  const p = (y) => (y + Math.floor(y / 4) - Math.floor(y / 100) + Math.floor(y / 400)) % 7;
  return p(year) === 4 || p(year - 1) === 3;
}

function isValidPeriodValue(value, periodType) {
  if (typeof value !== "string") return false;
  if (periodType === "day") {
    if (!DAY_PERIOD_RE.test(value)) return false;
    const d = new Date(value + "T00:00:00Z");
    return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === value;
  }
  if (periodType === "week") {
    const m = WEEK_PERIOD_RE.exec(value);
    if (!m) return false;
    const year = parseInt(m[1], 10);
    const week = parseInt(m[2], 10);
    if (week < 1 || week > 53) return false;
    if (week === 53 && !isoYearHasWeek53(year)) return false;
    return true;
  }
  if (periodType === "month") {
    return MONTH_PERIOD_RE.test(value);
  }
  return false;
}

function validateBundleShape(bundle, constants) {
  const problems = [];
  const fieldKindsByType = (constants && constants.report_field_kinds) || {};
  const enumSets = {
    facility_enum: constants.allowed_facility_values,
    category_enum: constants.canonical_categories,
    payer_group: constants.allowed_payer_groups,
    procedure_group: constants.allowed_procedure_groups,
    freshness_enum: constants.allowed_freshness_verdicts,
    table_name: constants.allowed_table_names,
    anomaly_metric: constants.allowed_anomaly_metrics,
    anomaly_severity: constants.allowed_anomaly_severities,
    period_type: constants.period_types,
  };
  const NONEMPTY_STRING_KINDS = new Set(["timestamp", "provider_name"]);

  outer:
  for (const [reportType, fieldKinds] of Object.entries(fieldKindsByType)) {
    const rows = bundle[reportType];
    if (!Array.isArray(rows)) {
      problems.push(`${reportType} is missing from the bundle`);
      continue;
    }
    const knownFields = new Set(Object.keys(fieldKinds));
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (row === null || typeof row !== "object") {
        problems.push(`${reportType}[${i}] is not an object`);
        continue;
      }
      for (const key of Object.keys(row)) {
        if (!knownFields.has(key)) {
          problems.push(`${reportType}[${i}] has an unexpected field '${key}' not in the approved schema`);
        }
      }
      for (const [field, kind] of Object.entries(fieldKinds)) {
        if (!(field in row)) {
          problems.push(`${reportType}[${i}] is missing required field '${field}'`);
          continue;
        }
        const v = row[field];
        if (INT_KINDS.has(kind) || FLOAT_KINDS.has(kind)) {
          if (typeof v !== "number" || !Number.isFinite(v)) {
            problems.push(`${reportType}[${i}].${field} is not a finite number`);
          }
        } else if (kind === "schema_version") {
          if (v !== constants.schema_version) {
            problems.push(`${reportType}[${i}].${field} schema_version '${v}' does not match this page's '${constants.schema_version}'`);
          }
        } else if (kind === "period") {
          if (!isValidPeriodValue(v, row.period_type)) {
            problems.push(`${reportType}[${i}].${field} '${v}' is not a valid period for period_type '${row.period_type}'`);
          }
        } else if (enumSets[kind]) {
          if (!enumSets[kind].includes(v)) {
            problems.push(`${reportType}[${i}].${field} has an unrecognized ${kind} value`);
          }
        } else if (NONEMPTY_STRING_KINDS.has(kind) && (typeof v !== "string" || !v)) {
          problems.push(`${reportType}[${i}].${field} is not a non-empty string`);
        }
      }
      if (problems.length > 50) break outer;
    }
  }
  return problems;
}

// --- header --------------------------------------------------------------

function renderBundleMeta() {
  const m = state.manifest;
  const el = document.getElementById("bundle-meta");
  const anyStale = (m.source_table_freshness || []).some((r) => r.verdict === "STALE");
  const staleSpan = anyStale
    ? '<span class="stale">some source data is STALE — see Data Freshness below</span>'
    : '<span class="fresh">source data is current</span>';
  el.innerHTML =
    `Bundle schema ${escapeHtml(m.bundle_schema_version)} &middot; ` +
    `generated ${escapeHtml(m.generated_at)} &middot; ` +
    `reporting period ${escapeHtml(m.reporting_period)} &middot; ` +
    staleSpan;
}

// --- filter controls -------------------------------------------------

function populateFacilitySelect() {
  const sel = document.getElementById("facility-select");
  const combinedLabel = `Combined (${state.knownFacilities.join(" + ")})`;
  const options = [`<option value="${escapeHtml(COMBINED_FACILITY)}">${escapeHtml(combinedLabel)}</option>`]
    .concat(state.knownFacilities.map((f) => `<option value="${escapeHtml(f)}">${escapeHtml(f)}</option>`));
  sel.innerHTML = options.join("");
  sel.value = state.facility;
}

function distinctPeriods(periodType) {
  const set = new Set();
  for (const row of state.bundle.visits_kpi) {
    if (row.period_type === periodType) set.add(row.period);
  }
  return Array.from(set).sort().reverse();
}

function populatePeriodSelect() {
  const sel = document.getElementById("period-select");
  const periods = distinctPeriods(state.periodType);
  sel.innerHTML = periods.map((p) => `<option value="${escapeHtml(p)}">${escapeHtml(p)}</option>`).join("");
  state.period = periods[0] || null;
  if (state.period) sel.value = state.period;
}

function populateFinancialMonthSelect() {
  const sel = document.getElementById("financial-month-select");
  const months = Array.from(new Set(state.bundle.financial_summary.map((r) => r.period))).sort().reverse();
  sel.innerHTML = months.map((p) => `<option value="${escapeHtml(p)}">${escapeHtml(p)}</option>`).join("");
  state.financialMonth = months[0] || null;
  if (state.financialMonth) sel.value = state.financialMonth;
}

function wireControls() {
  document.getElementById("period-type-select").addEventListener("change", (e) => {
    state.periodType = e.target.value;
    populatePeriodSelect();
    renderAll();
  });
  document.getElementById("period-select").addEventListener("change", (e) => {
    state.period = e.target.value;
    renderAll();
  });
  document.getElementById("facility-select").addEventListener("change", (e) => {
    state.facility = e.target.value;
    renderAll();
  });
  document.getElementById("financial-month-select").addEventListener("change", (e) => {
    state.financialMonth = e.target.value;
    renderFinancial();
  });
  document.getElementById("period-type-select").value = state.periodType;

  for (const btn of document.querySelectorAll(".download-btn")) {
    btn.addEventListener("click", () => downloadTableCsv(btn.dataset.table, btn.dataset.name));
  }
}

// --- aggregation helpers ---------------------------------------------------
//
// All summation below re-derives avg_collection_per_paid_visit and
// collection_rate_pct from the same summed fields (never independently), the
// same rule publish_exporter.py follows so the numbers stay internally
// consistent when this page sums across facilities client-side.

function sumVisitMetrics(rows) {
  const t = {
    visit_count: 0, new_patient_count: 0, new_patient_visit_count: 0,
    paid_visit_count: 0, charges: 0, allowed_amount: 0, total_collected: 0,
  };
  for (const r of rows) {
    t.visit_count += r.visit_count;
    t.new_patient_count += r.new_patient_count;
    t.new_patient_visit_count += r.new_patient_visit_count;
    t.paid_visit_count += r.paid_visit_count;
    t.charges += r.charges;
    t.allowed_amount += r.allowed_amount;
    t.total_collected += r.total_collected;
  }
  t.charges = round2(t.charges);
  t.allowed_amount = round2(t.allowed_amount);
  t.total_collected = round2(t.total_collected);
  t.avg_collection_per_paid_visit = t.paid_visit_count > 0 ? round2(t.total_collected / t.paid_visit_count) : 0;
  t.collection_rate_pct = t.allowed_amount > 0 ? round2((t.total_collected / t.allowed_amount) * 100) : 0;
  return t;
}

function sumPayerMetrics(rows) {
  const t = { paid_visit_count: 0, total_collected: 0 };
  for (const r of rows) {
    t.paid_visit_count += r.paid_visit_count;
    t.total_collected += r.total_collected;
  }
  t.total_collected = round2(t.total_collected);
  t.avg_collection_per_paid_visit = t.paid_visit_count > 0 ? round2(t.total_collected / t.paid_visit_count) : 0;
  return t;
}

function detectDuplicateKeys(rows, keyFields) {
  const seen = new Set();
  const dupes = new Set();
  for (const r of rows) {
    const k = keyFields.map((f) => r[f]).join(" ");
    if (seen.has(k)) dupes.add(keyFields.map((f) => r[f]).join(" / "));
    seen.add(k);
  }
  return dupes;
}

function groupBySum(rows, keyField, sumFn) {
  const groups = new Map();
  for (const r of rows) {
    const k = r[keyField];
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(r);
  }
  return Array.from(groups.entries()).map(([k, group]) => ({
    [keyField]: k, ...sumFn(group),
  }));
}

function visitsForPeriod() {
  return state.bundle.visits_kpi.filter(
    (r) => r.period_type === state.periodType && r.period === state.period
  );
}

// --- rendering -----------------------------------------------------------

function renderAll() {
  renderFacility();
  renderProvider();
  renderCategory();
  renderPayer();
  renderFinancial();
  renderFreshness();
}

function setPeriodLabel(id) {
  document.getElementById(id).textContent = `(${state.periodType}: ${state.period ?? "n/a"})`;
}

function renderIntegrityNote(panelId, message) {
  const panel = document.getElementById(panelId);
  let note = panel.querySelector(".integrity-note");
  if (!message) {
    if (note) note.remove();
    return;
  }
  if (!note) {
    note = document.createElement("div");
    note.className = "integrity-note";
    panel.insertBefore(note, panel.querySelector(".table-wrap"));
  }
  note.textContent = message;
}

const VISIT_COLUMNS = [
  { key: "visit_count", label: "Visits", fmt: fmtCount },
  { key: "new_patient_count", label: "New Patients (unique)", fmt: fmtCount },
  { key: "new_patient_visit_count", label: "New-Patient Visits", fmt: fmtCount },
  { key: "paid_visit_count", label: "Paid Visits", fmt: fmtCount },
  { key: "charges", label: "Charges", fmt: fmtMoney },
  { key: "allowed_amount", label: "Allowed", fmt: fmtMoney },
  { key: "total_collected", label: "Collected", fmt: fmtMoney },
  { key: "avg_collection_per_paid_visit", label: "Avg / Paid Visit", fmt: fmtMoney },
  { key: "collection_rate_pct", label: "Collection Rate", fmt: fmtPct },
];

function renderTable(tableId, labelKey, labelHeader, columns, rows, emptyMessage) {
  const table = document.getElementById(tableId);
  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");

  thead.innerHTML =
    "<tr><th>" + escapeHtml(labelHeader) + "</th>" +
    columns.map((c) => `<th>${escapeHtml(c.label)}</th>`).join("") + "</tr>";

  if (!rows.length) {
    tbody.innerHTML = "";
    const wrap = table.parentElement;
    let note = wrap.querySelector(".empty-note");
    if (!note) {
      note = document.createElement("div");
      note.className = "empty-note";
      wrap.appendChild(note);
    }
    note.textContent = emptyMessage;
    table.style.display = "none";
    return;
  }
  table.style.display = "";
  const wrap = table.parentElement;
  const note = wrap.querySelector(".empty-note");
  if (note) note.remove();

  tbody.innerHTML = rows.map((r) =>
    "<tr><td>" + escapeHtml(r[labelKey]) + "</td>" +
    columns.map((c) => `<td>${c.fmt(r[c.key])}</td>`).join("") + "</tr>"
  ).join("");
}

function renderFacility() {
  setPeriodLabel("facility-period-label");
  const rows = visitsForPeriod().filter(
    (r) => r.provider === state.rollupAllProviders && r.category === state.rollupAllCategories
  );
  const byFacility = new Map();
  const duplicates = new Set();
  for (const r of rows) {
    if (!state.knownFacilities.includes(r.facility)) continue;
    if (byFacility.has(r.facility)) duplicates.add(r.facility);
    byFacility.set(r.facility, r);
  }
  const out = [];
  for (const f of state.knownFacilities) {
    if (byFacility.has(f)) out.push({ facility: f, ...byFacility.get(f) });
  }
  if (out.length) {
    const missing = state.knownFacilities.filter((f) => !byFacility.has(f));
    const label = missing.length
      ? `Combined (partial — ${out.length} of ${state.knownFacilities.length} facilities; missing ${missing.join(", ")})`
      : "Combined (computed)";
    out.push({ facility: label, ...sumVisitMetrics(Array.from(byFacility.values())) });
  }
  renderTable("facility-table", "facility", "Facility", VISIT_COLUMNS, out,
    "No visit data for this period.");
  renderIntegrityNote("facility-panel", duplicates.size
    ? `Data integrity warning: duplicate rows found for ${Array.from(duplicates).join(", ")} — figures above may be wrong.`
    : null);
}

function providerCategoryFacilityFilter(rows) {
  if (state.facility === COMBINED_FACILITY) return rows;
  return rows.filter((r) => r.facility === state.facility);
}

function renderProvider() {
  setPeriodLabel("provider-period-label");
  let rows = visitsForPeriod().filter(
    (r) => r.provider !== state.rollupAllProviders && r.category === state.rollupAllCategories
  );
  rows = providerCategoryFacilityFilter(rows);
  const duplicates = detectDuplicateKeys(rows, ["facility", "provider"]);
  const out = state.facility === COMBINED_FACILITY
    ? groupBySum(rows, "provider", sumVisitMetrics)
    : rows.map((r) => ({ provider: r.provider, ...r }));
  out.sort((a, b) => b.visit_count - a.visit_count);
  renderTable("provider-table", "provider", "Provider", VISIT_COLUMNS, out,
    "No provider-level visit data for this period/facility.");
  renderIntegrityNote("provider-panel", duplicates.size
    ? `Data integrity warning: duplicate rows found for facility/provider ${Array.from(duplicates).join(", ")} — figures above may be wrong.`
    : null);
}

function renderCategory() {
  setPeriodLabel("category-period-label");
  let rows = visitsForPeriod().filter(
    (r) => r.provider === state.rollupAllProviders && r.category !== state.rollupAllCategories
  );
  rows = providerCategoryFacilityFilter(rows);
  const duplicates = detectDuplicateKeys(rows, ["facility", "category"]);
  const out = state.facility === COMBINED_FACILITY
    ? groupBySum(rows, "category", sumVisitMetrics)
    : rows.map((r) => ({ category: r.category, ...r }));
  out.sort((a, b) => b.visit_count - a.visit_count);
  renderTable("category-table", "category", "Category", VISIT_COLUMNS, out,
    "No category-level visit data for this period/facility.");
  renderIntegrityNote("category-panel", duplicates.size
    ? `Data integrity warning: duplicate rows found for facility/category ${Array.from(duplicates).join(", ")} — figures above may be wrong.`
    : null);
}

const PAYER_COLUMNS = [
  { key: "paid_visit_count", label: "Paid Visits", fmt: fmtCount },
  { key: "total_collected", label: "Collected", fmt: fmtMoney },
  { key: "avg_collection_per_paid_visit", label: "Avg / Paid Visit", fmt: fmtMoney },
];

function renderPayer() {
  setPeriodLabel("payer-period-label");
  let rows = state.bundle.payer_procedure_summary.filter(
    (r) => r.period_type === state.periodType && r.period === state.period
  );
  rows = providerCategoryFacilityFilter(rows);
  const duplicates = detectDuplicateKeys(rows, ["facility", "payer_group"]);
  const out = state.facility === COMBINED_FACILITY
    ? groupBySum(rows, "payer_group", sumPayerMetrics)
    : rows.map((r) => ({ payer_group: r.payer_group, ...r }));
  out.sort((a, b) => b.total_collected - a.total_collected);
  renderTable("payer-table", "payer_group", "Payer Group", PAYER_COLUMNS, out,
    "No payer data for this period/facility.");
  renderIntegrityNote("payer-panel", duplicates.size
    ? `Data integrity warning: duplicate rows found for facility/payer group ${Array.from(duplicates).join(", ")} — figures above may be wrong.`
    : null);
}

const FINANCIAL_COLUMNS = [
  { key: "revenue", label: "Revenue", fmt: fmtMoney },
  { key: "expenses", label: "Expenses", fmt: fmtMoney },
  { key: "net_income", label: "Net Income", fmt: fmtMoney },
];

function renderFinancial() {
  let rows = state.bundle.financial_summary.filter((r) => r.period === state.financialMonth);
  if (state.facility !== COMBINED_FACILITY) {
    rows = rows.filter((r) => r.facility === state.facility);
  } else {
    rows = rows.filter((r) => state.knownFacilities.includes(r.facility));
  }
  const out = rows.map((r) => ({ facility: r.facility, ...r }));
  if (state.facility === COMBINED_FACILITY && rows.length) {
    const present = new Set(rows.map((r) => r.facility));
    const missing = state.knownFacilities.filter((f) => !present.has(f));
    const revenue = round2(rows.reduce((s, r) => s + r.revenue, 0));
    const expenses = round2(rows.reduce((s, r) => s + r.expenses, 0));
    const label = missing.length
      ? `Combined (partial — missing ${missing.join(", ")})`
      : "Combined (computed)";
    out.push({ facility: label, revenue, expenses, net_income: round2(revenue - expenses) });
  }
  renderTable("financial-table", "facility", "Facility", FINANCIAL_COLUMNS, out,
    "No financial data for this month.");
}

function renderFreshness() {
  const rows = state.manifest.source_table_freshness.slice().sort((a, b) =>
    a.table_name.localeCompare(b.table_name)
  );
  const table = document.getElementById("freshness-table");
  table.querySelector("thead").innerHTML =
    "<tr><th>Table</th><th>Status</th><th>Age (days)</th></tr>";
  table.querySelector("tbody").innerHTML = rows.map((r) =>
    `<tr><td>${escapeHtml(r.table_name)}</td>` +
    `<td class="verdict-${escapeHtml(r.verdict)}">${escapeHtml(r.verdict)}</td>` +
    `<td>${r.age_days.toFixed(1)}</td></tr>`
  ).join("");
}

// --- CSV export ----------------------------------------------------------
//
// Serializes exactly what is currently rendered in the table — data already
// fetched from the approved bundle, reformatted client-side. No new request,
// no new field is ever introduced here.

function downloadTableCsv(tableId, name) {
  const table = document.getElementById(tableId);
  const rows = Array.from(table.querySelectorAll("tr"));
  const csv = rows.map((tr) =>
    Array.from(tr.querySelectorAll("th,td"))
      .map((cell) => csvEscape(cell.textContent))
      .join(",")
  ).join("\r\n");
  if (!csv) return;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${name}_${state.periodType}_${state.period}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// Spreadsheet formula injection: a cell starting with =, +, -, or @ can be
// interpreted as a formula by Excel/Sheets/LibreOffice when the CSV is
// opened. Rendered values here always come from the approved bundle (not
// user input), but provider/category/payer names are a free-ish business
// dimension (publish_schema._kind_provider_name has no character-class
// restriction beyond length/shape), so this is real defense-in-depth, not a
// theoretical concern. A leading apostrophe is the standard mitigation:
// every major spreadsheet app renders it as forcing plain-text, not part of
// the value.
const CSV_FORMULA_TRIGGER_RE = /^[=+\-@]/;

function csvEscape(value) {
  let s = String(value);
  if (CSV_FORMULA_TRIGGER_RE.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

init();
