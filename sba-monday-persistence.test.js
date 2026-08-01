"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const { SBA_BOARD } = require("./sba-inbound");
const {
  SBA_QUALIFICATION_COLUMN_IDS,
  buildSbaMondayUpdateValues,
  buildSbaQualificationSessionPatch
} = require("./sba-monday-persistence");

const metadata = {
  columns: [
    {
      id: SBA_BOARD.columns.businessEntityType,
      title: "Business Entity Type",
      settings: { labels: [{ name: "LLC" }, { name: "S-corp" }] }
    },
    {
      id: SBA_BOARD.columns.estimatedCreditScore,
      title: "Estimated Credit Score",
      settings: { labels: [{ name: "640 - 679" }, { name: "680+" }] }
    },
    {
      id: SBA_BOARD.columns.grossMonthlyRevenue,
      title: "Gross Monthly Revenue",
      settings: { labels: [{ name: "$5,000 - $25,000" }] }
    },
    {
      id: SBA_BOARD.columns.entityStatus,
      title: "Entity_Status",
      settings: { labels: { 1: "Complete" } }
    },
    {
      id: SBA_BOARD.columns.creditStatus,
      title: "Credit_Status",
      settings: { labels: { 1: "Done" } }
    },
    {
      id: SBA_BOARD.columns.incomeStatus,
      title: "Income_Status",
      settings: { labels: { 1: "Done" } }
    }
  ]
};

test("save_inbound_caller_context schema accepts every qualification field", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const start = server.indexOf('"save_inbound_caller_context"');
  const end = server.indexOf('"lookup_existing_sba_lead"', start);
  const schema = server.slice(start, end);
  for (const field of [
    "city",
    "state",
    "business_entity_type",
    "time_in_business",
    "estimated_credit_score",
    "gross_monthly_revenue",
    "first_name",
    "last_name",
    "email",
    "phone_number"
  ]) {
    assert.match(schema, new RegExp(`\\b${field}:`));
  }
  assert.match(server, /persistSbaQualificationFieldsToMonday\(\s*call\.call_id/);
});

test("entity answer maps to Business Entity Type and Entity_Status", () => {
  const values = buildSbaMondayUpdateValues({
    data: { business_entity_type: "LLC", entity_status: "Complete" },
    metadata
  });
  assert.deepEqual(values[SBA_BOARD.columns.businessEntityType], { labels: ["LLC"] });
  assert.deepEqual(values[SBA_BOARD.columns.entityStatus], { label: "Complete" });
});

test("credit answer maps to Estimated Credit Score and Credit_Status", () => {
  const values = buildSbaMondayUpdateValues({
    data: { estimated_credit_score: "680+", credit_status: "Done" },
    metadata
  });
  assert.deepEqual(values[SBA_BOARD.columns.estimatedCreditScore], { labels: ["680+"] });
  assert.deepEqual(values[SBA_BOARD.columns.creditStatus], { label: "Done" });
});

test("revenue answer maps to Gross Monthly Revenue and Income_Status", () => {
  const values = buildSbaMondayUpdateValues({
    data: { gross_monthly_revenue: "$5,000 - $25,000", income_status: "Done" },
    metadata
  });
  assert.deepEqual(values[SBA_BOARD.columns.grossMonthlyRevenue], {
    labels: ["$5,000 - $25,000"]
  });
  assert.deepEqual(values[SBA_BOARD.columns.incomeStatus], { label: "Done" });
});

test("city and contact values map to supplied SBA columns", () => {
  const values = buildSbaMondayUpdateValues({
    data: {
      city: "Tampa",
      first_name: "Avery",
      last_name: "Stone",
      email: "avery@example.com",
      phone: "+18135551212",
      updated_date: "2026-08-01T12:00:00.000Z"
    },
    metadata
  });
  assert.equal(values[SBA_BOARD.columns.city], "Tampa");
  assert.equal(values[SBA_BOARD.columns.firstName], "Avery");
  assert.equal(values[SBA_BOARD.columns.lastName], "Stone");
  assert.deepEqual(values[SBA_BOARD.columns.email], {
    email: "avery@example.com",
    text: "avery@example.com"
  });
  assert.deepEqual(values[SBA_BOARD.columns.phoneNumber], {
    phone: "+18135551212",
    countryShortName: "US"
  });
  assert.deepEqual(values[SBA_BOARD.columns.updatedDate], { date: "2026-08-01" });
});

test("live qualification mutation payload contains all four required column IDs", () => {
  const values = buildSbaMondayUpdateValues({
    data: {
      city: "Tampa",
      business_entity_type: "LLC",
      entity_status: "Complete",
      estimated_credit_score: "680+",
      credit_status: "Done",
      gross_monthly_revenue: "$5,000 - $25,000",
      income_status: "Done",
      updated_date: "2026-08-01T12:00:00.000Z"
    },
    metadata
  });
  assert.deepEqual(
    Object.keys(values).sort(),
    [
      "color_mm5626jc",
      "color_mm56cyba",
      "color_mm56g2kn",
      "date_mm3mzfd4",
      "dropdown_mm3m39yk",
      "dropdown_mm3m9731",
      "dropdown_mm3mpa8p",
      "text_mm4nfn2e"
    ].sort()
  );
});

test("inbound phone comparison normalizes common Monday formats", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const source = server.match(/function normalizePhone\(value\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(source, "normalizePhone source must be present");
  const normalizePhone = new Function(
    "cleanText",
    `${source}; return normalizePhone;`
  )((value, maximum) => String(value || "").trim().slice(0, maximum));
  for (const formatted of [
    "+18135551212",
    "(813) 555-1212",
    "813-555-1212"
  ]) {
    assert.equal(normalizePhone(formatted), "+18135551212");
  }
});

test("missing monday item IDs trigger linkage recovery before updates", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /"MONDAY_ITEM_ID_MISSING"/);
  assert.match(server, /await ensureInboundMondayCaller\(call\);\s*call = \(await getCallById\(call\.call_id\)\)/);
  assert.match(server, /await ensureInboundMondayCaller\(call\);\s*call = \(await getCallById\(call\.call_id\)\) \|\| call;\s*inboundLog\("\[MONDAY_LINK\]", "incoming_call_link_result"/);
  assert.match(server, /UPDATE ai_calls SET monday_item_id = \$2/);
});

test("production-safe Monday diagnostics cover linkage, tool, and mutation stages", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /"\[MONDAY_LINK\]", "phone_search_started"/);
  assert.match(server, /"\[MONDAY_LINK\]", "item_id_persisted"/);
  assert.match(server, /"\[MONDAY_SAVE_TOOL\]", "tool_invoked"/);
  assert.match(server, /"\[MONDAY_UPDATE\]", "qualification_update_attempt"/);
  assert.match(server, /"\[MONDAY_UPDATE\]", "graphql_response"/);
  assert.match(server, /"\[MONDAY_UPDATE\]", "mutation_confirmed"/);
  assert.doesNotMatch(server, /"\[MONDAY_UPDATE\]", "raw_graphql_response"/);
});

test("blank qualification values produce no Monday overwrites", () => {
  const values = buildSbaMondayUpdateValues({
    data: {
      city: " ",
      business_entity_type: null,
      estimated_credit_score: "",
      gross_monthly_revenue: undefined,
      email: ""
    },
    metadata
  });
  assert.deepEqual(values, {});
});

test("State and Time in Business remain in session without invented columns", () => {
  const patch = buildSbaQualificationSessionPatch({
    state: "Florida",
    time_in_business: "4 years"
  });
  assert.deepEqual(patch, {
    time_in_business: "4 years",
    state: "Florida"
  });
  assert.equal(SBA_QUALIFICATION_COLUMN_IDS.state, undefined);
  assert.equal(SBA_QUALIFICATION_COLUMN_IDS.time_in_business, undefined);
  assert.deepEqual(buildSbaMondayUpdateValues({ data: patch, metadata }), {});
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /Location: \$\{\[result\.city, result\.state\]/);
  assert.match(server, /Time in business: \$\{cleanText\(result\.time_in_business/);
});
