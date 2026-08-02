"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const { SBA_BOARD } = require("./sba-inbound");
const {
  SBA_MAIN_BOARD_COLUMN_IDS,
  SBA_MAIN_BOARD_FIELD_COLUMNS,
  SBA_QUALIFICATION_COLUMN_IDS,
  buildSbaMondayUpdateValues,
  buildSbaQualificationSessionPatch,
  selectBestSbaMondayMatch
} = require("./sba-monday-persistence");

const metadata = {
  columns: [
    { id: SBA_BOARD.columns.firstName, title: "First_Name", type: "text" },
    { id: SBA_BOARD.columns.lastName, title: "Last_Name", type: "text" },
    { id: SBA_BOARD.columns.email, title: "Email", type: "email" },
    { id: SBA_BOARD.columns.phoneNumber, title: "Phone", type: "phone" },
    { id: SBA_BOARD.columns.taxes, title: "Taxes", type: "text" },
    { id: SBA_BOARD.columns.updatedDate, title: "Updated_date", type: "date" },
    { id: SBA_BOARD.columns.zip, title: "Zip", type: "numbers" },
    { id: SBA_BOARD.columns.leadId, title: "Lead_id", type: "text" },
    { id: SBA_BOARD.columns.fundingGoal, title: "Funding Goal", type: "text" },
    { id: SBA_BOARD.columns.source, title: "Source", type: "text" },
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

function mondayMatch({
  id,
  updatedAt = "2026-01-01T00:00:00Z",
  createdAt = "2025-01-01T00:00:00Z",
  fields = {}
}) {
  return {
    id: String(id),
    updated_at: updatedAt,
    created_at: createdAt,
    group: { id: "topics" },
    column_values: Object.entries(fields).map(([columnId, text]) => ({
      id: columnId,
      text
    }))
  };
}

test("zero phone matches remain unresolved so the caller item is created", () => {
  assert.equal(selectBestSbaMondayMatch([]), null);
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /const item = await createInboundCallerItem\(initialData/);
});

test("exactly one phone match selects that existing item", () => {
  const item = mondayMatch({ id: "12268683564" });
  const selected = selectBestSbaMondayMatch([item]);
  assert.equal(selected.item.id, "12268683564");
  assert.equal(selected.selection_reason, "only_phone_match");
});

test("multiple phone matches prefer the populated SBA lead", () => {
  const blankNewerItem = mondayMatch({
    id: "300",
    updatedAt: "2026-07-01T00:00:00Z"
  });
  const populatedItem = mondayMatch({
    id: "200",
    updatedAt: "2026-06-01T00:00:00Z",
    fields: {
      [SBA_BOARD.columns.firstName]: "Avery",
      [SBA_BOARD.columns.lastName]: "Stone",
      [SBA_BOARD.columns.email]: "avery@example.com"
    }
  });
  const selected = selectBestSbaMondayMatch([blankNewerItem, populatedItem]);
  assert.equal(selected.item.id, "200");
  assert.equal(selected.selection_reason, "strongest_profile_data");
});

test("multiple populated phone matches prefer the most recently updated item", () => {
  const older = mondayMatch({
    id: "400",
    updatedAt: "2026-06-01T00:00:00Z",
    fields: { [SBA_BOARD.columns.email]: "avery@example.com" }
  });
  const newer = mondayMatch({
    id: "350",
    updatedAt: "2026-07-01T00:00:00Z",
    fields: { [SBA_BOARD.columns.email]: "avery@example.com" }
  });
  const selected = selectBestSbaMondayMatch([older, newer]);
  assert.equal(selected.item.id, "350");
  assert.equal(selected.selection_reason, "most_recently_updated");
});

test("multiple phone matches always resolve deterministically and never return null", () => {
  const selected = selectBestSbaMondayMatch([
    mondayMatch({ id: "900" }),
    mondayMatch({ id: "901" })
  ]);
  assert.ok(selected?.item);
  assert.equal(selected.item.id, "901");
  assert.equal(selected.selection_reason, "highest_item_id");
});

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
  const omitted = [];
  const values = buildSbaMondayUpdateValues({
    data: {
      city: "Tampa",
      first_name: "Avery",
      last_name: "Stone",
      email: "avery@example.com",
      phone: "+18135551212",
      updated_date: "2026-08-01T12:00:00.000Z"
    },
    metadata,
    onSkippedColumn: (entry) => omitted.push(entry)
  });
  assert.equal(values.text_mm4nfn2e, undefined);
  assert.ok(omitted.some((entry) =>
    entry.field === "city" && entry.reason === "sba_main_board_mapping_missing"
  ));
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

test("last name and lead source use the exact supplied SBA columns", () => {
  const values = buildSbaMondayUpdateValues({
    data: {
      last_name: "Stone",
      source: "Inbound - Website"
    },
    metadata
  });
  assert.equal(SBA_BOARD.columns.lastName, "text_mm3mwx5w");
  assert.equal(SBA_BOARD.columns.source, "text_mm5vdf21");
  assert.equal(values.text_mm3mwx5w, "Stone");
  assert.equal(values.text_mm5vdf21, "Inbound - Website");
  assert.equal(values.text_mm3mx5w, undefined);
});

test("funding goal uses the exact supplied SBA text column", () => {
  const values = buildSbaMondayUpdateValues({
    data: { funding_goal: "$100,000" },
    metadata
  });
  assert.equal(SBA_BOARD.columns.fundingGoal, "text_mm5vct3n");
  assert.equal(values.text_mm5vct3n, "$100,000");
});

test("SBA Source supports acquisition classification values", () => {
  for (const source of ["Inbound - Website", "Inbound - Phone", "Outbound"]) {
    const values = buildSbaMondayUpdateValues({ data: { source }, metadata });
    assert.equal(values.text_mm5vdf21, source);
  }
});

test("all valid supplied SBA intake fields retain their existing Monday mappings", () => {
  const omitted = [];
  const values = buildSbaMondayUpdateValues({
    data: {
      first_name: "Avery",
      last_name: "Stone",
      phone: "+18135551212",
      email: "avery@example.com",
      city: "Tampa",
      zip: "33602",
      business_entity_type: "LLC",
      entity_status: "Complete",
      taxes: "Filed",
      tax_status: "Complete",
      estimated_credit_score: "680+",
      credit_status: "Done",
      gross_monthly_revenue: "$5,000 - $25,000",
      income_status: "Done",
      updated_date: "2026-08-01T12:00:00.000Z",
      lead_id: "LEAD-100",
      funding_goal: "$100,000",
      source: "Inbound - Website"
    },
    metadata: {
      columns: [
        ...metadata.columns,
        {
          id: SBA_BOARD.columns.taxStatus,
          title: "Tax_Status",
          settings: { labels: { 1: "Complete" } }
        }
      ]
    },
    onSkippedColumn: (entry) => omitted.push(entry)
  });
  for (const columnId of [
    SBA_BOARD.columns.firstName,
    SBA_BOARD.columns.lastName,
    SBA_BOARD.columns.phoneNumber,
    SBA_BOARD.columns.email,
    SBA_BOARD.columns.zip,
    SBA_BOARD.columns.businessEntityType,
    SBA_BOARD.columns.entityStatus,
    SBA_BOARD.columns.taxes,
    SBA_BOARD.columns.taxStatus,
    SBA_BOARD.columns.estimatedCreditScore,
    SBA_BOARD.columns.creditStatus,
    SBA_BOARD.columns.grossMonthlyRevenue,
    SBA_BOARD.columns.incomeStatus,
    SBA_BOARD.columns.updatedDate,
    SBA_BOARD.columns.leadId,
    SBA_BOARD.columns.fundingGoal,
    SBA_BOARD.columns.source
  ]) {
    assert.ok(Object.hasOwn(values, columnId), `${columnId} must be in the mutation payload`);
  }
  assert.equal(values.text_mm4nfn2e, undefined);
  assert.ok(omitted.some((entry) => entry.field === "city"));
});

test("live qualification mutation payload keeps valid fields when City is unmapped", () => {
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
      "dropdown_mm3mpa8p"
    ].sort()
  );
});

test("SBA inbound allowlist excludes DPA, subitem, and stale City IDs", () => {
  for (const forbiddenId of [
    "text_mm4nfn2e",
    "color_mm571hke",
    "text_mm57ngpn",
    "phone_mm5790vb",
    "person",
    "status",
    "date0",
    "subitems_mm1kzcng"
  ]) {
    assert.equal(SBA_MAIN_BOARD_COLUMN_IDS.includes(forbiddenId), false);
  }
  assert.equal(SBA_MAIN_BOARD_FIELD_COLUMNS.city, null);
});

test("metadata validation omits an invalid optional mapping without discarding valid fields", () => {
  const omitted = [];
  const values = buildSbaMondayUpdateValues({
    data: { city: "Tampa", first_name: "Avery", taxes: "Filed" },
    metadata,
    onSkippedColumn: (entry) => omitted.push(entry)
  });
  assert.deepEqual(values, {
    [SBA_BOARD.columns.firstName]: "Avery",
    [SBA_BOARD.columns.taxes]: "Filed"
  });
  assert.ok(omitted.some((entry) =>
    entry.field === "city" && entry.reason === "sba_main_board_mapping_missing"
  ));
});

test("resolved item and SBA board IDs reach change_multiple_column_values", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const updateStart = server.indexOf("async function updateInboundCallerItem");
  const updateEnd = server.indexOf("async function updateInboundCallerFromSession", updateStart);
  const updateSource = server.slice(updateStart, updateEnd);
  assert.match(updateSource, /change_multiple_column_values\(board_id: \$boardId, item_id: \$itemId/);
  assert.match(updateSource, /boardId: MONDAY_BOARD_ID,\s*itemId: String\(itemId\)/);
  assert.match(updateSource, /"\[MONDAY_WRITE\]", "payload_prepared"/);
  assert.match(updateSource, /"\[MONDAY_WRITE\]", "invalid_column"/);
  assert.match(updateSource, /delete pendingColumnValues\[invalidColumnId\]/);
  assert.equal(SBA_BOARD.mainBoardId, "18414546873");
  assert.match(updateSource, /item_id: String\(itemId\)/);
});

test("successful SBA writes log the logical field and exact column ID", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /"\[MONDAY_WRITE\]", "field_written"/);
  assert.match(server, /logical_field: sbaLogicalFieldForColumnId\(columnId\)/);
  assert.match(server, /column_id: columnId/);
});

test("phone-origin Source is only supplied when creating an unmatched lead", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const syncStart = server.indexOf("async function syncInboundMondayCaller");
  const syncEnd = server.indexOf("async function ensureInboundMondayCaller", syncStart);
  const syncSource = server.slice(syncStart, syncEnd);
  assert.match(syncSource, /source: "Inbound - Phone"/);
  assert.match(syncSource, /createInboundCallerItem\(initialData/);
  assert.match(syncSource, /if \(existing\) \{[\s\S]*?updateInboundCallerItem\(resolvedItemId, \{\s*phone: initialData\.phone,\s*updated_date:/);
  assert.doesNotMatch(
    syncSource.match(/if \(existing\) \{[\s\S]*?\n    \}/)?.[0] || "",
    /source:/
  );
});

test("Monday missing-column diagnostics identify the rejected column for isolation", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const source = server.match(
    /function findMondayInvalidColumnId\(error\) \{[\s\S]*?\n\}/
  )?.[0];
  assert.ok(source, "findMondayInvalidColumnId source must be present");
  const findMondayInvalidColumnId = new Function(
    "cleanText",
    `${source}; return findMondayInvalidColumnId;`
  )((value, maximum) => String(value || "").trim().slice(0, maximum));
  const invalidColumnId = findMondayInvalidColumnId({
    mondayErrors: [{
      message: "This column ID doesn't exist for the board",
      extensions: {
        error_reason: "store.monday.automation.error.missing_column",
        diagnostic_data: { column_id: "stale_optional_column" }
      }
    }]
  });
  assert.equal(invalidColumnId, "stale_optional_column");
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

test("resolved duplicate item ID persists and is reused by qualification mutations", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const syncStart = server.indexOf("async function syncInboundMondayCaller");
  const syncEnd = server.indexOf("async function ensureInboundMondayCaller", syncStart);
  const syncSource = server.slice(syncStart, syncEnd);
  assert.match(syncSource, /"multiple_matches_resolved"/);
  assert.match(syncSource, /if \(!item\?\.id\) \{\s*throw new Error/);
  assert.match(syncSource, /const resolvedItemId = String\(item\.id\)/);
  assert.match(syncSource, /UPDATE ai_calls SET monday_item_id = \$2/);
  assert.match(syncSource, /call\.call_id,\s*resolvedItemId/);
  assert.match(syncSource, /"item_id_persisted"/);
  assert.doesNotMatch(syncSource, /item: null/);

  const qualificationStart = server.indexOf(
    "async function persistSbaQualificationFieldsToMonday"
  );
  const qualificationEnd = server.indexOf("async function lookupOutboundCaller", qualificationStart);
  const qualificationSource = server.slice(qualificationStart, qualificationEnd);
  assert.match(qualificationSource, /const itemId = cleanText\(call\?\.monday_item_id/);
  assert.match(qualificationSource, /updateInboundCallerItem\(itemId, qualificationPatch/);
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
