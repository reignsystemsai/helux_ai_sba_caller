"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  WIX_SBA_ANALYZER_PATH,
  createWixSbaAnalyzerHandlers,
  normalizeTaxFilingStatus,
  normalizeYearsInBusiness
} = require("./wix-sba-analyzer");

function mockResponse() {
  return {
    headers: {},
    statusCode: 200,
    body: null,
    ended: false,
    set(name, value) { this.headers[name] = value; return this; },
    vary(value) { this.headers.Vary = value; return this; },
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
    end() { this.ended = true; return this; }
  };
}

function request(body) {
  return {
    body,
    headers: { origin: "https://www.sbahelpcenter.com" },
    get(name) { return this.headers[String(name).toLowerCase()]; }
  };
}

function makeHandlers(overrides = {}) {
  const updates = [];
  const logs = [];
  const handlers = createWixSbaAnalyzerHandlers({
    findItem: async (itemId) => ({
      id: itemId,
      board: { id: "18414546873" }
    }),
    updateItem: async (itemId, patch) => {
      updates.push({ itemId, patch });
      return {
        id: itemId,
        sba_written_logical_fields: Object.keys(patch)
      };
    },
    log: (prefix, event, details) => logs.push({ prefix, event, details }),
    boardId: "18414546873",
    cleanText: (value, maximum = 255) => {
      const text = value === undefined || value === null
        ? ""
        : String(value).trim();
      return text ? text.slice(0, maximum) : null;
    },
    normalizeCreditScore: (value) => String(value).trim(),
    normalizeRevenueRange: (value) => String(value).trim(),
    ...overrides
  });
  return { handlers, updates, logs };
}

function validBody() {
  return {
    monday_item_id: "12692535403",
    updateData: {
      monthly_business_expenses: "$8,500",
      years_in_business: "2 - 5 years",
      tax_filing_status: "I have - 2yrs tax returns",
      average_ending_bank_balance: "$14,000",
      estimated_credit_score: "680+",
      gross_monthly_revenue: "$5,000 - $25,000",
      estimated_cash_flow: "$2,000",
      funding_range: "$50,000 - $75,000"
    }
  };
}

test("analyzer update endpoint uses the Wix bridge contract", () => {
  assert.equal(WIX_SBA_ANALYZER_PATH, "/api/intake/sba/update");
});

test("years-in-business labels normalize to meaningful values", () => {
  assert.equal(normalizeYearsInBusiness("Less than 1 year"), 0);
  assert.equal(normalizeYearsInBusiness("1 - 2 years"), 1);
  assert.equal(normalizeYearsInBusiness("2 - 5 years"), 2);
  assert.equal(normalizeYearsInBusiness("5+ years"), 5);
  assert.throws(() => normalizeYearsInBusiness(10), /scoring points/);
  assert.throws(() => normalizeYearsInBusiness(15), /scoring points/);
});

test("tax filing status accepts only the supplied human-readable labels", () => {
  assert.equal(
    normalizeTaxFilingStatus("I have - 3yr tax returns"),
    "I have - 3yr tax returns"
  );
  assert.throws(
    () => normalizeTaxFilingStatus("three years"),
    /Unsupported tax_filing_status/
  );
});

test("analyzer updates the existing item without forwarding scoring-only fields", async () => {
  const { handlers, updates, logs } = makeHandlers();
  const res = mockResponse();
  await handlers.post(request(validBody()), res);

  assert.equal(res.statusCode, 200);
  assert.equal(res.body.success, true);
  assert.equal(res.body.monday_item_id, "12692535403");
  assert.equal(updates.length, 1);
  assert.equal(updates[0].itemId, "12692535403");
  assert.deepEqual(updates[0].patch, {
    monthly_business_expenses: "$8,500",
    years_in_business: 2,
    tax_filing_status: "I have - 2yrs tax returns",
    average_ending_bank_balance: "$14,000",
    estimated_credit_score: "680+",
    gross_monthly_revenue: "$5,000 - $25,000"
  });
  assert.equal("estimated_cash_flow" in updates[0].patch, false);
  assert.equal("funding_range" in updates[0].patch, false);
  assert.deepEqual(logs.map(({ event }) => event), [
    "received",
    "monday_update_success"
  ]);
});

test("missing or unknown item IDs fail without calling the update mutation", async () => {
  const missing = makeHandlers();
  const missingRes = mockResponse();
  await missing.handlers.post(request({ updateData: validBody().updateData }), missingRes);
  assert.equal(missingRes.statusCode, 400);
  assert.equal(missing.updates.length, 0);

  const unknown = makeHandlers({ findItem: async () => null });
  const unknownRes = mockResponse();
  await unknown.handlers.post(request(validBody()), unknownRes);
  assert.equal(unknownRes.statusCode, 404);
  assert.equal(unknown.updates.length, 0);
});

test("analyzer returns failure unless Monday confirms every requested field", async () => {
  const { handlers, logs } = makeHandlers({
    updateItem: async (itemId) => ({
      id: itemId,
      sba_written_logical_fields: ["estimated_credit_score"]
    })
  });
  const res = mockResponse();
  await handlers.post(request(validBody()), res);
  assert.equal(res.statusCode, 502);
  assert.equal(res.body.success, false);
  assert.match(res.body.error, /did not confirm fields/);
  assert.equal(logs.at(-1).event, "monday_update_failed");
});
