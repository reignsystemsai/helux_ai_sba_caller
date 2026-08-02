"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const {
  WIX_SBA_INTAKE_PATH,
  createWixSbaIntakeHandlers
} = require("./wix-sba-intake");

function mockResponse() {
  return {
    headers: {},
    statusCode: 200,
    body: undefined,
    ended: false,
    set(name, value) {
      this.headers[name] = value;
      return this;
    },
    vary(value) {
      this.headers.Vary = value;
      return this;
    },
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
    end() {
      this.ended = true;
      return this;
    }
  };
}

function validBody() {
  return {
    first_name: "Avery",
    last_name: "Stone",
    full_name: "Avery Stone",
    email: "avery@example.com",
    phone: "(404) 555-0199",
    funding_goal: "$100,000",
    business_entity_type: "LLC",
    estimated_credit_score: "680+",
    gross_monthly_revenue: "$25,000 - $150,000",
    sample_cash_flow: "$9,000",
    estimated_funding: "$75,000",
    funding_readiness_score: "82",
    lead_source: "SBA Help Center Wix",
    page_url: "https://www.sbahelpcenter.com/readiness",
    submitted_at: "2026-08-01T16:00:00.000Z",
    trigger_outbound: true
  };
}

function makeHandlers(overrides = {}) {
  const calls = [];
  const logs = [];
  const handlers = createWixSbaIntakeHandlers({
    createItem: async (data) => {
      calls.push(data);
      return { id: "12268683564" };
    },
    log: (prefix, event, details) => logs.push({ prefix, event, details }),
    boardId: "18414546873",
    cleanText: (value, maximum = 255) => {
      const text = value === undefined || value === null ? "" : String(value).trim();
      return text ? text.slice(0, maximum) : null;
    },
    normalizeEmail: (value) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value).trim())
      ? String(value).trim().toLowerCase()
      : null,
    normalizePhone: (value) => {
      const digits = String(value || "").replace(/\D/g, "");
      return digits.length === 10 ? `+1${digits}` : `+${digits}`;
    },
    validPhone: (value) => /^\+[1-9]\d{7,14}$/.test(value),
    normalizeEntityType: (value) => String(value).trim(),
    normalizeCreditScore: (value) => String(value).trim(),
    normalizeRevenueRange: (value) => String(value).trim(),
    ...overrides
  });
  return { handlers, calls, logs };
}

function request(body = {}, origin = "https://www.sbahelpcenter.com") {
  return {
    body,
    headers: { origin },
    get(name) {
      return this.headers[String(name).toLowerCase()];
    }
  };
}

test("Wix SBA intake is registered at the requested public path", () => {
  assert.equal(WIX_SBA_INTAKE_PATH, "/api/intake/sba");
});

test("OPTIONS returns 204 with the required CORS headers", () => {
  const { handlers } = makeHandlers();
  const res = mockResponse();
  handlers.options(request(), res);
  assert.equal(res.statusCode, 204);
  assert.equal(res.ended, true);
  assert.equal(res.headers["Access-Control-Allow-Origin"], "https://www.sbahelpcenter.com");
  assert.equal(res.headers["Access-Control-Allow-Methods"], "POST, OPTIONS");
  assert.equal(res.headers["Access-Control-Allow-Headers"], "Content-Type");
});

test("CORS allows the apex sbahelpcenter.com origin", () => {
  const { handlers } = makeHandlers();
  const res = mockResponse();
  handlers.options(request({}, "https://sbahelpcenter.com"), res);
  assert.equal(res.statusCode, 204);
  assert.equal(res.headers["Access-Control-Allow-Origin"], "https://sbahelpcenter.com");
});

test("missing a required field returns 400", async () => {
  const { handlers, calls } = makeHandlers();
  const body = validBody();
  delete body.funding_goal;
  const res = mockResponse();
  await handlers.post(request(body), res, (error) => { throw error; });
  assert.equal(res.statusCode, 400);
  assert.equal(res.body.success, false);
  assert.match(res.body.error, /funding_goal/);
  assert.equal(calls.length, 0);
});

test("POST accepts valid Wix JSON, writes through the SBA item adapter, and returns success", async () => {
  const { handlers, calls, logs } = makeHandlers();
  const res = mockResponse();
  await handlers.post(request(validBody()), res, (error) => { throw error; });

  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, {
    success: true,
    monday_item_id: "12268683564",
    outbound_triggered: false
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].phone, "+14045550199");
  assert.equal(calls[0].updated_date, "2026-08-01");
  assert.deepEqual(logs.map(({ prefix, event }) => [prefix, event]), [
    ["[WIX_SBA_INTAKE]", "received"],
    ["[WIX_SBA_INTAKE]", "monday_write_success"],
    ["[WIX_SBA_INTAKE]", "outbound_triggered"]
  ]);
  assert.equal(JSON.stringify(logs).includes("avery@example.com"), false);
  assert.equal(JSON.stringify(logs).includes("4045550199"), false);
});
