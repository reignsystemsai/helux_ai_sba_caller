"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {
  SBA_BOARD,
  SBA_FINAL_THANK_YOU,
  SBA_INBOUND_SCRIPT,
  SBA_INTENTS,
  SBA_OPENING,
  buildSbaScheduledClosing
} = require("./sba-inbound");

function assertOrdered(source, fragments) {
  let previous = -1;
  for (const fragment of fragments) {
    const current = source.indexOf(fragment);
    assert.ok(current > previous, `${fragment} must appear in flow order`);
    previous = current;
  }
}

test("uses the verified SBA board mapping", () => {
  assert.equal(SBA_BOARD.mainBoardId, "18414546873");
  assert.equal(SBA_BOARD.subitemBoardId, "18414546876");
  assert.equal(SBA_BOARD.columns.lastName, "text_mm3mwx5w");
  assert.equal(SBA_BOARD.columns.source, "text_mm5vdf21");
  assert.equal(SBA_BOARD.columns.incomeStatus, "color_mm56cyba");
  assert.equal(SBA_BOARD.columns.city, null);
  assert.equal(SBA_BOARD.columns.zip, "numeric_mm4nnp3g");
});

test("opening is exact and contains no virtual-assistant wording", () => {
  assert.equal(
    SBA_OPENING,
    "Thank you for calling the SBA Help Center. This is Daisy. How can I help you today?"
  );
  assert.doesNotMatch(SBA_OPENING, /virtual assistant|virtual funding assistant/i);
});

test("normalizes the revised three-path flow", () => {
  assert.deepEqual(SBA_INTENTS, [
    "FUNDING_AMOUNT",
    "QUALIFICATION",
    "READY_TO_START"
  ]);
});

test("unknown caller follows location, path, qualification, scheduling, and closing order", () => {
  assertOrdered(SBA_INBOUND_SCRIPT, [
    "How can I help you today?",
    "what city and state are you calling from?",
    "are you mainly calling because you'd like to know how much funding",
    "Do you have a business entity such as",
    "How many years have you been in business?",
    "About what would you say your credit score is?",
    "gross monthly revenue?",
    "may I have your first and last name?",
    "have I answered all of your questions?",
    "You're calling from the number ending in {caller_phone_last_four}, correct?",
    "what's a good email address for you?",
    "When would be a good time for me to follow up",
    "Just to confirm"
  ]);
  assert.match(SBA_INBOUND_SCRIPT, /Ask only one primary question at a time/i);
});

test("business name and industry are not required core qualification questions", () => {
  const coreStart = SBA_INBOUND_SCRIPT.indexOf("FOUR CORE QUALIFICATION QUESTIONS");
  const coreEnd = SBA_INBOUND_SCRIPT.indexOf("READINESS RECOMMENDATION", coreStart);
  const coreFlow = SBA_INBOUND_SCRIPT.slice(coreStart, coreEnd);
  assert.match(coreFlow, /contains only entity, time in business, estimated credit, and gross monthly revenue/i);
  assert.match(coreFlow, /Never proactively ask for the business name, industry, type of work, business description/i);
  assert.match(coreFlow, /Ask about a non-core detail only when the caller's specific question makes that detail necessary/i);
  assert.match(coreFlow, /Do not collect a non-core field merely because a tool schema/i);
  for (const prohibitedQuestion of [
    "What is the name of your business?",
    "What kind of work do you do?",
    "What industry is your business in?",
    "What type of business do you operate?"
  ]) {
    assert.doesNotMatch(SBA_INBOUND_SCRIPT, new RegExp(prohibitedQuestion.replace(/[?]/g, "\\?"), "i"));
  }
  assert.match(SBA_INBOUND_SCRIPT, /Never ask for business name, industry, type of work, business description, employees, expenses, debt, tax returns, or documentation merely to complete the summary/i);
});

test("end-of-call contact verification precedes scheduling", () => {
  assert.match(SBA_INBOUND_SCRIPT, /Phone verification status is already verified, do not ask again/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not ask the caller to repeat the full number/i);
  assert.match(SBA_INBOUND_SCRIPT, /phone_number and phone_verified true/i);
  assert.match(SBA_INBOUND_SCRIPT, /corrected phone replaces the original caller phone/i);
  assert.match(SBA_INBOUND_SCRIPT, /Save the normalized email immediately with save_inbound_caller_context/i);
  assert.match(SBA_INBOUND_SCRIPT, /Read it back only when clarification is genuinely necessary/i);
});

test("SBA capture wiring separates location and persists corrected contact data", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /return "caller_city_state"/);
  assert.match(server, /\{ city: location\.city, state: location\.state \}/);
  assert.match(server, /return "caller_phone_confirmation"/);
  assert.match(server, /return "caller_phone_correction"/);
  assert.match(server, /phone = COALESCE\(\$3, phone\)/);
  assert.match(server, /\{ phone_number: correctedPhone, phone_verified: true \}/);
  assert.match(server, /replaceAll\("\{caller_phone_last_four\}", callerPhoneLastFour\)/);
  assert.match(server, /replaceAll\("\{customer_first_name\}", callerName\)/);
});

test("corrected callback phone normalization accepts digits and spoken digits", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const normalizeSource = server.match(/function normalizePhone\(value\) \{[\s\S]*?\n\}/)?.[0];
  const spokenSource = server.match(/function normalizeInboundSpokenPhone\(value\) \{[\s\S]*?\n\}/)?.[0];
  const validSource = server.match(/function validE164Phone\(value\) \{[\s\S]*?\n\}/)?.[0];
  assert.ok(normalizeSource && spokenSource && validSource);
  const normalizeInboundSpokenPhone = new Function(
    "cleanText",
    `${normalizeSource}; ${spokenSource}; ${validSource}; return normalizeInboundSpokenPhone;`
  )((value, maximum) => String(value || "").trim().slice(0, maximum));
  assert.equal(normalizeInboundSpokenPhone("813-555-1212"), "+18135551212");
  assert.equal(
    normalizeInboundSpokenPhone("eight one three five five five one two one two"),
    "+18135551212"
  );
  assert.equal(normalizeInboundSpokenPhone("not a phone number"), null);
});

test("qualification fields write progressively with sanitized SBA diagnostics", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /"\[SBA_FIELD_CAPTURED\]", "fields_captured"/);
  assert.match(server, /"\[SBA_MONDAY_WRITE_ATTEMPT\]", "qualification_fields"/);
  assert.match(server, /"\[SBA_MONDAY_WRITE_SUCCESS\]", "qualification_fields"/);
  assert.match(server, /"\[SBA_MONDAY_WRITE_FAILED\]", "qualification_fields"/);
  assert.match(server, /persistSbaQualificationFieldsToMonday\(\s*call\.call_id,\s*savedFields/);
  assert.doesNotMatch(server, /SBA_FIELD_CAPTURED[\s\S]{0,300}(?:email:|phone_number:|city:|state:)/);
  assert.match(server, /persistFinalInboundSession\(/);
  assert.match(server, /sba_written_logical_fields: writtenLogicalFields/);
  assert.match(server, /captured_logical_fields: logicalFields/);
});

test("existing Monday lead does not repeat confirmed core values", () => {
  assert.match(SBA_INBOUND_SCRIPT, /Entity, estimated credit, and gross monthly revenue already stored and confirmed count as completed/i);
  assert.match(SBA_INBOUND_SCRIPT, /Ask only the next missing question/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not ask either entity question when the stored entity type was already confirmed/i);
});

test("caller questions are answered before qualification resumes", () => {
  assert.match(SBA_INBOUND_SCRIPT, /answer it naturally before returning to the next unanswered qualification question/i);
  assert.match(SBA_INBOUND_SCRIPT, /Of course\. What else can I answer for you\?/i);
  assert.match(SBA_INBOUND_SCRIPT, /Is there anything else I can answer for you\?/i);
});

test("qualification transitions prohibit internal narration and repetitive confirmations", () => {
  assert.match(SBA_INBOUND_SCRIPT, /Never speak internal thinking, planning, deliberation, workflow selection, or next-step selection aloud/i);
  assert.match(SBA_INBOUND_SCRIPT, /Never say "Let me think about the next step/i);
  assert.match(SBA_INBOUND_SCRIPT, /Never use more than one acknowledgment phrase in a turn/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not repeat or summarize the caller's answer/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not say "thank you" after routine entity, time-in-business, credit, or revenue answers/i);
  assert.match(SBA_INBOUND_SCRIPT, /Confirm or clarify only when the answer is unclear/i);
  assert.match(SBA_INBOUND_SCRIPT, /Required scheduling confirmation remains unchanged/i);
  assert.match(SBA_INBOUND_SCRIPT, /clear credit answer such as "about 700" should lead directly to a brief acknowledgment and the gross-monthly-revenue question/i);
});

test("ready-to-start path still completes qualification", () => {
  assert.match(SBA_INBOUND_SCRIPT, /READY_TO_START: the caller is ready to start/i);
  assert.match(SBA_INBOUND_SCRIPT, /The next step is our SBA Help Center readiness application/i);
  assert.match(SBA_INBOUND_SCRIPT, /Proceed to the next unanswered qualification question/i);
});

test("qualification language remains preliminary and safe", () => {
  assert.match(SBA_INBOUND_SCRIPT, /do not underwrite, approve, guarantee eligibility/i);
  assert.match(SBA_INBOUND_SCRIPT, /don't want to quote you something inaccurate/i);
  assert.match(SBA_INBOUND_SCRIPT, /SBA 7\(a\)/i);
  assert.doesNotMatch(SBA_INBOUND_SCRIPT, /down payment|homebuyer|realtor|mortgage/i);
});

test("scheduled and normal closings use the revised wording without reconnect", () => {
  assert.equal(
    buildSbaScheduledClosing("Avery"),
    "Excellent, Avery. I have that scheduled. In the meantime, you can complete the readiness application anytime at the SBA Help Center website. It should only take about two or three minutes, and there's no credit check to complete it. Thank you for calling the SBA Help Center. We look forward to helping you explore your business funding options. Have a great day."
  );
  assert.match(SBA_FINAL_THANK_YOU, /Have a great day\.$/);
  assert.match(SBA_INBOUND_SCRIPT, /Allow interruption/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not add another closing, keep talking, trigger a reconnect/i);
});

test("Twilio inbound HTTP and media-stream routes remain registered", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /app\.post\(\["\/incoming-call", "\/api\/v1\/twilio\/inbound"\]/);
  assert.match(server, /websocketUrl = `\$\{inboundWebsocketBaseUrl\(req\)\}\/api\/v1\/twilio\/media`/);
  assert.match(server, /requestUrl\.pathname !== "\/api\/v1\/twilio\/media"/);
  assert.match(server, /mediaServer\.handleUpgrade\(request, socket, head/);
});

test("existing caller lookup hydrates a unique SBA phone match", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /async function findInboundCallersByPhone\(phone, options = \{\}\)/);
  assert.match(server, /normalizePhone\(value\?\.text\) === normalized/);
  assert.match(server, /selectBestSbaMondayMatch\(candidates\)/);
  assert.match(server, /profile: sbaProfileFromMondayItem\(selection\.item\)/);
  assert.match(server, /existing_profile_loaded: Boolean\(existing && profile\)/);
});

test("SBA Monday updates use supplied IDs and nonblank field mapping", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  const adapter = fs.readFileSync(
    path.join(__dirname, "sba-monday-persistence.js"),
    "utf8"
  );
  assert.match(server, /buildSbaMondayUpdateValues/);
  assert.match(server, /persistSbaQualificationFieldsToMonday/);
  assert.match(adapter, /mappedSbaMainBoardColumn\(metadata, field, skip\)/);
  assert.match(adapter, /SBA_MAIN_BOARD_COLUMN_IDS/);
  assert.doesNotMatch(adapter, /columnByIdOrTitle/);
  assert.doesNotMatch(server, /buildInboundMondayUpdateValues\(/);
});
