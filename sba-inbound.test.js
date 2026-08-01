"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {
  SBA_BOARD,
  SBA_INBOUND_SCRIPT,
  SBA_INTENTS,
  SBA_OPENING
} = require("./sba-inbound");

test("uses the verified SBA board mapping", () => {
  assert.equal(SBA_BOARD.mainBoardId, "18414546873");
  assert.equal(SBA_BOARD.subitemBoardId, "18414546876");
  assert.equal(SBA_BOARD.columns.lastName, "text_mm3mx5w");
  assert.equal(SBA_BOARD.columns.incomeStatus, "color_mm56cyba");
  assert.equal(SBA_BOARD.columns.city, "text_mm4nfn2e");
  assert.equal(SBA_BOARD.columns.zip, "numeric_mm4nnp3g");
});

test("defines the exact SBA opening and four supported intents", () => {
  assert.equal(
    SBA_OPENING,
    "Thank you for calling the SBA Help Center. This is Daisy, your virtual funding assistant. How can I help you today?"
  );
  assert.deepEqual(SBA_INTENTS, [
    "EXISTING_FUNDING_PREVIEW",
    "NEW_FUNDING_REQUEST",
    "QUALIFICATION_OR_FUNDING_AMOUNT",
    "GENERAL_FUNDING_QUESTION"
  ]);
});

test("existing website lead uses stored SBA profile without recollection", () => {
  assert.match(SBA_INBOUND_SCRIPT, /If one lead matched/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not recollect fields that the caller confirms/i);
  assert.match(SBA_INBOUND_SCRIPT, /confirm stored Gross Monthly Revenue/i);
});

test("new funding inquiry collects a progressive minimum profile", () => {
  assert.match(SBA_INBOUND_SCRIPT, /If no lead matched/i);
  assert.match(SBA_INBOUND_SCRIPT, /Obtain first and last name/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not ask for all contact fields at once/i);
  assert.match(SBA_INBOUND_SCRIPT, /What are you primarily looking to use the funding for/i);
});

test("qualification, amount, rates, and program answers remain preliminary", () => {
  assert.match(SBA_INBOUND_SCRIPT, /do not underwrite, approve, guarantee eligibility/i);
  assert.match(SBA_INBOUND_SCRIPT, /opportunities up to \$5,000,000/i);
  assert.match(SBA_INBOUND_SCRIPT, /don't want to quote you something inaccurate/i);
  assert.match(SBA_INBOUND_SCRIPT, /SBA 7\(a\)/i);
  assert.doesNotMatch(SBA_INBOUND_SCRIPT, /down payment|homebuyer|realtor|mortgage/i);
});

test("interruption and normal closing retain the existing call lifecycle", () => {
  assert.match(SBA_INBOUND_SCRIPT, /Ask one primary question, stop, and wait/i);
  assert.match(SBA_INBOUND_SCRIPT, /Allow interruption/i);
  assert.match(SBA_INBOUND_SCRIPT, /Do not add a second closing or trigger a reconnect/i);
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
  assert.match(server, /async function findInboundCallersByPhone\(phone\)/);
  assert.match(server, /normalizePhone\(value\?\.text\) === normalized/);
  assert.match(server, /matches\.length === 1/);
  assert.match(server, /profile: sbaProfileFromMondayItem\(matches\[0\]\)/);
  assert.match(server, /existing_profile_loaded: Boolean\(existing && profile\)/);
});

test("SBA Monday updates use supplied IDs and nonblank field mapping", () => {
  const server = fs.readFileSync(path.join(__dirname, "server.js"), "utf8");
  assert.match(server, /function buildSbaMondayUpdateValues/);
  assert.match(server, /if \(value\) values\[columnId\] = value/);
  assert.match(server, /inboundMondayColumnByTitle\(metadata, \[columnTitle\]\)/);
  assert.doesNotMatch(server, /buildInboundMondayUpdateValues\(/);
});
