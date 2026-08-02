const express = require("express");
const http = require("http");
const { randomUUID, createHash } = require("crypto");
const { Pool } = require("pg");
const twilio = require("twilio");
const WebSocket = require("ws");
const { WebSocketServer } = WebSocket;
const {
  KeyedSerialQueue,
  mondayDateValue: buildInboundMondayDateValue,
  normalizeLocalDate,
  normalizeLocalTime,
  persistLatestSession,
  retryTransientOperation,
  validateMondayEnvelope
} = require("./inbound-persistence");
const {
  SBA_BOARD,
  SBA_FINAL_THANK_YOU,
  SBA_INBOUND_SCRIPT,
  SBA_INTENTS,
  SBA_OPENING,
  buildSbaScheduledClosing
} = require("./sba-inbound");
const {
  SBA_MAIN_BOARD_COLUMN_IDS,
  SBA_QUALIFICATION_COLUMN_IDS,
  buildSbaMondayUpdateValues,
  buildSbaQualificationSessionPatch,
  sbaLogicalFieldForColumnId,
  selectBestSbaMondayMatch
} = require("./sba-monday-persistence");
const {
  WIX_SBA_INTAKE_PATH,
  createWixSbaIntakeHandlers
} = require("./wix-sba-intake");

/* Inlined production dependencies — formerly ./src modules */

const INLINE_INTENTS = Object.freeze({
  SAVE_CALL_PROGRESS: "save_call_progress",
  CALCULATE_PRELIMINARY_DTI: "calculate_preliminary_dti",
  SEND_RESOURCE_LINK: "send_resource_link",
  CREATE_SPECIALIST_HANDOFF: "create_specialist_handoff",
  TRANSFER_TO_SPECIALIST: "transfer_to_specialist",
  MARK_CONTACT_RESTRICTION: "mark_contact_restriction",
  CREATE_CONFIRMED_APPOINTMENT: "create_confirmed_appointment",
  COMPLETE_CALL: "complete_call"
});

const INLINE_TOOL_TO_INTENT = Object.freeze(Object.fromEntries(
  Object.entries(INLINE_INTENTS).map(([intent, toolName]) => [toolName, intent])
));

function inlineTool(name, description, properties, required) {
  return { type: "function", name, description, parameters: { type: "object", properties, required, additionalProperties: false } };
}

const BASE_REALTIME_TOOLS = Object.freeze([
  inlineTool("save_call_progress", "Save confirmed progress and an allowed conversation transition.", {
    current_state: { type: "string" }, next_state: { type: "string" }, answers: { type: "object" },
    sentiment: { type: "string", enum: ["positive", "neutral", "skeptical", "confused", "frustrated", "urgent", "excited", "hesitant", "fearful", "disappointed"] },
    notes: { type: "string" }, current_objective: { type: "string" }, last_confirmed_fact: { type: "string" }, pending_question: { type: ["string", "null"] }, next_best_action: { type: "string" }
  }, ["current_state", "next_state", "answers"]),
  inlineTool("calculate_preliminary_dti", "Calculate a preliminary DTI planning estimate.", {
    gross_monthly_household_income: { type: "number", minimum: 1 }, monthly_recurring_debt: { type: "number", minimum: 0 }
  }, ["gross_monthly_household_income", "monthly_recurring_debt"]),
  inlineTool("send_resource_link", "Send one approved resource after customer consent.", {
    resource_type: { type: "string", enum: ["application", "dti_calculator", "prephub", "credit_readiness", "tax_readiness", "employment_readiness"] },
    consent_confirmed: { type: "boolean" }
  }, ["resource_type", "consent_confirmed"]),
  inlineTool("create_specialist_handoff", "Create a structured specialist handoff.", {
    reason: { type: "string" }, priority: { type: "string", enum: ["normal", "high", "urgent"] }, summary: { type: "string" }
  }, ["reason", "priority", "summary"]),
  inlineTool("transfer_to_specialist", "Attempt a live transfer only after explicit agreement.", {
    reason: { type: "string" }, priority: { type: "string", enum: ["normal", "high", "urgent"] }, prospect_confirmed: { type: "boolean" }
  }, ["reason", "priority", "prospect_confirmed"]),
  inlineTool("mark_contact_restriction", "Apply a wrong-number, invalid-number, opt-out, or not-interested restriction.", {
    restriction_type: { type: "string", enum: ["wrong_number", "invalid_number", "do_not_call", "not_interested"] }, reason: { type: "string" }, stop_voice: { type: "boolean" }, stop_sms: { type: "boolean" }, stop_email: { type: "boolean" }
  }, ["restriction_type", "reason", "stop_voice", "stop_sms", "stop_email"]),
  inlineTool("create_confirmed_appointment", "Create a future phone appointment only after the customer confirms the complete local date, time, and timezone.", {
    customer_local_date: { type: "string", description: "Exact local date in YYYY-MM-DD format." },
    customer_local_time: { type: "string", description: "Exact local time including hour and minute." },
    callback_at: { type: "string", description: "Confirmed appointment as an ISO 8601 UTC timestamp." },
    timezone: { type: "string", enum: ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"] },
    timezone_label: { type: "string", enum: ["Eastern", "Central", "Mountain", "Pacific"] },
    callback_type: { type: "string", enum: ["call_one_rescheduled", "call_two_application_follow_up"] },
    callback_reason: { type: "string" },
    prospect_confirmed: { type: "boolean" },
    source_call_id: { type: "string" },
    discussion_summary: { type: "string" }
  }, ["customer_local_date", "customer_local_time", "callback_at", "timezone", "timezone_label", "callback_type", "callback_reason", "prospect_confirmed", "source_call_id", "discussion_summary"]),
  inlineTool("complete_call", "Record the connected-call result and sequence instruction.", {
    outcome: { type: "string", enum: ["qualified", "hot_transfer", "specialist_handoff", "application_link_sent", "dti_calculator_sent", "needs_review", "nurture", "voicemail", "no_answer", "busy", "not_interested", "wrong_number", "opt_out", "disconnected", "technical_failure", "agent_notified"] },
    next_action: { type: "string" }, summary: { type: "string" }, stop_sequence: { type: "boolean" }, pause_sequence: { type: "boolean" }
  }, ["outcome", "next_action", "summary", "stop_sequence", "pause_sequence"])
]);

const INLINE_STATES = Object.freeze([
  "identity_verification", "introduction", "trust_confirmation", "time_check",
  "roadmap", "need", "dpa_education", "knowledge_discovery",
  "timeline_discovery", "realtor_discovery", "lender_discovery", "urgency",
  "dti_offer", "dti_in_progress", "application_next_step",
  "nurture", "specialist_handoff", "closing", "completed"
]);

const INLINE_NEXT = Object.freeze({
  identity_verification: ["introduction", "closing"],
  introduction: ["trust_confirmation", "closing"],
  trust_confirmation: ["time_check", "closing"],
  time_check: ["roadmap", "closing"],
  roadmap: ["need", "closing"],
  need: ["dpa_education", "closing"],
  dpa_education: ["knowledge_discovery", "timeline_discovery", "closing"],
  knowledge_discovery: ["dpa_education", "timeline_discovery", "closing"],
  timeline_discovery: ["realtor_discovery", "nurture", "closing"],
  realtor_discovery: ["lender_discovery", "closing"],
  lender_discovery: ["urgency", "closing"],
  urgency: ["dti_offer", "application_next_step", "nurture", "closing"],
  dti_offer: ["dti_in_progress", "application_next_step", "closing"],
  dti_in_progress: ["application_next_step", "closing"],
  application_next_step: ["specialist_handoff", "closing"],
  nurture: ["closing", "completed"],
  specialist_handoff: ["closing", "completed"],
  closing: ["completed"]
});

const INLINE_LEGACY_ALIASES = Object.freeze({
  greeting: "identity_verification",
  readiness_confirmation: "trust_confirmation",
  qualification: "timeline_discovery",
  application_link_sent: "application_next_step",
  reconnect_pending: "identity_verification",
  reconnect_in_progress: "identity_verification"
});

function inlineNormalizeState(value) {
  const state = String(value || "").trim().toLowerCase();
  return INLINE_LEGACY_ALIASES[state] || (INLINE_STATES.includes(state) ? state : null);
}

function inlineCanTransition(from, to, options = {}) {
  const current = inlineNormalizeState(from);
  const next = inlineNormalizeState(to);
  if (!current || !next) return options.allowLegacy !== false;
  if (current === next || next === "closing") return true;
  return (INLINE_NEXT[current] || []).includes(next);
}

function inlineIntentFailureValidation(code, retryable = false) {
  return { valid: false, error: { code, retryable } };
}

function inlineValidateIntent(toolName, args = {}, context = {}) {
  if (!INLINE_TOOL_TO_INTENT[toolName]) return inlineIntentFailureValidation("UNKNOWN_INTENT");
  if (!args || typeof args !== "object" || Array.isArray(args)) return inlineIntentFailureValidation("INVALID_ARGUMENTS");
  if (toolName === "send_resource_link" && args.consent_confirmed !== true) return inlineIntentFailureValidation("SMS_CONSENT_REQUIRED");
  if (toolName === "transfer_to_specialist" && args.prospect_confirmed !== true) return inlineIntentFailureValidation("TRANSFER_CONFIRMATION_REQUIRED");
  if (toolName === "create_confirmed_appointment" && args.prospect_confirmed !== true) return inlineIntentFailureValidation("APPOINTMENT_CONFIRMATION_REQUIRED");
  if (toolName === "save_call_progress" && !inlineCanTransition(context.current_state || args.current_state, args.next_state, { allowLegacy: true })) return inlineIntentFailureValidation("INVALID_STATE_TRANSITION");
  return { valid: true, intent: INLINE_TOOL_TO_INTENT[toolName], error: null };
}

function inlineIntentSuccess(intent, customerSafeMessage, data = {}) {
  return { success: true, intent, customer_safe_message: customerSafeMessage, data, error: null };
}

function inlineIntentFailure(intent, code, retryable = false) {
  return { success: false, intent, customer_safe_message: null, data: {}, error: { code, retryable } };
}

const INLINE_ACTION_MESSAGES = Object.freeze({
  SAVE_CALL_PROGRESS: null,
  CALCULATE_PRELIMINARY_DTI: "Your preliminary DTI estimate is ready.",
  SEND_RESOURCE_LINK: "The requested resource was sent to your phone.",
  CREATE_SPECIALIST_HANDOFF: "Your specialist follow-up has been created.",
  TRANSFER_TO_SPECIALIST: "Your transfer is ready.",
  MARK_CONTACT_RESTRICTION: "Your contact preference has been recorded.",
  CREATE_CONFIRMED_APPOINTMENT: null,
  COMPLETE_CALL: null
});

const INLINE_INTENT_HANDLERS = Object.freeze({
  send_resource_link: (execute, call, args) => execute(call, "send_resource_link", args),
  save_call_progress: (execute, call, args) => execute(call, "save_call_progress", args),
  calculate_preliminary_dti: (execute, call, args) => execute(call, "calculate_preliminary_dti", args),
  create_specialist_handoff: (execute, call, args) => execute(call, "create_specialist_handoff", args),
  transfer_to_specialist: (execute, call, args) => execute(call, "transfer_to_specialist", args),
  mark_contact_restriction: (execute, call, args) => execute(call, "mark_contact_restriction", args),
  complete_call: (execute, call, args) => execute(call, "complete_call", args),
  create_confirmed_appointment: (execute, call, args) => execute(call, "create_confirmed_appointment", args)
});

function inlineIntentPublicData(result = {}) {
  const allowed = ["resource_type", "preliminary_dti_percent", "preliminary_dti_classification", "timezone", "timezone_label", "outcome", "sequence_status", "saved_fields", "current_state", "next_state", "confirmation_sms_sent", "callback_at", "customer_local_date", "customer_local_time", "callback_type", "callback_reason", "appointment_id", "next_action"];
  return Object.fromEntries(allowed.filter((key) => result[key] !== undefined).map((key) => [key, result[key]]));
}

async function routeIntent({ toolName, args, call, execute }) {
  const intent = INLINE_TOOL_TO_INTENT[toolName] || "UNKNOWN_INTENT";
  const validation = inlineValidateIntent(toolName, args, call || {});
  if (!validation.valid) return inlineIntentFailure(intent, validation.error.code, validation.error.retryable);
  try {
    const raw = await INLINE_INTENT_HANDLERS[toolName](execute, call, args);
    if (!raw || raw.success !== true) return inlineIntentFailure(intent, `${intent}_FAILED`, raw?.retryable === true);
    return inlineIntentSuccess(intent, INLINE_ACTION_MESSAGES[intent], inlineIntentPublicData(raw));
  } catch {
    return inlineIntentFailure(intent, `${intent}_FAILED`, true);
  }
}

const INLINE_RATE_RESPONSE = "That will be covered by your DPA Program Specialist when you speak with them. Interest rates and loan details depend on the individual homebuyer's situation, so I'm not permitted to discuss or estimate them.";
const INLINE_RATE_REDIRECT = "What I can help you with is getting started through the readiness application at dpahelpcenter.com.";
const SBA_RATE_RESPONSE = "Rates and terms vary by program, lender, credit profile, business financials, loan structure, and underwriting, so I don't want to quote you something inaccurate.";

function isInterestRateQuestion(value) {
  const text = String(value || "").toLowerCase();
  return /\b(?:interest|mortgage)\s+rates?\b|\brate\s+(?:today|now|lock|quote)\b/.test(text);
}

function inlineAssistantRateViolation(value) {
  const text = String(value || "").toLowerCase();
  return /\b(?:interest\s+)?rates?\b.{0,35}\b\d+(?:\.\d+)?\s*%|\b\d+(?:\.\d+)?\s*%.{0,35}\b(?:interest\s+)?rates?\b/.test(text) ||
    /\b(?:rates?|interest rates?)\s+(?:are|is|seem|look)\s+(?:high|low|good|bad)/.test(text) ||
    /\b(?:lock|wait for|expect|predict)\b.{0,30}\brates?\b/.test(text) ||
    /\brates?\b.{0,30}\b(?:rise|fall|drop|increase|decrease)\b/.test(text);
}

function interestRateResponse() {
  return `${INLINE_RATE_RESPONSE} ${INLINE_RATE_REDIRECT}`;
}

const INLINE_PROHIBITED_REQUESTS = Object.freeze([
  /social security|\bssn\b/i, /full date of birth|\bdob\b/i, /bank(?:ing)? (?:login|password)/i,
  /card number/i, /password/i, /one[- ]time (?:passcode|code)|\botp\b/i
]);

const INLINE_UNSCRIPTED_FILLER = Object.freeze([
  /\bbefore we dive deeper\b/i,
  /\bgreat[,.]?\s+i(?:'ll| will)\s+(?:get|collect|ask for)\s+(?:you|your|the caller'?s)?\s*email\b/i,
  /\blet me\s+(?:think|adjust|figure|consider|decide|check|make a note|wrap)\b/i,
  /\b(?:here'?s|this is)\s+what\s+(?:we|i)\s+(?:need|should|will)\s+(?:to\s+)?do next\b/i,
  /\bi can point you\s+(?:to|where|how)\s+(?:to\s+)?apply\b/i,
  /\bi can give you\s+(?:an?|the)\s+(?:general\s+)?example\b/i
]);

function inlineRequestsProhibitedInformation(value) {
  return INLINE_PROHIBITED_REQUESTS.some((pattern) => pattern.test(String(value || "")));
}

function guardAssistantOutput(value, callPhase = "INBOUND") {
  if (inlineAssistantRateViolation(value)) return { allowed: false, code: "INTEREST_RATE_POLICY", replacement: callPhase === "INBOUND" ? SBA_RATE_RESPONSE : interestRateResponse() };
  if (inlineRequestsProhibitedInformation(value)) return { allowed: false, code: "SENSITIVE_INFORMATION_REQUEST", replacement: callPhase === "INBOUND" ? "I don't need that sensitive information. We can continue with the non-sensitive business information needed for your funding profile." : "I don't need that sensitive information. Let's continue with the non-sensitive information needed for your DPA next step." };
  if (INLINE_UNSCRIPTED_FILLER.some((pattern) => pattern.test(String(value || "")))) {
    return {
      allowed: false,
      code: "UNSCRIPTED_FILLER",
      replacement: null
    };
  }
  return { allowed: true, code: null, replacement: null };
}

const INLINE_LISTENING = new Set(["mm hmm", "mmm hmm", "mhm", "uh huh", "right", "okay", "ok", "yeah", "i see", "got it"]);

function inlineNormalizeListening(value) {
  return String(value || "").toLowerCase().replace(/-/g, " ").replace(/[^a-z\s]/g, " ").replace(/\s+/g, " ").trim();
}

function isListeningAcknowledgement(value) {
  return INLINE_LISTENING.has(inlineNormalizeListening(value));
}

function semanticTurnDelay(value) {
  const text = String(value || "").toLowerCase();
  if (/\b(?:income|debt|payment|date|time|let me think|not sure|frustrat|confus|afraid)\b/.test(text)) return 750;
  return 350;
}

const REALTIME_DEFAULTS = Object.freeze({ transcriptDebounceMs: 350, meaningfulInterruptionMs: 700, silenceReminderMs: 8000 });

function buildRealtimeSession({ model, voice, transcriptionModel, instructions, tools }) {
  const input = {
    format: { type: "audio/pcmu" }, noise_reduction: { type: "near_field" },
    turn_detection: { type: "server_vad", threshold: 0.62, prefix_padding_ms: 300, silence_duration_ms: 500, create_response: false, interrupt_response: false, idle_timeout_ms: 12000 }
  };
  if (transcriptionModel) input.transcription = { model: transcriptionModel, language: "en" };
  return { type: "realtime", model, output_modalities: ["audio"], instructions, tools, tool_choice: "auto", audio: { input, output: { format: { type: "audio/pcmu" }, voice } } };
}

const INLINE_TIMEZONES = Object.freeze({
  Eastern: "America/New_York",
  Central: "America/Chicago",
  Mountain: "America/Denver",
  Pacific: "America/Los_Angeles"
});

const INLINE_CALLBACKS = Object.freeze({
  call_one_rescheduled: {
    reason: "Customer requested another time to complete Call One",
    nextAction: "Resume Call One at the scheduled time"
  },
  call_two_application_follow_up: {
    reason: "Application status, program options, and preliminary DTI follow-up",
    nextAction: "Complete the application before Call Two"
  }
});

class SchedulingError extends Error {
  constructor(code, message, statusCode = 422) {
    super(message);
    this.name = "SchedulingError";
    this.code = code;
    this.statusCode = statusCode;
  }
}

function inlineSchedulingClean(value, maximum = 4000) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text ? text.slice(0, maximum) : null;
}

function inlineNormalizeAppointmentTimezone(timezone, timezoneLabel) {
  const zone = inlineSchedulingClean(timezone, 100);
  const label = inlineSchedulingClean(timezoneLabel, 30);
  const zoneLabelMatch = Object.keys(INLINE_TIMEZONES).find(
    (candidate) => candidate.toLowerCase() === String(zone || "").toLowerCase()
  );
  const labelMatch = Object.keys(INLINE_TIMEZONES).find(
    (candidate) => candidate.toLowerCase() === String(label || "").toLowerCase()
  );
  const zoneMatch = Object.entries(INLINE_TIMEZONES).find(
    ([, candidate]) => candidate === zone
  );
  const resolvedLabel = zoneLabelMatch || zoneMatch?.[0] || labelMatch || null;
  const resolvedZone = zoneLabelMatch
    ? INLINE_TIMEZONES[zoneLabelMatch]
    : zoneMatch?.[1] || (labelMatch ? INLINE_TIMEZONES[labelMatch] : null);

  if (!resolvedLabel || !resolvedZone) {
    throw new SchedulingError(
      "INVALID_TIMEZONE",
      "Timezone must be Eastern, Central, Mountain, or Pacific."
    );
  }
  if (zone && !zoneLabelMatch && !zoneMatch) {
    throw new SchedulingError("INVALID_TIMEZONE", "Unsupported timezone.");
  }
  if (label && !labelMatch) {
    throw new SchedulingError("INVALID_TIMEZONE_LABEL", "Unsupported timezone label.");
  }
  if (zone && label && INLINE_TIMEZONES[labelMatch] !== resolvedZone) {
    throw new SchedulingError("TIMEZONE_MISMATCH", "Timezone and timezone label do not match.");
  }
  return { timezone: resolvedZone, timezoneLabel: resolvedLabel };
}

function inlineParseLocalDate(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(inlineSchedulingClean(value, 10) || "");
  if (!match) throw new SchedulingError("INVALID_DATE", "Date must use YYYY-MM-DD.");
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const check = new Date(Date.UTC(year, month - 1, day));
  if (
    check.getUTCFullYear() !== year ||
    check.getUTCMonth() !== month - 1 ||
    check.getUTCDate() !== day
  ) throw new SchedulingError("INVALID_DATE", "Date is not a valid calendar date.");
  return { year, month, day, value: match[0] };
}

function inlineParseLocalTime(value) {
  const text = inlineSchedulingClean(value, 20) || "";
  let match = /^(\d{1,2}):(\d{2})\s*([AaPp][Mm])$/.exec(text);
  let hour;
  let minute;
  if (match) {
    hour = Number(match[1]);
    minute = Number(match[2]);
    if (hour < 1 || hour > 12 || minute > 59) {
      throw new SchedulingError("INVALID_TIME", "Time is invalid.");
    }
    if (match[3].toLowerCase() === "pm" && hour !== 12) hour += 12;
    if (match[3].toLowerCase() === "am" && hour === 12) hour = 0;
  } else {
    match = /^(\d{2}):(\d{2})$/.exec(text);
    if (!match) {
      throw new SchedulingError("INVALID_TIME", "Time must include an exact hour and minute.");
    }
    hour = Number(match[1]);
    minute = Number(match[2]);
    if (hour > 23 || minute > 59) throw new SchedulingError("INVALID_TIME", "Time is invalid.");
  }
  return { hour, minute, value: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}` };
}

function inlineLocalPartsAt(date, timezone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  }).formatToParts(date);
  return Object.fromEntries(parts.filter((part) => part.type !== "literal").map((part) => [part.type, Number(part.value)]));
}

function localDateTimeToUtc(localDate, localTime, timezone) {
  const date = inlineParseLocalDate(localDate);
  const time = inlineParseLocalTime(localTime);
  const target = Date.UTC(date.year, date.month - 1, date.day, time.hour, time.minute);
  const matches = [];
  for (let deltaMinutes = -14 * 60; deltaMinutes <= 14 * 60; deltaMinutes += 1) {
    const candidate = new Date(target + deltaMinutes * 60000);
    const parts = inlineLocalPartsAt(candidate, timezone);
    if (
      parts.year === date.year && parts.month === date.month && parts.day === date.day &&
      parts.hour === time.hour && parts.minute === time.minute
    ) matches.push(candidate);
  }
  if (matches.length === 0) {
    throw new SchedulingError("NONEXISTENT_LOCAL_TIME", "That local time does not exist because of a daylight-saving transition.");
  }
  if (matches.length > 1) {
    throw new SchedulingError("AMBIGUOUS_LOCAL_TIME", "That local time occurs twice because of a daylight-saving transition; choose another exact time.");
  }
  return { callbackAt: matches[0], localDate: date.value, localTime: time.value };
}

function formatInboundFollowUpConfirmation(followUpRecord) {
  if (
    !followUpRecord ||
    followUpRecord.follow_up_declined === true ||
    !followUpRecord.follow_up_at ||
    !followUpRecord.follow_up_timezone
  ) {
    return "";
  }

  const followUpAt = new Date(followUpRecord.follow_up_at);
  if (Number.isNaN(followUpAt.getTime())) return "";

  const timezone = cleanText(followUpRecord.follow_up_timezone, 100);
  const timezoneLabel =
    Object.entries(INLINE_TIMEZONES).find(
      ([, candidate]) => candidate === timezone
    )?.[0] || "";
  if (!timezoneLabel) return "";

  const dateLabel = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    weekday: "long",
    month: "long",
    day: "numeric"
  }).format(followUpAt);
  const timeLabel = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    hour: "numeric",
    minute: "2-digit",
    hour12: true
  }).format(followUpAt);

  return `Your follow-up call is scheduled for ${dateLabel} at ${timeLabel} ${timezoneLabel} Time.`;
}

function inlineConfirmedTimezoneFromCall(call) {
  const result = call?.result || {};
  const payload = call?.payload || {};
  const candidates = [
    result.customer_timezone_confirmed === true
      ? [result.customer_timezone, result.customer_timezone_label, result.customer_timezone_confirmed_at]
      : null,
    payload.customer_timezone_confirmed === true
      ? [payload.customer_timezone || payload.timezone, payload.customer_timezone_label || payload.timezone_label, payload.customer_timezone_confirmed_at]
      : null
  ].filter(Boolean);
  for (const [timezone, label, confirmedAt] of candidates) {
    try {
      return {
        ...inlineNormalizeAppointmentTimezone(timezone, label),
        confirmedAt: inlineSchedulingClean(confirmedAt, 100)
      };
    } catch { /* Invalid saved values are ignored. */ }
  }
  return null;
}

function inlineCustomerKey(call) {
  if (inlineSchedulingClean(call.lead_id, 150)) return `lead:${inlineSchedulingClean(call.lead_id, 150)}`;
  if (inlineSchedulingClean(call.case_id, 150)) return `case:${inlineSchedulingClean(call.case_id, 150)}`;
  return `request:${inlineSchedulingClean(call.request_key, 320) || call.call_id}`;
}

async function inlineFindSavedTimezone(client, call) {
  const active = inlineConfirmedTimezoneFromCall(call);
  if (active) return active;
  const prior = await client.query(
    `SELECT result, payload FROM ai_calls
     WHERE call_id <> $1
       AND (($2::text IS NOT NULL AND case_id = $2) OR ($3::text IS NOT NULL AND lead_id = $3))
       AND (result->>'customer_timezone_confirmed' = 'true'
         OR payload->>'customer_timezone_confirmed' = 'true')
     ORDER BY updated_at DESC LIMIT 20`,
    [call.call_id, call.case_id || null, call.lead_id || null]
  );
  for (const row of prior.rows) {
    const saved = inlineConfirmedTimezoneFromCall(row);
    if (saved) return saved;
  }
  return null;
}

function inlineValidateCallback(callbackType, callbackReason) {
  const definition = INLINE_CALLBACKS[inlineSchedulingClean(callbackType, 80)];
  if (!definition) throw new SchedulingError("UNSUPPORTED_CALLBACK_TYPE", "Unsupported callback type.");
  if (inlineSchedulingClean(callbackReason, 500) !== definition.reason) {
    throw new SchedulingError("UNSUPPORTED_CALLBACK_REASON", "Callback reason does not match the callback type.");
  }
  return definition;
}

function inlineSchedulingPublicId(prefix) {
  return `${prefix}-${Date.now().toString(36).toUpperCase()}-${randomUUID().split("-")[0].toUpperCase()}`;
}

async function createConfirmedAppointment({ pool, input, now = new Date() }) {
  if (input?.prospect_confirmed !== true) {
    throw new SchedulingError("CONFIRMATION_REQUIRED", "Explicit customer confirmation is required.");
  }
  const localDate = inlineSchedulingClean(input.customer_local_date, 10);
  const localTime = inlineSchedulingClean(input.customer_local_time, 20);
  if (!localDate) throw new SchedulingError("DATE_REQUIRED", "Customer local date is required.");
  if (!localTime) throw new SchedulingError("TIME_REQUIRED", "Customer local time is required.");
  const callbackType = inlineSchedulingClean(input.callback_type, 80);
  const callbackReason = inlineSchedulingClean(input.callback_reason, 500);
  const callback = inlineValidateCallback(callbackType, callbackReason);
  const sourceCallId = inlineSchedulingClean(input.source_call_id, 100);
  if (!sourceCallId) throw new SchedulingError("SOURCE_CALL_REQUIRED", "Source call ID is required.");

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const callResult = await client.query("SELECT * FROM ai_calls WHERE call_id = $1 FOR UPDATE", [sourceCallId]);
    const call = callResult.rows[0];
    if (!call) throw new SchedulingError("CALL_NOT_FOUND", "Current call was not found.", 404);

    const savedTimezone = await inlineFindSavedTimezone(client, call);
    let timezoneSelection;
    if (inlineSchedulingClean(input.timezone, 100) || inlineSchedulingClean(input.timezone_label, 30)) {
      timezoneSelection = inlineNormalizeAppointmentTimezone(input.timezone, input.timezone_label);
      if (
        savedTimezone?.timezone === timezoneSelection.timezone &&
        savedTimezone?.timezoneLabel === timezoneSelection.timezoneLabel
      ) timezoneSelection.confirmedAt = savedTimezone.confirmedAt;
    } else {
      timezoneSelection = savedTimezone;
      if (!timezoneSelection) {
        throw new SchedulingError("TIMEZONE_REQUIRED", "A confirmed customer timezone is required.");
      }
    }

    const converted = localDateTimeToUtc(localDate, localTime, timezoneSelection.timezone);
    if (converted.callbackAt.getTime() <= now.getTime()) {
      throw new SchedulingError("APPOINTMENT_NOT_FUTURE", "Appointment must be in the future.");
    }

    const appointmentId = inlineSchedulingPublicId("APPT");
    const createdAt = now.toISOString();
    const nextAction = callback.nextAction;
    const key = inlineCustomerKey(call);
    const appointmentInsert = await client.query(
      `INSERT INTO scheduled_appointments (
         appointment_id, customer_key, source_call_id, case_id, lead_id,
         callback_at, customer_local_date, customer_local_time, timezone,
         timezone_label, callback_type, callback_reason, discussion_summary,
         prospect_confirmed, next_action, created_at, updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,TRUE,$14,$15,$15)
       ON CONFLICT (customer_key, callback_type, callback_at) DO NOTHING RETURNING *`,
      [
        appointmentId, key, call.call_id, call.case_id, call.lead_id,
        converted.callbackAt, converted.localDate, converted.localTime,
        timezoneSelection.timezone, timezoneSelection.timezoneLabel,
        callbackType, callbackReason, inlineSchedulingClean(input.discussion_summary, 4000),
        nextAction, now
      ]
    );
    if (!appointmentInsert.rows[0]) {
      throw new SchedulingError("DUPLICATE_APPOINTMENT", "This appointment already exists.", 409);
    }

    const attemptNumberResult = await client.query(
      "SELECT COALESCE(MAX(attempt_number), 0) + 1 AS next_number FROM call_attempts WHERE call_id = $1",
      [call.call_id]
    );
    const attemptId = inlineSchedulingPublicId("ATTEMPT");
    await client.query(
      `INSERT INTO call_attempts (
         attempt_id, call_id, attempt_number, call_leg, technical_status,
         attempt_type, idempotency_key, scheduled_for, appointment_id,
         callback_type, callback_reason, callback_timezone,
         callback_timezone_label, source_call_id
       ) VALUES ($1,$2,$3,1,'pending',$4,$5,$6,$7,$4,$8,$9,$10,$2)`,
      [
        attemptId, call.call_id, Number(attemptNumberResult.rows[0].next_number),
        callbackType, `appointment:${appointmentId}`, converted.callbackAt,
        appointmentId, callbackReason, timezoneSelection.timezone,
        timezoneSelection.timezoneLabel
      ]
    );

    const crm = {
      customer_timezone: timezoneSelection.timezone,
      customer_timezone_label: timezoneSelection.timezoneLabel,
      customer_timezone_confirmed: true,
      customer_timezone_confirmed_at: timezoneSelection.confirmedAt || createdAt,
      callback_at: converted.callbackAt.toISOString(),
      callback_local_date: converted.localDate,
      callback_local_time: converted.localTime,
      callback_timezone: timezoneSelection.timezone,
      callback_timezone_label: timezoneSelection.timezoneLabel,
      callback_reason: callbackReason,
      callback_type: callbackType,
      callback_confirmed: true,
      callback_created_at: createdAt,
      callback_source_call_id: call.call_id,
      appointment_id: appointmentId,
      next_action: nextAction,
      discussion_summary: inlineSchedulingClean(input.discussion_summary, 4000)
    };
    await client.query(
      `UPDATE ai_calls SET timezone = $2, next_action = $3,
         summary = COALESCE($4, summary), result = result || $5::jsonb,
         actions = actions || $6::jsonb, updated_at = NOW()
       WHERE call_id = $1`,
      [
        call.call_id, timezoneSelection.timezone, nextAction,
        crm.discussion_summary, JSON.stringify(crm),
        JSON.stringify([{ action: "create_confirmed_appointment", success: true, appointment_id: appointmentId, callback_at: crm.callback_at, callback_type: callbackType, created_at: createdAt }])
      ]
    );
    await client.query("COMMIT");
    return {
      success: true,
      callback_at: crm.callback_at,
      customer_local_date: converted.localDate,
      customer_local_time: converted.localTime,
      timezone: timezoneSelection.timezone,
      timezone_label: timezoneSelection.timezoneLabel,
      callback_type: callbackType,
      callback_reason: callbackReason,
      appointment_id: appointmentId,
      next_action: nextAction
    };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/*
 * HELUX AI INBOUND CALLS - DAISY INBOUND DEMO
 * Daisy, the SBA Help Center inbound virtual funding assistant.
 * monday.com failures never block or terminate a customer call.
 */

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const HELUX_API_KEY = process.env.HELUX_API_KEY;

const HELUX_BASE_URL = String(
  process.env.HELUX_BASE_URL || "https://helux-os.onrender.com"
).replace(/\/+$/, "");
const HELUX_RESULTS_PATH =
  process.env.HELUX_RESULTS_PATH || "/api/v1/calls/results";

const PUBLIC_BASE_URL = String(
  process.env.PUBLIC_BASE_URL || "https://helux-ai-inbound-calls.onrender.com"
).replace(/\/+$/, "");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-realtime-2.1";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "marin";
const OPENAI_TRANSCRIPTION_MODEL =
  process.env.OPENAI_TRANSCRIPTION_MODEL || "gpt-4o-mini-transcribe";

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN;
const TWILIO_FROM_NUMBER = process.env.TWILIO_FROM_NUMBER;
const SPECIALIST_PHONE_NUMBER = process.env.SPECIALIST_PHONE_NUMBER || "";
const OUTBOUND_LOOKUP_URL = cleanText(process.env.OUTBOUND_LOOKUP_URL, 1000);
const OUTBOUND_LOOKUP_API_KEY = cleanText(
  process.env.OUTBOUND_LOOKUP_API_KEY,
  1000
);

const DPA_APPLICATION_URL =
  process.env.DPA_APPLICATION_URL || "https://dpahelpcenter.com";
const DTI_CALCULATOR_URL =
  process.env.DTI_CALCULATOR_URL || "https://www.dpahelpcenter.com/dti";
const PREPHUB_URL =
  process.env.PREPHUB_URL || "https://www.dpahelpcenter.com/prephub";
const CREDIT_READINESS_URL =
  process.env.CREDIT_READINESS_URL || "https://www.creditjump.ai/";
const TAX_READINESS_URL =
  process.env.TAX_READINESS_URL || "https://www.estimatemytaxreturn.com/";
const EMPLOYMENT_READINESS_URL =
  process.env.EMPLOYMENT_READINESS_URL || "https://www.dpahelpcenter.com/job";

const DAISY_RESOURCE_LIBRARY = Object.freeze({
  application: {
    url: DPA_APPLICATION_URL,
    description: "DPA Help Center application"
  },
  dti_calculator: {
    url: DTI_CALCULATOR_URL,
    description: "DPA Help Center DTI calculator"
  },
  prephub: {
    url: PREPHUB_URL,
    description: "DPA Help Center Prephub"
  },
  credit_readiness: {
    url: CREDIT_READINESS_URL,
    description: "CreditJump credit-readiness resource"
  },
  tax_readiness: {
    url: TAX_READINESS_URL,
    description: "tax-readiness resource"
  },
  employment_readiness: {
    url: EMPLOYMENT_READINESS_URL,
    description: "employment-readiness resource"
  }
});

const OUTBOUND_CALLS_ENABLED = false;
const ENFORCE_CALL_CONSENT =
  String(process.env.ENFORCE_CALL_CONSENT || "false").toLowerCase() ===
  "true";
const DEFAULT_TIMEZONE =
  process.env.DEFAULT_TIMEZONE || "America/New_York";

/*
 * Daisy does not interrupt herself for a single VAD spike. A possible customer
 * interruption must remain active long enough to resemble sustained speech.
 * Short clicks, phone movement, dishes, static, and other brief sounds are
 * ignored locally even when the upstream VAD reports speech_started.
 */
const DAISY_SPEECH_CONFIRM_MS = Math.max(
  900,
  Number(process.env.DAISY_SPEECH_CONFIRM_MS || 1200)
);

const DAISY_MIN_TRANSCRIPT_SETTLE_MS = Math.max(
  450,
  Number(process.env.DAISY_MIN_TRANSCRIPT_SETTLE_MS || 550)
);

/* monday.com is optional and isolated from the live caller. */
const MONDAY_API_URL = "https://api.monday.com/v2";
const MONDAY_API_TOKEN = process.env.MONDAY_API_TOKEN || "";
const MONDAY_API_VERSION = process.env.MONDAY_API_VERSION || "2026-04";
const INBOUND_MONDAY_DIAGNOSTIC_CALL_ID = String(
  process.env.INBOUND_MONDAY_DIAGNOSTIC_CALL_ID || ""
).trim();
const INBOUND_MONDAY_DIAGNOSTICS_ONCE =
  String(process.env.INBOUND_MONDAY_DIAGNOSTICS_ONCE || "false")
    .toLowerCase() === "true";
const INBOUND_MONDAY = Object.freeze({
  boards: Object.freeze({
    inbound: SBA_BOARD.mainBoardId,
    subitems: SBA_BOARD.subitemBoardId
  }),
  groups: Object.freeze({
    newInboundCalls: SBA_BOARD.groups.newLeads,
    formLinkSent: SBA_BOARD.groups.newLeads,
    existingApplicantFollowUp: SBA_BOARD.groups.newLeads,
    transferredToOutbound: SBA_BOARD.groups.newLeads,
    closed: SBA_BOARD.groups.newLeads
  }),
  columns: SBA_BOARD.columns,
  subitemColumns: SBA_BOARD.subitemColumns
});
const MONDAY_BOARD_ID = INBOUND_MONDAY.boards.inbound;
const MONDAY_SUBITEM_BOARD_ID = INBOUND_MONDAY.boards.subitems;
const INBOUND_MONDAY_CONNECTED = Boolean(
  MONDAY_API_TOKEN && MONDAY_BOARD_ID && MONDAY_SUBITEM_BOARD_ID
);
const DPA_BOARD_ID = cleanText(process.env.DPA_BOARD_ID, 100);
const MONDAY_CALL_CONTROL_COLUMNS = Object.freeze({
  has_realtor: "color_mm57ev4f",
  applied_with_lender: "color_mm57bjwh",
  app_started_confirmation: "color_mm576a7j",
  time_frame: "color_mm57v24g"
});
const DPA_DEPARTMENT_COLUMNS = Object.freeze({
  app_started: "color_mm571hke",
  realtor_name: "text_mm57ngpn",
  realtor_phone: "phone_mm5790vb"
});
const MONDAY_SYNC_REQUESTED = false;
const MONDAY_SYNC_ENABLED = Boolean(
  MONDAY_SYNC_REQUESTED &&
    MONDAY_API_TOKEN &&
    MONDAY_BOARD_ID &&
    MONDAY_SUBITEM_BOARD_ID
);
const MONDAY_METADATA_CACHE_MS = Math.max(
  60000,
  Number(process.env.MONDAY_METADATA_CACHE_MS || 900000)
);
const MONDAY_REQUEST_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.MONDAY_REQUEST_TIMEOUT_MS || 12000)
);
const INBOUND_FINAL_PERSIST_TIMEOUT_MS = Math.max(
  5000,
  Number(process.env.INBOUND_FINAL_PERSIST_TIMEOUT_MS || 45000)
);
const MONDAY_SYNC_DEBOUNCE_MS = Math.max(
  100,
  Number(process.env.MONDAY_SYNC_DEBOUNCE_MS || 750)
);
const MONDAY_INBOUND_SYNC_ENABLED =
  String(process.env.MONDAY_INBOUND_SYNC_ENABLED || "true").toLowerCase() ===
  "true";
const MONDAY_WEBHOOK_SECRET = String(
  process.env.MONDAY_WEBHOOK_SECRET ||
    createHash("sha256")
      .update(`${HELUX_API_KEY}:${MONDAY_BOARD_ID}:monday-inbound`)
      .digest("hex")
      .slice(0, 32)
);

const REQUIRED_ENVIRONMENT = {
  DATABASE_URL,
  HELUX_API_KEY,
  OPENAI_API_KEY,
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  TWILIO_FROM_NUMBER
};

const missingEnvironment = Object.entries(REQUIRED_ENVIRONMENT)
  .filter(([, value]) => !value)
  .map(([key]) => key);

if (missingEnvironment.length) {
  console.error(
    `Missing required environment variables: ${missingEnvironment.join(", ")}`
  );
  process.exit(1);
}

const DOUG_CONFIG = Object.freeze({
  agentVersion: "daisy-sba-inbound-1.0.0",
  promptVersion: "daisy-inbound-sba-v1.0",
  toolVersion: "sba-inbound-actions-v1.0",
  knowledgeVersion: "sba-funding-general-v1",
  routingVersion: "sba-routing-v1",
  mondayAdapterVersion: "monday-sba-inbound-v1",
  voiceRules: {
    maximumResponseSeconds: 12,
    questionsPerTurn: 1,
    interruptible: true
  }
});

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000
});
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

const app = express();
app.set("trust proxy", true);
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: false }));

const server = http.createServer(app);
const mediaServer = new WebSocketServer({ noServer: true });

let mondayMetadataCache = null;
let mondayMetadataExpiresAt = 0;
let inboundMondayConnectionHealthy = false;
const mondaySyncTimers = new Map();
const mondaySyncChains = new Map();
const inboundMondayCallerPromises = new Map();
const inboundFinalPersistenceChains = new Map();
const inboundSessionPersistenceQueue = new KeyedSerialQueue();
let inboundMondayDiagnosticClaimedCallId = null;
let inboundMondayDiagnosticCompleted = false;

class HttpError extends Error {
  constructor(statusCode, message) {
    super(message);
    this.statusCode = statusCode;
  }
}

function cleanText(value, maximumLength = 255) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text ? text.slice(0, maximumLength) : null;
}

function normalizeBoolean(value) {
  if (value === true || value === false) return value;
  const normalized = String(value || "").trim().toLowerCase();
  if (["true", "yes", "1", "confirmed", "granted"].includes(normalized)) {
    return true;
  }
  if (["false", "no", "0", "denied", "revoked"].includes(normalized)) {
    return false;
  }
  return null;
}

function normalizeExplicitYesNo(value) {
  if (value === true || value === false) return value;
  const normalized = String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9'\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const negativePatterns = [
    /\bno\b/,
    /\bnope\b/,
    /\bnot yet\b/,
    /\bdon't\b/,
    /\bdo not\b/,
    /\bi don't have\b/,
    /\bi do not have\b/,
    /\bnot working with\b/,
    /\bwithout a lender\b/,
    /\bwithout a realtor\b/,
    /\bneed a lender\b/,
    /\bneed a realtor\b/
  ];
  if (negativePatterns.some((pattern) => pattern.test(normalized))) return false;

  const positivePatterns = [
    /\byes\b/,
    /\byeah\b/,
    /\byep\b/,
    /\byup\b/,
    /\bcorrect\b/,
    /\babsolutely\b/,
    /\bi do\b/,
    /\bi have one\b/,
    /\bi already have one\b/,
    /\bi'm working with\b/,
    /\bi am working with\b/
  ];
  if (positivePatterns.some((pattern) => pattern.test(normalized))) return true;
  return null;
}
function wholeNumberToWords(value) {
  const number = Math.trunc(Number(value));
  if (!Number.isSafeInteger(number) || number < 0) return null;
  if (number === 0) return "zero";
  const ones = [
    "", "one", "two", "three", "four", "five", "six", "seven", "eight",
    "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
    "sixteen", "seventeen", "eighteen", "nineteen"
  ];
  const tens = [
    "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy",
    "eighty", "ninety"
  ];
  const underThousand = (amount) => {
    const parts = [];
    if (amount >= 100) {
      parts.push(`${ones[Math.floor(amount / 100)]} hundred`);
      amount %= 100;
    }
    if (amount >= 20) {
      parts.push(tens[Math.floor(amount / 10)]);
      amount %= 10;
    }
    if (amount > 0) parts.push(ones[amount]);
    return parts.join(" ");
  };
  const scales = [
    [1_000_000_000, "billion"],
    [1_000_000, "million"],
    [1_000, "thousand"]
  ];
  let remaining = number;
  const words = [];
  for (const [scale, label] of scales) {
    if (remaining >= scale) {
      words.push(`${underThousand(Math.floor(remaining / scale))} ${label}`);
      remaining %= scale;
    }
  }
  if (remaining > 0) words.push(underThousand(remaining));
  return words.join(" ");
}

function formatIncomeForDaisy(value) {
  const numeric = Number(String(value ?? "").replace(/[$,\s]/g, ""));
  if (!Number.isFinite(numeric) || numeric < 0) return cleanText(value, 160);
  const wholeDollars = Math.round(numeric);
  const words = wholeNumberToWords(wholeDollars);
  const display = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(wholeDollars);
return words ? `${words} dollars` : display;
}

function normalizePhone(value) {
  const original = cleanText(value, 50);
  if (!original) return null;
  const digits = original.replace(/\D/g, "");
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length >= 11 && digits.length <= 15) return `+${digits}`;
  return original;
}

function normalizeInboundSpokenPhone(value) {
  const original = cleanText(value, 200);
  if (!original) return null;
  const digitBased = normalizePhone(original);
  if (validE164Phone(digitBased)) return digitBased;
  const spokenDigits = {
    zero: "0", oh: "0", o: "0", one: "1", two: "2", three: "3",
    four: "4", five: "5", six: "6", seven: "7", eight: "8", nine: "9"
  };
  const digits = original.toLowerCase().match(/[a-z]+|\d/g)?.map((token) =>
    /^\d$/.test(token) ? token : spokenDigits[token]
  ).filter((token) => token !== undefined).join("") || "";
  return validE164Phone(normalizePhone(digits)) ? normalizePhone(digits) : null;
}

function validE164Phone(value) {
  return /^\+[1-9]\d{7,14}$/.test(String(normalizePhone(value) || ""));
}

function normalizeTimeFrame(value) {
  const normalized = normalizeMondayKey(value);
  if ([
    "3060",
    "3060days",
    "30to60",
    "30days60days",
    "2months",
    "twomonths",
    "within2months",
    "withintwomonths"
  ].includes(normalized)) {
    return "30 - 60";
  }
  if (["6090", "6090days", "60to90", "60days90days"].includes(normalized)) {
    return "60 - 90";
  }
  if ([
    "withinsixmonths",
    "within6months",
    "36months",
    "3to6months",
    "4months",
    "fourmonths",
    "within4months",
    "withinfourmonths",
    "6months",
    "sixmonths"
  ].includes(normalized)) {
    return "Within six months";
  }
  if ([
    "morethansixmonths",
    "morethan6months",
    "over6months",
    "justlooking",
    "looking",
    "nurture",
    "1year",
    "oneyear",
    "12months",
    "twelvemonths"
  ].includes(normalized)) {
    return "More than six months";
  }
  return null;
}

function interestForTimeFrame(value) {
  const timeFrame = normalizeTimeFrame(value);
  if (timeFrame === "30 - 60") return "High";
  if (timeFrame === "60 - 90") return "Medium";
  if (timeFrame === "Within six months") return "Medium";
  if (timeFrame === "More than six months") return "Nurture";
  return null;
}

function normalizeDaisyAnswers(input) {
  const answers = input && typeof input === "object" ? { ...input } : {};
  const timeFrame = normalizeTimeFrame(answers.time_frame);
  if (timeFrame) {
    answers.time_frame = timeFrame;
    answers.interest_level = interestForTimeFrame(timeFrame);
  }
  for (const key of ["has_realtor", "applied_with_lender", "has_lender"]) {
    if (!Object.prototype.hasOwnProperty.call(answers, key)) continue;
    const normalized = normalizeExplicitYesNo(answers[key]);
    if (normalized === null) delete answers[key];
    else answers[key] = normalized ? "Yes" : "No";
  }
  if (answers.app_started_confirmation) {
    answers.app_started_confirmation = cleanText(
      answers.app_started_confirmation,
      80
    );
  }
  if (answers.application_link_sent !== undefined) {
    answers.application_link_sent = answers.application_link_sent === true;
  }
  return answers;
}

function normalizeTimezone(value) {
  const candidate = cleanText(value, 100) || DEFAULT_TIMEZONE;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(new Date());
    return candidate;
  } catch {
    return DEFAULT_TIMEZONE;
  }
}

function formatAssistanceAmount(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const numericValue = Number(
    String(value)
      .replace(/[$,\s]/g, "")
      .trim()
  );

  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return null;
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(numericValue);
}

function authenticateHelux(req, res, next) {
  const provided = req.headers["x-helux-key"];
  if (!provided || Array.isArray(provided) || provided !== HELUX_API_KEY) {
    return res.status(401).json({ success: false, error: "Unauthorized." });
  }
  next();
}

function createPublicId(prefix) {
  return `${prefix}-${Date.now()
    .toString(36)
    .toUpperCase()}-${randomUUID().split("-")[0].toUpperCase()}`;
}

function createStreamToken() {
  return randomUUID().replace(/-/g, "") + randomUUID().replace(/-/g, "");
}

function safetyIdentifier(call) {
  return createHash("sha256")
    .update(String(call.case_id || call.lead_id || call.call_id))
    .digest("hex");
}

function stableHash(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}

function websocketBaseUrl() {
  return PUBLIC_BASE_URL.replace(/^http:/, "ws:").replace(/^https:/, "wss:");
}

function inboundWebsocketBaseUrl(req) {
  const requestHost = cleanText(req.get("host"), 300);
  return requestHost ? `wss://${requestHost}` : websocketBaseUrl();
}

function callRequestKey(payload) {
  const caseId = cleanText(payload.case_id, 150);
  const leadId = cleanText(payload.lead_id, 150);
  if (caseId) return `case:${caseId}`;
  if (leadId) return `lead:${leadId}`;
  throw new HttpError(422, "case_id or lead_id is required.");
}

function terminalCallStatus(status) {
  return ["completed", "busy", "failed", "no-answer", "canceled", "cancelled"].includes(
    String(status || "").toLowerCase()
  );
}

function stopOutcome(outcome) {
  return [
    "qualified",
    "hot_transfer",
    "specialist_handoff",
    "application_link_sent",
    "dti_calculator_sent",
    "agent_notified",
    "needs_review",
    "not_interested",
    "wrong_number",
    "opt_out"
  ].includes(String(outcome || "").toLowerCase());
}

function confirmedConsent(payload) {
  const explicit = normalizeBoolean(
    payload.consent_confirmed ?? payload.ai_voice_consent
  );
  if (explicit !== null) return explicit;
  const status = String(payload.consent_status || "").toLowerCase();
  return ["confirmed", "granted", "approved", "yes"].includes(status);
}


const SUPPORTED_INBOUND_INTENTS = SBA_INTENTS;

function savedInboundIntent(call) {
  return [
    call?.intent,
    call?.result?.inbound_intent,
    call?.payload?.inbound_intent
  ]
    .map((value) => cleanText(value, 80))
    .find((value) => SUPPORTED_INBOUND_INTENTS.includes(value)) || null;
}

function inboundIntentStateInstruction(call) {
  const intent = savedInboundIntent(call);
  if (!intent) {
    return [
      "SESSION INTENT STATE",
      "The inbound intent is not yet saved.",
      "Use the existing routing question at most once, and only if the caller's reason remains unclear."
    ].join("\n");
  }
  return [
    "SESSION INTENT STATE",
    `The saved inbound intent is ${intent}. Intent discovery is complete.`,
    "Do not repeat or restate the inbound routing choices after questions, contact collection, tools, interruptions, follow-up scheduling, resumption, or near the closing.",
    "The routing choices may be repeated only if the caller explicitly asks to hear the options, says they do not understand, says none match, or clearly changes the purpose of the call.",
    "Only when the caller clearly changes the purpose of the call may you save a different intent, using intent_change_confirmed true.",
    "Never present the routing choices during the closing."
  ].join("\n");
}

function inboundSbaProfileEntries(call) {
  const result = call?.result || {};
  return [
    ["name", [result.first_name, result.last_name].filter(Boolean).join(" ") || result.full_name],
    ["email", result.email],
    ["business entity", result.business_entity_type],
    ["entity status", result.entity_status],
    ["estimated credit", result.estimated_credit_score || result.credit_score],
    ["gross monthly revenue", result.gross_monthly_revenue],
    ["tax information", result.taxes || result.tax_returns_status || result.tax_return_status],
    ["city", result.city],
    ["ZIP", result.zip],
    ["lead ID", result.lead_id]
  ].filter(([, value]) => cleanInboundContactValue(value, 500));
}

function inboundSbaLeadStateInstruction(call) {
  const result = call?.result || {};
  const matchStatus = cleanText(result.sba_lead_match_status, 50) || "not_checked";
  const profileEntries = inboundSbaProfileEntries(call);
  return [
    "SESSION SBA LEAD STATE",
    `Phone lookup status: ${matchStatus}.`,
    profileEntries.length
      ? `Existing profile fields: ${profileEntries.map(([label, value]) => `${label}: ${value}`).join("; ")}.`
      : "No existing SBA intake values are loaded.",
    matchStatus === "matched"
      ? "Use these values naturally, confirm the important profile together, and do not recollect confirmed fields."
      : matchStatus === "multiple_matches"
        ? "Multiple phone matches exist. Ask for one identifying detail at a time and use lookup_existing_sba_lead before choosing a record."
        : "Treat the caller as a new funding inquiry and collect only the information needed for the caller's purpose and next step."
  ].join("\n");
}


function buildDaisyInboundInstructions(call) {
  const payload = call?.payload || {};
  const callerPhone = validE164Phone(call?.phone)
    ? String(call.phone)
    : "not provided";
  const callerName = inboundCallerFirstName(call) || "not provided";
  const callerPhoneLastFour = String(callerPhone).replace(/\D/g, "").slice(-4) ||
    "unavailable";
  const phoneVerificationStatus = call?.result?.phone_verified === true
    ? "verified"
    : "not_verified";
  const leadSource = cleanText(payload.lead_source, 160) || "not provided";

  const profileEntries = inboundSbaProfileEntries(call);
  const script = SBA_INBOUND_SCRIPT
    .replaceAll("{caller_phone}", callerPhone)
    .replaceAll("{caller_phone_last_four}", callerPhoneLastFour)
    .replaceAll("{phone_verification_status}", phoneVerificationStatus)
    .replaceAll("{customer_first_name}", callerName)
    .replaceAll("{caller_name}", callerName)
    .replaceAll("{lead_source}", leadSource)
    .replaceAll(
      "{lead_match_status}",
      cleanText(call?.result?.sba_lead_match_status, 50) || "not_checked"
    )
    .replaceAll(
      "{existing_profile}",
      profileEntries.length
        ? profileEntries.map(([label, value]) => `${label}: ${value}`).join("; ")
        : "none loaded"
    );
  return [
    inboundIntentStateInstruction(call),
    inboundSbaLeadStateInstruction(call),
    script
  ].join("\n\n");
}

const DOUGLAS_DAISY_SCRIPT = String.raw`
DAISY 3.2 — DPA CALL SCRIPT

These are internal operating instructions. Never read headings, rules, braces, or placeholders aloud.

==================================================
1. NON-NEGOTIABLE CALL BEHAVIOR
==================================================
- SCRIPT-LOCK MODE: Daisy must speak only the exact dialogue written inside quotation marks in this script.
- Do not add, remove, paraphrase, summarize, acknowledge, transition, preface, conclude, or improvise any words.
- After the customer answers, silently select the next appropriate quoted scripted line and say it exactly as written.
- Text outside quotation marks is instruction only and must never be spoken.
- When a quoted line contains a placeholder, replace only the placeholder value. Do not change any other wording.
- When the customer asks a separate question, Daisy may answer only when the question is directly related to down payment assistance, the homebuying process, the current application, preliminary DTI, required documents, lenders, Realtors, or the next step being discussed.
- Daisy must keep any permitted answer brief, accurate, and within the scope of the call.
- Daisy must never discuss or quote interest rates, guarantee eligibility, approval, assistance amounts, loan terms, closing dates, or home prices.
- For any question outside the scope of down payment assistance or homebuying, Daisy says exactly: "That is something a specialist will discuss with you."
- After answering or redirecting, Daisy must immediately continue with the exact pending scripted line.
- Daisy must not begin a side conversation, ask an additional unscripted question, or add any other wording.
- Say the complete opening sentence before waiting: "Hi, is {customer_name} available?"
- Never say only "Hi" and pause.
- The opening question is the identity check. Do not add a second identity-verification speech.
- After the customer confirms, continue: "Great, this is Daisy with the DPA Help Center. How are you?"
- Ask one question at a time.
- After every question, stop speaking and wait for a completed customer response.
- Never answer your own question or move forward without an answer.
- For a yes-or-no question, treat "yes," "yeah," "yep," "yup," "mmm-hmm," "mhm," "uh-huh," "sure," "absolutely," and "correct" as affirmative answers.
- Ignore background noise, clicks, phone movement, dishes, static, music, television, echo, and other brief non-speech sounds.
- Do not stop, pause, restart, or change the call because of background noise.
- Only yield for sustained meaningful customer speech or a clear command such as "wait," "stop," "hold on," or "excuse me."
- Do not interrupt the customer while they are finishing a thought.
- If the customer asks a separate question, answer it briefly, then return to the one pending script question.
- Use submitted information. Confirm it instead of repeating the intake form.
- Never manufacture, infer, or complete an answer for the customer.
-- Never narrate internal thinking, planning, tool execution, retries, calculations, saving, CRM updates, or next-step selection.
- After every routine customer answer, do not acknowledge, thank, summarize, confirm receipt, comment, or elaborate. Never say "got it," "thanks," "thank you for that," "understood," "okay," "perfect," "excellent," "let me think," "let me figure that out," or any similar filler.
- Silently save the answer when appropriate and immediately continue with the next exact scripted sentence or question.
- The only exceptions are the brief emotional response immediately after "How are you?", acknowledging a customer correction, a brief apology after a genuine system failure, and the final departing message.
- Do not fill tool-execution time with narration.
- If a tool fails, do not narrate a retry.
- Never discuss or quote interest rates.
- Never guarantee approval, eligibility, a program, an assistance amount, a closing date, or a home price.
- DTI and homebuying power are preliminary estimates only.
- Read income and dollar amounts as natural currency. Never read a multi-digit dollar amount one digit at a time.
- Daisy cannot offer, send, or claim to have sent text messages.
- Do not ask for SMS consent.
- Do not call or mention send_resource_link.
- Never claim a handoff or other action succeeded until the tool confirms success.
- Before ending a connected call, save the outcome, confirm the next step, use complete_call, give one brief closing, and end normally.

When the current call has no remaining question or action:
- Use complete_call.
- After the tool succeeds, say exactly: "Thank you for your time, {customer_name}. If there's nothing else, please feel free to hang up and disconnect the call. Have a great day."
- Allow the full closing audio to play.
- The server controls the physical hangup.
- Do not decide whether the telephone line should remain connected.
- Do not continue after the final closing.
- Disconnect the telephone line.
- Do not wait silently on the line.
- Do not restart the conversation.
- Do not trigger reconnect.

- A normal goodbye is not an unexpected disconnect.

Runtime mode: {call_mode}
Customer: {customer_name}
Estimated assistance: {estimated_dpa}
Submitted credit score:  {credit_score_submitted}
Submitted income: {income_submitted}
Submitted work history: {work_history_submitted}
Submitted tax-return information: {tax_return_submitted}
Readiness score: {readiness_score}
Saved purchase timeline: {purchase_timeframe}
Saved purchase area: {purchase_area}
Saved lender status: {has_lender}
Saved Realtor status: {has_realtor}
Confirmed customer timezone: {customer_timezone_label}
Current source call ID: {source_call_id}
Current call timestamp: {current_call_timestamp}
Previous call summary: {previous_call_summary}

Never speak "not provided" as though it were customer data. When a value is unavailable, use a natural generic version of the sentence.

==================================================
2. INTERNAL SPECIALIST NOTIFICATION MODE
==================================================

Use this section only when Runtime mode says INTERNAL SPECIALIST NOTIFICATION.

Daisy says:
"Hi, is {agent_name} available?"

WAIT.

After confirmation Daisy says:
"Great, this is Daisy with the DPA Help Center. I'm calling to let you know that {internal_customer_name} has started the DPA application."

Briefly provide the saved purchase timeline and purchase area when available.

Daisy asks:
"Can you confirm you received that?"

WAIT.

After confirmation:
- Use complete_call with outcome agent_notified.
- Set stop_sequence true.
- Set pause_sequence false.
- Thank the specialist.
- End the call normally.

==================================================
3. RECONNECT MODE
==================================================

Use this section only when Runtime mode says RECONNECT.

Daisy says:
"Hi, is {customer_name} available?"

WAIT.

After confirmation Daisy says:
"Great, this is Daisy with the DPA Help Center. I think we got disconnected. Is now still a good time?"

WAIT.

Resume from the saved summary and next action.

Do not restart Call One.
Do not repeat confirmed answers.
A normal goodbye or a call ending at the end of the script must never trigger a reconnect call.

==================================================
4. CALL ONE — DISCOVERY
==================================================

Use this section when Runtime mode says CALL ONE.

OPENING

Daisy says:
"Hi, is {customer_name} available?"

WAIT.

After the customer confirms, Daisy says:
"Great, this is Daisy with the DPA Help Center. How are you?"

WAIT.

Listen to the completed response, then reply with at most one very brief natural sentence appropriate to the customer's mood: positive, "I'm glad to hear that"; neutral, "Good to hear"; negative, "I'm sorry to hear that"; busy or distracted, "I understand." Do not ask how they feel, start a side conversation, overreact, or repeat their words. Immediately continue to the next scripted sentence.

CONFIRM THE REQUEST

When the assistance estimate is available, Daisy says:
"I see you're a first-time homebuyer looking for up to {estimated_dpa} in down payment assistance to purchase a home. Is that correct?"

When the assistance estimate is unavailable, Daisy says:
"I see you're a first-time homebuyer looking for down payment assistance to purchase a home. Is that correct?"

WAIT.

The words "Is that correct?" are required and must not be omitted.

When the customer corrects the amount or first-time-homebuyer status:
- Acknowledge the correction.
- Save the updated information.
- Do not argue or repeat the original information.

CONFIRM SUBMITTED INFORMATION

When all submitted values are available, Daisy says:
"Excellent. Based on your submitted credit score of {credit_score_submitted}, your submitted income of {income_submitted}, your work history of {work_history_submitted}, and your tax return information of {tax_return_submitted}, reviewing down payment assistance options should be in your favor. Is all of that information still correct?"

When one or more values are unavailable:
- Confirm only the values that are available.
- End with: "Is that information still correct?"

WAIT.

Save corrections without asking the customer to repeat information that remains correct.

CONFIRM AVAILABILITY

Daisy says:
"Wonderful. {customer_name}, it really sounds like you're ready to secure down payment assistance funds and become a homeowner. Do you have a minute or two so I can explain our two-call process?"

WAIT.

IF ANOTHER TIME IS BETTER

Daisy says:
"No problem. What date and time would work better for you?"

WAIT. Require a specific calendar date and exact time. Ask only for a missing detail. Never accept "later," "sometime tomorrow," "in the afternoon," "next week," or "whenever" as complete.

If Confirmed customer timezone is Eastern, Central, Mountain, or Pacific, reuse it and do not ask for it. Otherwise ask exactly:
"Oh, by the way, {customer_name}, is that Eastern, Central, Mountain, or Pacific time?"

WAIT.

Repeat the exact complete appointment. When speaking the date, say only the weekday, month, and day. Never speak the year. Continue using the full YYYY-MM-DD date internally for the appointment tool:
"Excellent. I'll call you on {callback_date} at {callback_time} {callback_timezone}. Is that correct?"

WAIT. Only a clear yes confirms it. If the customer corrects the timezone, use the correction, recalculate, repeat the complete appointment, and require confirmation again.

After clear confirmation call create_confirmed_appointment with callback_type "call_one_rescheduled", callback_reason "Customer requested another time to complete Call One", prospect_confirmed true, and Current source call ID. Do not call it with incomplete or inferred values. Do not claim scheduling succeeded unless the tool returns success true.

After success, use complete_call. The server then plays this final closing:
"Perfect. I have us scheduled to speak again. Thank you for your time, {customer_name}. I'll speak with you then. If there's nothing else, please feel free to hang up and disconnect the call. Have a great day."

End normally after the closing. The server physically disconnects the current call.

EXPLAIN THE CALL

When the customer can continue, Daisy says:
"Perfect, this will be quick. Our two-call process is simple. Call One, which is now, quickly covers your purchase timeline, whether you're working with a lender or Realtor, and the area where you'd like to purchase. On Call Two, we'll review your application status, debt-to-income ratio, and potential program options, and make sure you're connected with DPA lender and Realtor specialists when needed. How does that sound?"

WAIT.

Do not provide another long explanation after the customer agrees.

QUESTION ONE — PURCHASE TIMELINE

Daisy says:
"As far as your timeline, how soon would you like to become a homeowner: within the next two months, four months, six months, or one year?"

WAIT.

Save the customer's exact choice as purchase_timeline_detail.

Also normalize time_frame as follows:
- Two months = "30 - 60"
- Four months = "Within six months"
- Six months = "Within six months"
- One year = "More than six months"

Map interest level as:
- Two months = "High"
- Four months = "Medium"
- Six months = "Medium"
- One year = "Nurture"

QUESTION TWO — LENDER

Daisy says:
"Understood. Are you currently working with a lender?"

WAIT.

Save applied_with_lender as Yes or No.

Do not treat the DPA Help Center as the outside lender referenced by this question.

QUESTION THREE — REALTOR

Daisy says:
"Okay. Are you currently working with a Realtor?"

WAIT.

Save has_realtor as Yes or No.

QUESTION FOUR — PURCHASE AREA

Daisy says:
"And one more question before we schedule your second call: what area would you like to purchase a home in?"

WAIT.

Save the customer's exact meaningful answer as purchase_area without changing its spelling or location. Never infer it from lead city, ZIP code, intake data, Monday.com, another lead, or a nearby city. If the answer is unclear, ask exactly: "What city or area would you like to purchase in?" Do not guess.

After saving purchase_area, use that exact saved answer in the personalized closing during this same call. When it is available, never replace it with "your area," "the area you mentioned," or "your desired location."

SCHEDULE CALL TWO

After saving the exact purchase area, Daisy says:
"{purchase_area_closing}"

Then Daisy says:
"Your next step is to start the application so I can follow up with you about its status, explore potential program options, and review your preliminary debt-to-income ratio. How does that sound?"

WAIT.

Then ask:
"{customer_name}, do you think you'll have time to start the application today?"

WAIT.

If yes, ask:
"Excellent. Would it be okay if I scheduled our second call for approximately 24 hours from now?"

WAIT. If the customer agrees, do not ask what time works, do not ask them to repeat the current time, and do not collect another callback time. Add exactly 24 hours to Current call timestamp. Convert that resulting UTC instant to the customer's confirmed saved timezone and use its exact local date and time. When a valid confirmed timezone is saved, proceed directly to the complete appointment confirmation without asking for the timezone again.

If the customer cannot start today, ask:
"That's understandable. When do you think you'll have time to get started?"

WAIT. Propose the exact calendar date approximately 24 hours after that point, then collect the exact time. Ask only for missing details.

For either path, reuse Confirmed customer timezone when valid and do not ask again. If it is not available, ask exactly:
"Oh, by the way, {customer_name}, is that Eastern, Central, Mountain, or Pacific time?"

WAIT.

Repeat the exact complete appointment. When speaking the date, say only the weekday, month, and day. Never speak the year. Continue using the full YYYY-MM-DD date internally for the appointment tool:
"I have us confirmed to speak on {callback_date} at {callback_time} {callback_timezone}. Is that correct?"

WAIT. Only a clear yes confirms it. A timezone correction requires recalculation, a complete corrected repetition, and confirmation again.

After clear confirmation call create_confirmed_appointment with callback_type "call_two_application_follow_up", callback_reason "Application status, program options, and preliminary DTI follow-up", the calculated local date, calculated local time, remembered timezone, calculated UTC callback_at, prospect_confirmed true, and Current source call ID. Never call it from an assumed, vague, incomplete, or inferred answer. Do not claim scheduling succeeded unless the tool returns success true.

After success, save all captured answers and the summary, then use complete_call. The server plays this final closing:
"Excellent. Thank you for your time, {customer_name}. I look forward to speaking with you then. If there's nothing else, please feel free to hang up and disconnect the call. Have a great day."

Allow the closing to finish and end normally. The server physically disconnects the current call. A successful normal completion must not trigger reconnect.

==================================================
6. REQUIRED RHYTHM
==================================================

DAISY ASKS ONE QUESTION
↓
DAISY STOPS SPEAKING
↓
CUSTOMER ANSWERS
↓
DAISY UNDERSTANDS THE RESPONSE
↓
DAISY SAVES THE ANSWER WHEN APPROPRIATE
↓
DAISY MOVES TO THE NEXT STEP

Daisy must never:
- Ask multiple primary questions in one turn.
- Continue speaking after asking a question.
- Answer the question herself.
- Move to the next objective without an answer.
- Treat background noise as an interruption.
- Treat background noise as a customer answer.
- Treat a customer question as the answer to a pending structured question.
- End normally and then call back as though the call disconnected.
`;

function resolveSessionCallPhase(call, attempt = null) {
  if (
    call?.direction === "inbound" ||
    normalizeMondayKey(call?.payload?.direction) === "inbound"
  ) return "INBOUND";
  const callType = normalizeMondayKey(call?.payload?.call_type);
  const outboundReason = normalizeMondayKey(
    call?.result?.outbound_call_reason || call?.payload?.outbound_call_reason
  );
  const attemptType = normalizeMondayKey(attempt?.attempt_type);

  if (
    callType === "dpaagentnotification" ||
    attemptType === "specialistnotification"
  ) return "SPECIALIST_NOTIFICATION";
  if (
    outboundReason === "unexpecteddisconnectreconnect" ||
    attemptType === "disconnectreconnect" ||
    call?.result?.reconnect_source_call_id
  ) return "RECONNECT";
  return "CALL_ONE";
}

function buildDouglasDaisyInstructions(call, sessionCallPhase) {
  const lead = call.payload || {};
  const result = normalizeDaisyAnswers(call.result || {});
  const confirmedPurchaseArea = cleanText(result.purchase_area, 1000);
  const callMode = {
    CALL_ONE: "CALL ONE",
    RECONNECT: "RECONNECT",
    SPECIALIST_NOTIFICATION: "INTERNAL SPECIALIST NOTIFICATION"
  }[sessionCallPhase] || "CALL ONE";

  const values = {
    call_mode: callMode,
    customer_name:
      cleanText(
        lead.first_name || lead.customer_name || lead.name,
        160
      ) || "the customer",
    agent_name:
      cleanText(lead.agent_name || lead.first_name, 160) ||
      "the assigned specialist",
    internal_customer_name:
      cleanText(lead.customer_name, 160) || "the customer",
    estimated_dpa:
  formatAssistanceAmount(lead.estimated_dpa) || "not provided",
credit_score_submitted:
  cleanText(
    lead.credit_score ??
      lead.mid_fico ??
      lead.fico_score ??
      lead.fico,
    50
  ) || "not provided",
income_submitted:
  formatIncomeForDaisy(lead.household_income ?? lead.income) ||
  "not provided",
    work_history_submitted:
      cleanText(
        lead.employment_history ?? lead.employment,
        200
      ) || "not provided",
    tax_return_submitted:
      cleanText(
        lead.tax_return_history ?? lead.taxes_filed,
        160
      ) || "not provided",
    readiness_score:
      lead.readiness_score ?? "not provided",
    has_lender:
      result.applied_with_lender ??
      lead.has_lender ??
      "not provided",
    has_realtor:
      result.has_realtor ??
      lead.has_realtor ??
      "not provided",
    purchase_timeframe:
      result.purchase_timeline_detail ??
      result.time_frame ??
      lead.purchase_timeframe ??
      lead.time_frame ??
      "not provided",
    purchase_area:
      confirmedPurchaseArea || "not provided",
   purchase_area_closing: confirmedPurchaseArea
  ? `Well, that's everything for this call, and now you're one step closer to becoming a homeowner in ${confirmedPurchaseArea}.`
  : "Well, that's everything for this call, and now you're one step closer to becoming a homeowner.",
    customer_timezone_label:
      result.customer_timezone_confirmed === true &&
      ["Eastern", "Central", "Mountain", "Pacific"].includes(result.customer_timezone_label)
        ? result.customer_timezone_label
        : "not confirmed",
    source_call_id: call.call_id,
    current_call_timestamp: new Date().toISOString(),
    previous_call_summary:
      cleanText(
        call.summary ??
          result.discussion_summary ??
          result.summary,
        4000
      ) || "not provided"
  };

  return Object.entries(values).reduce(
    (script, [name, value]) => {
      return script.replaceAll(`{${name}}`, String(value));
    },
    DOUGLAS_DAISY_SCRIPT
  );
}

const DOUG_TOOLS = [
  {
    type: "function",
    name: "save_call_progress",
    description:
      "Save the current conversation state, structured answers, sentiment, and next state without ending the call.",
    parameters: {
      type: "object",
      properties: {
        current_state: { type: "string" },
        next_state: { type: "string" },
        answers: { type: "object" },
        sentiment: {
          type: "string",
          enum: [
            "positive",
            "neutral",
            "skeptical",
            "confused",
            "frustrated",
            "urgent",
            "excited"
          ]
        },
        notes: { type: "string" }
      },
      required: ["current_state", "next_state", "answers"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "calculate_preliminary_dti",
    description:
      "Calculate a preliminary debt-to-income percentage from gross monthly household income and recurring monthly debts.",
    parameters: {
      type: "object",
      properties: {
        gross_monthly_household_income: { type: "number", minimum: 1 },
        monthly_recurring_debt: { type: "number", minimum: 0 }
      },
      required: [
        "gross_monthly_household_income",
        "monthly_recurring_debt"
      ],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "send_resource_link",
    description:
      "Send an approved DPA Help Center readiness resource by SMS after customer confirmation.",
    parameters: {
      type: "object",
      properties: {
        resource_type: {
          type: "string",
          enum: [
            "application",
            "dti_calculator",
            "prephub",
            "credit_readiness",
            "tax_readiness",
            "employment_readiness"
          ]
        },
        consent_confirmed: { type: "boolean" }
      },
      required: ["resource_type", "consent_confirmed"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "create_specialist_handoff",
    description: "Create a structured handoff for a DPA specialist.",
    parameters: {
      type: "object",
      properties: {
        reason: { type: "string" },
        priority: {
          type: "string",
          enum: ["normal", "high", "urgent"]
        },
        summary: { type: "string" }
      },
      required: ["reason", "priority", "summary"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "transfer_to_specialist",
    description:
      "Attempt a live transfer to an available DPA specialist after explicit customer agreement.",
    parameters: {
      type: "object",
      properties: {
        reason: { type: "string" },
        priority: {
          type: "string",
          enum: ["normal", "high", "urgent"]
        },
        prospect_confirmed: { type: "boolean" }
      },
      required: ["reason", "priority", "prospect_confirmed"],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "mark_contact_restriction",
    description:
      "Stop or restrict future contact when the number is wrong, invalid, not interested, or opted out.",
    parameters: {
      type: "object",
      properties: {
        restriction_type: {
          type: "string",
          enum: [
            "wrong_number",
            "invalid_number",
            "do_not_call",
            "not_interested"
          ]
        },
        reason: { type: "string" },
        stop_voice: { type: "boolean" },
        stop_sms: { type: "boolean" },
        stop_email: { type: "boolean" }
      },
      required: [
        "restriction_type",
        "reason",
        "stop_voice",
        "stop_sms",
        "stop_email"
      ],
      additionalProperties: false
    }
  },
  {
    type: "function",
    name: "complete_call",
    description:
      "Complete the conversation with the final outcome, next action, and summary.",
    parameters: {
      type: "object",
      properties: {
        outcome: {
          type: "string",
          enum: [
            "qualified",
            "hot_transfer",
            "specialist_handoff",
            "application_link_sent",
            "dti_calculator_sent",
            "needs_review",
            "nurture",
            "voicemail",
            "no_answer",
            "busy",
            "not_interested",
            "wrong_number",
            "opt_out",
            "disconnected",
            "technical_failure",
            "agent_notified"
          ]
        },
        next_action: { type: "string" },
        summary: { type: "string" },
        stop_sequence: { type: "boolean" },
        pause_sequence: { type: "boolean" }
      },
      required: [
        "outcome",
        "next_action",
        "summary",
        "stop_sequence",
        "pause_sequence"
      ],
      additionalProperties: false
    }
  }
];

const INBOUND_TOOLS = Object.freeze([
  inlineTool(
    "save_inbound_caller_context",
    "Save confirmed SBA funding-profile details and call progress without ending the call.",
    {
      intent: {
        type: "string",
        enum: [...SBA_INTENTS]
      },
      intent_change_confirmed: { type: ["boolean", "null"] },
      full_name: { type: ["string", "null"] },
      first_name: { type: ["string", "null"] },
      last_name: { type: ["string", "null"] },
      phone_number: { type: ["string", "null"] },
      phone_verified: { type: ["boolean", "null"] },
      email: { type: ["string", "null"] },
      lead_source: { type: ["string", "null"] },
      business_name: { type: ["string", "null"] },
      business_entity_type: { type: ["string", "null"] },
      entity_status: { type: ["string", "null"] },
      time_in_business: { type: ["string", "null"] },
      industry: { type: ["string", "null"] },
      city: { type: ["string", "null"] },
      state: { type: ["string", "null"] },
      zip: { type: ["string", "number", "null"] },
      lead_id: { type: ["string", "null"] },
      funding_use: { type: ["string", "null"] },
      funding_amount: { type: ["number", "string", "null"] },
      funding_timeline: { type: ["string", "null"] },
      estimated_credit_score: { type: ["string", "number", "null"] },
      gross_monthly_revenue: { type: ["string", "number", "null"] },
      monthly_business_expenses: { type: ["string", "number", "null"] },
      existing_business_financing: { type: ["string", "null"] },
      tax_returns_status: { type: ["string", "null"] },
      tax_years_available: { type: ["string", "number", "null"] },
      bank_statements_available: { type: ["boolean", "null"] },
      financial_statements_available: { type: ["boolean", "null"] },
      website_funding_preview_completed: { type: ["boolean", "null"] },
      existing_profile_confirmed: { type: ["boolean", "null"] },
      preliminary_readiness: { type: ["string", "null"] },
      missing_items: { type: ["array", "null"], items: { type: "string" } },
      next_action: { type: ["string", "null"] },
      call_summary: { type: ["string", "null"] },
      call_outcome: { type: ["string", "null"] }
    },
    []
  ),
  inlineTool(
    "lookup_existing_sba_lead",
    "Resolve an existing SBA Monday lead by caller phone plus confirmed name or email without fabricating a match.",
    {
      email: { type: ["string", "null"] },
      first_name: { type: ["string", "null"] },
      last_name: { type: ["string", "null"] }
    },
    []
  ),
  inlineTool(
    "create_inbound_follow_up",
    "Create or decline a confirmed SBA funding-review follow-up task.",
    {
      follow_up_date: { type: ["string", "null"], description: "Exact local date in YYYY-MM-DD format." },
      follow_up_time: { type: ["string", "null"], description: "Exact local time including hour and minute." },
      follow_up_timezone: { type: ["string", "null"] },
      follow_up_reason: { type: "string", enum: ["funding_review"] },
      follow_up_declined: { type: "boolean" },
      call_summary: { type: ["string", "null"] }
    },
    ["follow_up_reason", "follow_up_declined"]
  ),
  inlineTool(
    "create_funding_specialist_handoff",
    "Route the SBA funding profile to a Funding Specialist without placing an outbound call.",
    {
      reason: { type: "string" },
      priority: { type: "string", enum: ["normal", "high", "urgent"] }
    },
    ["reason", "priority"]
  ),
  DOUG_TOOLS.find((toolDefinition) => toolDefinition.name === "complete_call")
]);

const REALTIME_TOOLS = INBOUND_TOOLS;

async function runMigrationStep(name, sql, options = {}) {
  const { optional = false } = options;
  try {
    await pool.query(sql);
    console.log(`Database migration complete: ${name}`);
  } catch (error) {
    console.error(`Database migration failed: ${name}`, error);
    if (!optional) throw error;
  }
}

async function initializeDatabase() {
  console.log("[INBOUND] Initializing database.");

  await runMigrationStep(
    "create ai_calls",
    `
      CREATE TABLE IF NOT EXISTS ai_calls (
        id BIGSERIAL PRIMARY KEY,
        call_id VARCHAR(100) UNIQUE NOT NULL,
        request_key VARCHAR(320) UNIQUE NOT NULL,
        case_id VARCHAR(150),
        lead_id VARCHAR(150),
        phone VARCHAR(50) NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'created',
        stream_token VARCHAR(160) NOT NULL,
        twilio_call_sid VARCHAR(80),
        attempts INTEGER NOT NULL DEFAULT 0,
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
        result JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_error TEXT,
        started_at TIMESTAMPTZ,
        answered_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `
  );

  const aiCallColumns = [
    ["direction", "VARCHAR(20) NOT NULL DEFAULT 'inbound'"],
    ["intent", "VARCHAR(80)"],
    ["sequence_status", "VARCHAR(50) NOT NULL DEFAULT 'ready'"],
    ["timezone", "VARCHAR(100) NOT NULL DEFAULT 'America/New_York'"],
    ["consent_status", "VARCHAR(50) NOT NULL DEFAULT 'unverified'"],
    ["consent_timestamp", "TIMESTAMPTZ"],
    ["consent_source", "VARCHAR(255)"],
    ["do_not_call", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["wrong_number", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["invalid_number", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["current_state", "VARCHAR(80) NOT NULL DEFAULT 'greeting'"],
    ["next_state", "VARCHAR(80)"],
    ["sentiment", "VARCHAR(50)"],
    ["outcome", "VARCHAR(80)"],
    ["next_action", "TEXT"],
    ["summary", "TEXT"],
    ["actions", "JSONB NOT NULL DEFAULT '[]'::jsonb"],
    ["last_attempt_id", "VARCHAR(100)"],
    ["agent_version", "VARCHAR(50)"],
    ["prompt_version", "VARCHAR(80)"],
    ["tool_version", "VARCHAR(80)"],
    ["knowledge_version", "VARCHAR(80)"],
    ["routing_version", "VARCHAR(80)"],
    ["monday_item_id", "VARCHAR(100)"],
    ["monday_group_id", "VARCHAR(100)"],
    ["monday_last_sync_at", "TIMESTAMPTZ"],
    ["monday_last_error", "TEXT"],
    ["human_owner_id", "VARCHAR(100)"],
    ["priority", "VARCHAR(30) NOT NULL DEFAULT 'normal'"],
    ["last_attempt_at", "TIMESTAMPTZ"],
    ["awaiting_customer_response", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["pending_question_type", "VARCHAR(100)"],
    ["pending_question_text", "TEXT"],
    ["question_asked_at", "TIMESTAMPTZ"],
    ["response_reminder_count", "INTEGER NOT NULL DEFAULT 0"]
  ];

  for (const [columnName, definition] of aiCallColumns) {
    await runMigrationStep(
      `ai_calls.${columnName}`,
      `ALTER TABLE ai_calls ADD COLUMN IF NOT EXISTS ${columnName} ${definition}`
    );
  }

  await runMigrationStep(
    "create call_attempts",
    `
      CREATE TABLE IF NOT EXISTS call_attempts (
        id BIGSERIAL PRIMARY KEY,
        attempt_id VARCHAR(100) UNIQUE NOT NULL,
        call_id VARCHAR(100) NOT NULL,
        attempt_number INTEGER NOT NULL,
        call_leg INTEGER NOT NULL DEFAULT 1,
        dialed_at TIMESTAMPTZ,
        answered_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        twilio_call_sid VARCHAR(80),
        technical_status VARCHAR(50) NOT NULL DEFAULT 'created',
        business_outcome VARCHAR(80),
        answered_by VARCHAR(30) NOT NULL DEFAULT 'unknown',
        voicemail_left BOOLEAN NOT NULL DEFAULT FALSE,
        sms_sent BOOLEAN NOT NULL DEFAULT FALSE,
        duration_seconds INTEGER NOT NULL DEFAULT 0,
        disconnect_reason TEXT,
        transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
        summary TEXT,
        actions JSONB NOT NULL DEFAULT '[]'::jsonb,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `
  );

  const attemptColumns = [
    ["attempt_id", "VARCHAR(100)"],
    ["call_id", "VARCHAR(100)"],
    ["attempt_number", "INTEGER"],
    ["call_leg", "INTEGER NOT NULL DEFAULT 1"],
    ["dialed_at", "TIMESTAMPTZ"],
    ["answered_at", "TIMESTAMPTZ"],
    ["completed_at", "TIMESTAMPTZ"],
    ["twilio_call_sid", "VARCHAR(80)"],
    ["technical_status", "VARCHAR(50) NOT NULL DEFAULT 'created'"],
    ["business_outcome", "VARCHAR(80)"],
    ["answered_by", "VARCHAR(30) NOT NULL DEFAULT 'unknown'"],
    ["voicemail_left", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["sms_sent", "BOOLEAN NOT NULL DEFAULT FALSE"],
    ["duration_seconds", "INTEGER NOT NULL DEFAULT 0"],
    ["disconnect_reason", "TEXT"],
    ["transcript", "JSONB NOT NULL DEFAULT '[]'::jsonb"],
    ["summary", "TEXT"],
    ["actions", "JSONB NOT NULL DEFAULT '[]'::jsonb"],
    ["last_error", "TEXT"],
    ["monday_subitem_id", "VARCHAR(100)"],
    ["monday_last_sync_at", "TIMESTAMPTZ"],
    ["monday_last_error", "TEXT"],
    ["attempt_type", "VARCHAR(50) NOT NULL DEFAULT 'initial_lead_call'"],
    ["idempotency_key", "VARCHAR(255)"],
    ["cancellation_reason", "TEXT"],
    ["scheduled_for", "TIMESTAMPTZ"],
    ["appointment_id", "VARCHAR(100)"],
    ["callback_type", "VARCHAR(80)"],
    ["callback_reason", "TEXT"],
    ["callback_timezone", "VARCHAR(100)"],
    ["callback_timezone_label", "VARCHAR(30)"],
    ["source_call_id", "VARCHAR(100)"],
    ["created_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"],
    ["updated_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"]
  ];

  for (const [columnName, definition] of attemptColumns) {
    await runMigrationStep(
      `call_attempts.${columnName}`,
      `ALTER TABLE call_attempts ADD COLUMN IF NOT EXISTS ${columnName} ${definition}`
    );
  }

  await runMigrationStep(
    "create scheduled_appointments",
    `
      CREATE TABLE IF NOT EXISTS scheduled_appointments (
        id BIGSERIAL PRIMARY KEY,
        appointment_id VARCHAR(100) UNIQUE NOT NULL,
        customer_key VARCHAR(400) NOT NULL,
        source_call_id VARCHAR(100) NOT NULL,
        case_id VARCHAR(150),
        lead_id VARCHAR(150),
        callback_at TIMESTAMPTZ NOT NULL,
        customer_local_date DATE NOT NULL,
        customer_local_time TIME NOT NULL,
        timezone VARCHAR(100) NOT NULL,
        timezone_label VARCHAR(30) NOT NULL,
        callback_type VARCHAR(80) NOT NULL,
        callback_reason TEXT NOT NULL,
        discussion_summary TEXT,
        prospect_confirmed BOOLEAN NOT NULL CHECK (prospect_confirmed = TRUE),
        next_action TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (customer_key, callback_type, callback_at)
      )
    `
  );

  await runMigrationStep(
    "create integration_state",
    `
      CREATE TABLE IF NOT EXISTS integration_state (
        state_key VARCHAR(150) PRIMARY KEY,
        state_value JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `
  );

  await runMigrationStep(
    "create sms_deliveries",
    `
      CREATE TABLE IF NOT EXISTS sms_deliveries (
        message_sid VARCHAR(80) PRIMARY KEY,
        call_id VARCHAR(100) NOT NULL,
        message_type VARCHAR(80) NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'accepted',
        error_code VARCHAR(50),
        error_message TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `
  );

  const indexSteps = [
    [
      "idx_ai_calls_case_id",
      "CREATE INDEX IF NOT EXISTS idx_ai_calls_case_id ON ai_calls(case_id)"
    ],
    [
      "idx_ai_calls_lead_id",
      "CREATE INDEX IF NOT EXISTS idx_ai_calls_lead_id ON ai_calls(lead_id)"
    ],
    [
      "idx_ai_calls_status",
      "CREATE INDEX IF NOT EXISTS idx_ai_calls_status ON ai_calls(status)"
    ],
    [
      "idx_ai_calls_twilio_sid",
      "CREATE INDEX IF NOT EXISTS idx_ai_calls_twilio_sid ON ai_calls(twilio_call_sid)"
    ],
    [
      "idx_ai_calls_monday_item",
      "CREATE INDEX IF NOT EXISTS idx_ai_calls_monday_item ON ai_calls(monday_item_id)"
    ],
    [
      "idx_call_attempts_call_id",
      "CREATE INDEX IF NOT EXISTS idx_call_attempts_call_id ON call_attempts(call_id, attempt_number, call_leg)"
    ],
    [
      "idx_call_attempts_twilio_sid",
      "CREATE INDEX IF NOT EXISTS idx_call_attempts_twilio_sid ON call_attempts(twilio_call_sid)"
    ],
    [
      "idx_call_attempts_monday_subitem",
      "CREATE INDEX IF NOT EXISTS idx_call_attempts_monday_subitem ON call_attempts(monday_subitem_id)"
    ],
    [
      "idx_call_attempts_idempotency_key",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_call_attempts_idempotency_key ON call_attempts(idempotency_key) WHERE idempotency_key IS NOT NULL"
    ],
    [
      "idx_call_attempts_appointment",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_call_attempts_appointment ON call_attempts(appointment_id) WHERE appointment_id IS NOT NULL"
    ],
    [
      "idx_call_attempts_scheduled_for",
      "CREATE INDEX IF NOT EXISTS idx_call_attempts_scheduled_for ON call_attempts(scheduled_for) WHERE technical_status = 'pending'"
    ],
    [
      "idx_sms_deliveries_call_id",
      "CREATE INDEX IF NOT EXISTS idx_sms_deliveries_call_id ON sms_deliveries(call_id, message_type)"
    ]
  ];

  for (const [name, sql] of indexSteps) {
    await runMigrationStep(name, sql, { optional: true });
  }

  console.log("[INBOUND] Database initialized.");
}

async function getCallById(callId) {
  const result = await pool.query(
    "SELECT * FROM ai_calls WHERE call_id = $1 LIMIT 1",
    [callId]
  );
  return result.rows[0] || null;
}

async function getCallByRequestKey(requestKey) {
  const result = await pool.query(
    "SELECT * FROM ai_calls WHERE request_key = $1 LIMIT 1",
    [requestKey]
  );
  return result.rows[0] || null;
}

async function getAttemptById(attemptId) {
  const result = await pool.query(
    "SELECT * FROM call_attempts WHERE attempt_id = $1 LIMIT 1",
    [attemptId]
  );
  return result.rows[0] || null;
}

function pendingAttemptStatus(status) {
  return ["pending", "created"].includes(
    String(status || "").toLowerCase()
  );
}

const PERMITTED_OUTBOUND_CALL_REASONS = Object.freeze([
  "initial_lead_call",
  "unexpected_disconnect_reconnect"
]);

function permittedOutboundCallReason(value) {
  return PERMITTED_OUTBOUND_CALL_REASONS.includes(
    String(value || "").trim()
  );
}

function internalNotificationCallReason(value) {
  const reason = String(value || "").trim().toLowerCase();
  return [
    "dpa_agent_notification",
    "specialist_notification",
    "department_notification",
    "internal_notification"
  ].includes(reason) || (
    reason.endsWith("_notification") &&
    /(?:dpa|agent|specialist|department|internal)/.test(reason)
  );
}

function resolveOutboundCallReason(call, attempt, options = {}) {
  const explicitReason = cleanText(
    options.callReason ||
      call?.result?.outbound_call_reason ||
      call?.payload?.outbound_call_reason,
    100
  );
  if (explicitReason) return explicitReason;
  const notificationReason =
    call?.payload?.call_type || attempt?.attempt_type;
  if (internalNotificationCallReason(notificationReason)) {
    return String(notificationReason).trim().toLowerCase();
  }
  return null;
}

function outboundLeadId(call) {
  return cleanText(
    call?.lead_id || call?.payload?.lead_id || call?.case_id || call?.request_key,
    320
  );
}

function logOutboundCallRejected(call, resolvedCallReason, rejectionReason) {
  console.log(JSON.stringify({
    event: "outbound_call_rejected",
    call_id: call?.call_id || null,
    lead_id: outboundLeadId(call),
    call_reason: resolvedCallReason || null,
    reason: rejectionReason
  }));
}

function logOutboundCallFinalEligibility(call, resolvedCallReason) {
  console.log(JSON.stringify({
    event: "outbound_call_final_eligibility",
    call_id: call.call_id,
    lead_id: outboundLeadId(call),
    call_reason: resolvedCallReason,
    reconnect_source_call_id:
      call.result?.reconnect_source_call_id || null,
    eligible: true
  }));
}

function outboundCallSource(attempt, requestedSource) {
  if (requestedSource) return requestedSource;
  return attempt?.attempt_type === "disconnect_reconnect"
    ? "unexpected_reconnect"
    : "initial";
}

function blockDisabledOutboundCall(call, source) {
  console.log(JSON.stringify({
    event: "outbound_call_blocked",
    call_id: call?.call_id || null,
    source,
    reason: "OUTBOUND_CALLS_ENABLED_is_false"
  }));
}

function attemptTypeForCall(call, requestedType = null) {
  if (requestedType) return requestedType;
  if (
    resolveOutboundCallReason(call, null) === "unexpected_disconnect_reconnect" ||
    ["reconnect_pending", "reconnect_in_progress"].includes(call.current_state)
  ) {
    return "disconnect_reconnect";
  }
  if (call.payload?.call_type === "dpa_agent_notification") {
    return "specialist_notification";
  }
  return "initial_lead_call";
}

async function createCallAttempt(callId, options = {}) {
  const client = options.client || pool;
  const callResult = await client.query(
    "SELECT * FROM ai_calls WHERE call_id = $1 LIMIT 1",
    [callId]
  );
  const call = callResult.rows[0];
  if (!call) throw new Error("Call sequence not found.");

  const attemptType = attemptTypeForCall(call, options.attemptType);
  const attemptNumber = Number(call.attempts || 0) + 1;
  const idempotencyKey =
    options.idempotencyKey || `${attemptType}:${call.call_id}:${attemptNumber}`;
  const inserted = await client.query(
    `
      INSERT INTO call_attempts (
        attempt_id, call_id, attempt_number, call_leg,
        technical_status, attempt_type, idempotency_key
      )
      VALUES ($1, $2, $3, 1, 'created', $4, $5)
      ON CONFLICT DO NOTHING
      RETURNING *
    `,
    [
      createPublicId("ATTEMPT"),
      call.call_id,
      attemptNumber,
      attemptType,
      idempotencyKey
    ]
  );
  return inserted.rows[0] || null;
}

async function sequenceHasUnresolvedWork(callId, client = pool) {
  const result = await client.query(
    `
      SELECT EXISTS (
        SELECT 1 FROM ai_calls
        WHERE call_id = $1 AND (
          sequence_status = 'human_action'
          OR awaiting_customer_response = TRUE
        )
      ) OR EXISTS (
        SELECT 1 FROM call_attempts
        WHERE call_id = $1 AND completed_at IS NULL
          AND technical_status NOT IN ('completed', 'busy', 'failed', 'no-answer', 'canceled')
      ) OR EXISTS (
        SELECT 1 FROM sms_deliveries
        WHERE call_id = $1 AND LOWER(status) IN ('accepted', 'queued', 'sending')
      ) AS unresolved
    `,
    [callId]
  );
  return Boolean(result.rows[0]?.unresolved);
}
async function getCallByMondayItemId(itemId) {
  const result = await pool.query(
    "SELECT * FROM ai_calls WHERE monday_item_id = $1 LIMIT 1",
    [String(itemId)]
  );
  return result.rows[0] || null;
}

async function getIntegrationState(stateKey) {
  const result = await pool.query(
    "SELECT state_value FROM integration_state WHERE state_key = $1 LIMIT 1",
    [stateKey]
  );
  return result.rows[0]?.state_value || null;
}

async function setIntegrationState(stateKey, stateValue) {
  await pool.query(
    `
      INSERT INTO integration_state (state_key, state_value, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (state_key)
      DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = NOW()
    `,
    [stateKey, JSON.stringify(stateValue || {})]
  );
}

async function getAttemptsForCall(callId) {
  const result = await pool.query(
    `
      SELECT *
      FROM call_attempts
      WHERE call_id = $1
      ORDER BY attempt_number ASC, call_leg ASC
    `,
    [callId]
  );
  return result.rows;
}

async function validateCallToken(callId, token) {
  const result = await pool.query(
    `
      SELECT *
      FROM ai_calls
      WHERE call_id = $1 AND stream_token = $2
      LIMIT 1
    `,
    [callId, token]
  );
  return result.rows[0] || null;
}

async function appendTranscript(callId, speaker, text) {
  const cleaned = cleanText(text, 8000);
  if (!cleaned) return;

  const entry = { speaker, text: cleaned, at: new Date().toISOString() };
  await pool.query(
    `
      UPDATE ai_calls
      SET transcript = transcript || $2::jsonb, updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId, JSON.stringify([entry])]
  );

  const call = await getCallById(callId);
  if (call && call.last_attempt_id) {
    await pool.query(
      `
        UPDATE call_attempts
        SET transcript = transcript || $2::jsonb, updated_at = NOW()
        WHERE attempt_id = $1
      `,
      [call.last_attempt_id, JSON.stringify([entry])]
    );
  }
}

async function appendAction(callId, action) {
  const entry = { ...action, at: new Date().toISOString() };
  await pool.query(
    `
      UPDATE ai_calls
      SET actions = actions || $2::jsonb, updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId, JSON.stringify([entry])]
  );

  const call = await getCallById(callId);
  if (call && call.last_attempt_id) {
    await pool.query(
      `
        UPDATE call_attempts
        SET actions = actions || $2::jsonb, updated_at = NOW()
        WHERE attempt_id = $1
      `,
      [call.last_attempt_id, JSON.stringify([entry])]
    );
  }
}

async function mergeCallResult(callId, patch) {
  await pool.query(
    `
      UPDATE ai_calls
      SET result = result || $2::jsonb, updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId, JSON.stringify(patch || {})]
  );
}

function extractPrimaryQuestion(value) {
  const text = cleanText(value, 8000);
  if (!text) return null;
  const questions = text.match(/[^?]+\?/g) || [];
  const rawQuestion = questions.at(-1) || "";
  const lastPeriod = rawQuestion.lastIndexOf(". ");
  const lastExclamation = rawQuestion.lastIndexOf("! ");
  const boundary = Math.max(lastPeriod, lastExclamation);
  const question = cleanText(
    boundary >= 0 ? rawQuestion.slice(boundary + 2) : rawQuestion,
    2000
  );
  if (!question) return null;
  return { text: question, count: questions.length };
}

function pendingQuestionType(value) {
  const text = normalizeMondayKey(value);
  if (/speakwith|isthis/.test(text)) return "identity_confirmation";
  if (/pointyouintherightdirection|bestdescribeswhyyourecalling/.test(text)) {
    return "intent_discovery";
  }
  if (/firstandlastname|fullname|yourname/.test(text)) return "caller_name";
  if (/emailaddress|email/.test(text)) return "caller_email";
  if (/betterserve.*cityandstate|cityandstate.*callingfrom/.test(text)) {
    return "caller_city_state";
  }
  if (/numberendingin.*correct|rightnumber.*endingin/.test(text)) {
    return "caller_phone_confirmation";
  }
  if (/correctcallbacknumber|correctphonenumber|bestcallbacknumber/.test(text)) {
    return "caller_phone_correction";
  }
  if (/realtor|realestateagent/.test(text)) return "has_realtor";
  if (/lender|preapproved|preapproval/.test(text)) return "applied_with_lender";
  if (/what.*(?:city|area).*purchase|area.*purchase.*home/.test(text)) {
    return "purchase_area";
  }
  if (/send.*link|text.*link|want.*link|receive.*link/.test(text)) {
    return "application_link_permission";
  }
  if (/correct|isthatright|stillaccurate|confirm/.test(text)) {
    return "confirmation";
  }
  if (/creditscore|score.*credit/.test(text)) return "credit_score";
  if (/grossmonthlyrevenue|monthlyrevenue|revenue.*month/.test(text)) {
    return "gross_monthly_revenue";
  }
  if (/entitytype|business.*(?:llc|corporation|entity)|how.*business.*structured/.test(text)) {
    return "business_entity_type";
  }
  if (/howlong.*business|timeinbusiness|business.*operating/.test(text)) {
    return "time_in_business";
  }
  if (/howmuchfunding|fundingamount|amount.*funding/.test(text)) {
    return "funding_amount";
  }
  if (/use.*funding|funding.*(?:for|purpose)/.test(text)) return "funding_use";
  if (
    /howmuch.*homes?.*(?:considering|looking)|homes?.*(?:considering|looking).*howmuch/.test(
      text
    )
  ) {
    return "estimated_home_price";
  }
  if (
    /howsoon.*(?:becomeahomeowner|buy|purchase)|when.*(?:buy|purchase).*home/.test(
      text
    )
  ) {
    return "homebuying_timeline";
  }
  if (
    /annual.*household.*income|household.*income|income.*beforetax/.test(text)
  ) {
    return "annual_household_income";
  }
  if (/filed.*taxreturns?|taxreturns?.*lasttwoyears/.test(text)) {
    return "tax_return_status";
  }
  if (/employed.*(?:past|last).*twoyears|employmenthistory|workhistory/.test(text)) {
    return "employment_history";
  }
  if (/timeframe|timeline|revenue|cashflow|credit|business|funding|tax/.test(text)) {
    return "qualification";
  }
  return "general_question";
}

const INBOUND_US_STATE_VARIANTS = Object.freeze([
  ["Alabama", "AL"], ["Alaska", "AK"], ["Arizona", "AZ"],
  ["Arkansas", "AR"], ["California", "CA"], ["Colorado", "CO"],
  ["Connecticut", "CT"], ["Delaware", "DE"], ["Florida", "FL"],
  ["Georgia", "GA"], ["Hawaii", "HI"], ["Idaho", "ID"],
  ["Illinois", "IL"], ["Indiana", "IN"], ["Iowa", "IA"],
  ["Kansas", "KS"], ["Kentucky", "KY"], ["Louisiana", "LA"],
  ["Maine", "ME"], ["Maryland", "MD"], ["Massachusetts", "MA"],
  ["Michigan", "MI"], ["Minnesota", "MN"], ["Mississippi", "MS"],
  ["Missouri", "MO"], ["Montana", "MT"], ["Nebraska", "NE"],
  ["Nevada", "NV"], ["New Hampshire", "NH"], ["New Jersey", "NJ"],
  ["New Mexico", "NM"], ["New York", "NY"], ["North Carolina", "NC"],
  ["North Dakota", "ND"], ["Ohio", "OH"], ["Oklahoma", "OK"],
  ["Oregon", "OR"], ["Pennsylvania", "PA"], ["Rhode Island", "RI"],
  ["South Carolina", "SC"], ["South Dakota", "SD"], ["Tennessee", "TN"],
  ["Texas", "TX"], ["Utah", "UT"], ["Vermont", "VT"],
  ["Virginia", "VA"], ["Washington", "WA"], ["West Virginia", "WV"],
  ["Wisconsin", "WI"], ["Wyoming", "WY"],
  ["District of Columbia", "DC"]
]);

function exactMeaningfulPurchaseArea(value) {
  const area = cleanText(value, 1000);
  if (!area) return null;
  if (
    /\b(i don'?t know|not sure|unsure|anywhere|no preference|whatever|doesn'?t matter)\b/i.test(
      area
    )
  ) {
    return null;
  }
  return cleanText(area.replace(/[.!?]+$/, ""), 1000);
}

function parseInboundPurchaseLocation(value) {
  const purchaseArea = exactMeaningfulPurchaseArea(value);
  if (!purchaseArea) return null;
  const cleaned = purchaseArea
    .replace(
      /^(?:i(?:'d| would)?\s+(?:like|want)\s+to\s+(?:buy|purchase)(?:\s+a\s+home)?\s+|i(?:'m| am)\s+looking\s+)(?:in|around)\s+/i,
      ""
    )
    .replace(/^(?:in|around|the city of)\s+/i, "")
    .trim();
  const variants = INBOUND_US_STATE_VARIANTS
    .flatMap(([name, abbreviation]) => [name, abbreviation])
    .sort((left, right) => right.length - left.length);
  const commaParts = cleaned
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (commaParts.length >= 2) {
    const state = commaParts.at(-1);
    const knownState = variants.some(
      (variant) => variant.toLowerCase() === state.toLowerCase()
    );
    const city = commaParts.slice(0, -1).join(", ").trim();
    return knownState && city ? { city, state } : null;
  }
  const lower = cleaned.toLowerCase();
  for (const variant of variants) {
    const suffix = ` ${variant.toLowerCase()}`;
    if (!lower.endsWith(suffix)) continue;
    const city = cleaned.slice(0, -suffix.length).trim();
    if (city) return { city, state: cleaned.slice(-variant.length) };
  }
  return null;
}

function normalizeCustomerUtterance(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[\[\](){}]/g, " ")
    .replace(/[^a-z0-9'\s-]/g, " ")
    .replace(/[-_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function callerExplicitlyRequestsIntentOptions(value) {
  const normalized = normalizeCustomerUtterance(value);
  return [
    /\bwhat (?:are|were) (?:the|my) (?:options|choices)\b/,
    /\b(?:repeat|restate|list|say) (?:the|those) (?:options|choices)(?: again)?\b/,
    /\b(?:i do not|i don't|i dont) understand\b/,
    /\b(?:i am|i'm|im) confused\b/,
    /\bnone of (?:those|the options|the choices) (?:match|apply|fit)\b/,
    /\b(?:actually|instead),? i(?:'m| am) calling (?:about|for)\b/,
    /\bmy reason for calling (?:changed|is different)\b/
  ].some((pattern) => pattern.test(normalized));
}

function affirmativeCustomerResponse(value) {
  const normalized = normalizeCustomerUtterance(value);

  return [
    "yes",
    "yes sir",
    "yes ma'am",
    "yeah",
    "yep",
    "yup",
    "mmm hmm",
    "mm hmm",
    "mhm",
    "mmhm",
    "uh huh",
    "sure",
    "sure is",
    "absolutely",
    "correct",
    "that's correct",
    "that is correct",
    "sounds good",
    "that sounds good"
  ].includes(normalized);
}

function likelyBackgroundNoiseTranscript(value) {
  const normalized = normalizeCustomerUtterance(value);

  if (!normalized) return true;

  return [
    "noise",
    "background noise",
    "blank audio",
    "inaudible",
    "silence",
    "static",
    "click",
    "clicking",
    "clatter",
    "clattering",
    "dish",
    "dishes",
    "music",
    "television",
    "tv",
    "echo",
    "beep",
    "beeping",
    "rustling",
    "shuffling",
    "phone movement",
    "phone moving",
    "door",
    "door closes",
    "door slams"
  ].includes(normalized);
}

function isMeaningfulCustomerTranscript(value) {
  const normalized = normalizeCustomerUtterance(value);

  if (!normalized || likelyBackgroundNoiseTranscript(normalized)) {
    return false;
  }

  return ![
    "um",
    "uh",
    "hmm",
    "hm",
    "mm"
  ].includes(normalized);
}

function customerAskedSeparateQuestion(value) {
  const text = String(value || "").trim();
  return (
    /\?\s*$/.test(text) ||
    /^(what|why|when|where|who|how|can|could|would|will|do|does|did|is|are)\b/i.test(
      text
    )
  );
}

function customerRequestedMoreTime(value) {
  return /\b(give me (a |one )?(minute|moment|second)|one moment|hold on|let me (think|check)|need (a |one )?(minute|moment|second)|more time)\b/i.test(
    String(value || "")
  );
}

function customerExplicitlyInterrupted(value) {
  return /\b(wait|stop|hold on|hang on|excuse me|one moment)\b/i.test(
    String(value || "")
  );
}

function directYesNoQuestion(value) {
  const text = String(value || "").trim();

  return (
    /^(hi,?\s+)?(is|are|am|was|were|do|does|did|have|has|had|can|could|would|will|should)\b/i.test(
      text
    ) ||
    /\b(correct|right)\?$/i.test(text) ||
    /\bhow does that sound\?$/i.test(text) ||
    /\bdoes that sound (good|okay)\?$/i.test(text)
  );
}

function presenceOnlyResponse(value) {
  const normalized = String(value || "")
    .toLowerCase()
    .replace(/[^a-z\s']/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return [
    "yes", "yeah", "yep", "i am", "i'm here", "im here", "still here",
    "yes i am", "yeah i'm here", "yeah im here"
  ].includes(normalized);
}

function logCustomerResponseState(callId, details = {}) {
  console.log(JSON.stringify({
    event: "customer_response_wait_state",
    call_id: callId,
    pending_question_type: details.pending_question_type || null,
    awaiting_customer_response: details.awaiting_customer_response === true,
    question_asked_at: details.question_asked_at || null,
    customer_speech_detected: details.customer_speech_detected === true,
    completed_transcript_received:
      details.completed_transcript_received === true,
    response_reminder_count: Number(details.response_reminder_count || 0),
    waiting_state_end_reason: details.waiting_state_end_reason || null
  }));
}

async function setAwaitingCustomerResponse(callId, question) {
  const askedAt = new Date();
  const questionType = pendingQuestionType(question.text);
  await pool.query(
    `
      UPDATE ai_calls
      SET awaiting_customer_response = TRUE,
          pending_question_type = $2,
          pending_question_text = $3,
          question_asked_at = $4,
          response_reminder_count = 0,
          updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId, questionType, question.text, askedAt]
  );
  logCustomerResponseState(callId, {
    pending_question_type: questionType,
    awaiting_customer_response: true,
    question_asked_at: askedAt.toISOString(),
    response_reminder_count: 0
  });
  if (question.count > 1) {
    console.warn(JSON.stringify({
      event: "stacked_question_detected",
      call_id: callId,
      question_count: question.count,
      pending_question_text: question.text
    }));
  }
  return {
    pending_question_type: questionType,
    pending_question_text: question.text,
    question_asked_at: askedAt.toISOString(),
    response_reminder_count: 0
  };
}

async function clearAwaitingCustomerResponse(callId, reason) {
  const previous = await getCallById(callId);
  await pool.query(
    `
      UPDATE ai_calls
      SET awaiting_customer_response = FALSE,
          pending_question_type = NULL,
          pending_question_text = NULL,
          question_asked_at = NULL,
          response_reminder_count = 0,
          updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId]
  );
  logCustomerResponseState(callId, {
    pending_question_type: previous?.pending_question_type,
    question_asked_at: previous?.question_asked_at,
    awaiting_customer_response: false,
    response_reminder_count: 0,
    waiting_state_end_reason: reason
  });
}

async function setResponseReminderCount(callId, count, questionState) {
  await pool.query(
    `UPDATE ai_calls SET response_reminder_count = $2, updated_at = NOW()
     WHERE call_id = $1`,
    [callId, count]
  );
  logCustomerResponseState(callId, {
    ...questionState,
    awaiting_customer_response: true,
    response_reminder_count: count
  });
}

/* -------------------------------------------------------------------------- */
/* monday.com isolated adapter */
/* -------------------------------------------------------------------------- */

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function withTimeout(promise, milliseconds, message) {
  let timer = null;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(message || "The operation timed out.")),
      milliseconds
    );
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function normalizeMondayKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function parseMondaySettings(settings) {
  if (!settings) return {};
  if (typeof settings === "object") return settings;
  try {
    return JSON.parse(settings);
  } catch {
    return {};
  }
}

async function mondayRequest(query, variables = {}, options = {}) {
  if (!MONDAY_SYNC_ENABLED) {
    throw new Error("monday.com sync is disabled or not fully configured.");
  }

  const mutation = /^\s*mutation\b/i.test(query);
  const maxRetries = Number.isInteger(options.maxRetries)
    ? options.maxRetries
    : 2;
  const idempotencyKey = options.idempotencyKey
    ? stableHash(options.idempotencyKey)
    : null;

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      MONDAY_REQUEST_TIMEOUT_MS
    );

    try {
      const headers = {
        Authorization: MONDAY_API_TOKEN,
        "Content-Type": "application/json",
        "API-Version": MONDAY_API_VERSION
      };

      if (mutation && idempotencyKey) {
        headers["Idempotency-Key"] = idempotencyKey;
      }

      const response = await fetch(MONDAY_API_URL, {
        method: "POST",
        headers,
        body: JSON.stringify({ query, variables }),
        signal: controller.signal
      });

      const rawBody = await response.text();
      let body = null;

      try {
        body = rawBody ? JSON.parse(rawBody) : {};
      } catch {
        body = { raw: rawBody };
      }

      const retryableHttp = [408, 409, 429, 500, 502, 503, 504].includes(
        response.status
      );

      if (!response.ok) {
        const error = new Error(
          `monday.com HTTP ${response.status}: ${cleanText(rawBody, 1200)}`
        );
        error.statusCode = response.status;

        if (retryableHttp && attempt < maxRetries) {
          const retryAfterSeconds = Number(response.headers.get("retry-after"));
          const waitMs = Number.isFinite(retryAfterSeconds)
            ? Math.max(500, retryAfterSeconds * 1000)
            : 500 * 2 ** attempt;
          await sleep(waitMs);
          continue;
        }

        throw error;
      }

      if (Array.isArray(body.errors) && body.errors.length) {
        const message = body.errors
          .map((entry) => entry.message || "Unknown monday.com error")
          .join(" | ");
        const error = new Error(`monday.com GraphQL error: ${message}`);
        error.mondayErrors = body.errors;
        throw error;
      }

      return body.data || {};
    } catch (error) {
      const retryableNetwork =
        error.name === "AbortError" ||
        ["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ENOTFOUND"].includes(
          error.code
        );

      if (retryableNetwork && attempt < maxRetries) {
        await sleep(500 * 2 ** attempt);
        continue;
      }

      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error("monday.com request failed after retries.");
}

let inboundMondayMetadataCache = null;

function inboundLog(prefix, event, details = {}) {
  console.log(`${prefix} ${JSON.stringify({ event, ...details })}`);
}

function inboundMondayDiagnosticContext(callId) {
  const normalizedCallId = cleanText(callId, 100);
  if (!normalizedCallId || inboundMondayDiagnosticCompleted) return null;
  if (INBOUND_MONDAY_DIAGNOSTIC_CALL_ID) {
    return normalizedCallId === INBOUND_MONDAY_DIAGNOSTIC_CALL_ID
      ? { call_id: normalizedCallId }
      : null;
  }
  if (!INBOUND_MONDAY_DIAGNOSTICS_ONCE) return null;
  if (!inboundMondayDiagnosticClaimedCallId) {
    inboundMondayDiagnosticClaimedCallId = normalizedCallId;
    inboundLog("[MONDAY_DIAGNOSTIC]", "test_call_claimed", {
      call_id: normalizedCallId
    });
  }
  return inboundMondayDiagnosticClaimedCallId === normalizedCallId
    ? { call_id: normalizedCallId }
    : null;
}

function maskMondayDiagnosticText(value, visibleCharacters = 1) {
  const text = cleanText(value, 1000);
  if (!text) return null;
  return `${text.slice(0, visibleCharacters)}***`;
}

function sanitizeMondayDiagnostic(value, key = "") {
  const normalizedKey = normalizeMondayKey(key);
  if (
    /token|authorization|password|secret|socialsecurity|ssn|bank|routingnumber|accountnumber|onetime|otp/
      .test(normalizedKey)
  ) {
    return "[REDACTED]";
  }
  if (Array.isArray(value)) {
    return value.map((entry) => sanitizeMondayDiagnostic(entry, key));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([childKey, childValue]) => [
        childKey,
        sanitizeMondayDiagnostic(childValue, childKey)
      ])
    );
  }
  if (/email/.test(normalizedKey)) {
    const email = normalizeInboundEmail(value);
    if (!email) return null;
    const [local, domain] = email.split("@");
    return `${local.slice(0, 1)}***@${domain}`;
  }
  if (/phone/.test(normalizedKey)) {
    return maskedPhoneLastFour(value);
  }
  if (
    normalizedKey === "name" ||
    /firstname|lastname|fullname|customername/.test(normalizedKey)
  ) {
    return maskMondayDiagnosticText(value);
  }
  if (/summary|transcript|answers|questions|notes/.test(normalizedKey)) {
    const text = cleanText(value, 4000);
    return text ? { present: true, length: text.length } : null;
  }
  return value;
}

function sanitizeMondayDiagnosticColumn(columnId, value) {
  const logicalName = Object.entries(INBOUND_MONDAY.columns)
    .find(([, configuredId]) => configuredId === columnId)?.[0] || columnId;
  return sanitizeMondayDiagnostic(value, logicalName);
}

function maskedPhoneLastFour(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits ? `***${digits.slice(-4)}` : null;
}

function splitFullName(fullName = "") {
  const parts = String(fullName).trim().split(/\s+/);

  return {
    full_name: parts.join(" "),
    first_name: parts[0] || "",
    last_name: parts.slice(1).join(" ")
  };
}

function normalizeInboundTaxReturnStatus(value) {
  const cleaned = cleanText(value, 100);
  if (!cleaned) return "";
  const normalized = normalizeMondayKey(cleaned);
  if (
    ["null", "undefined", "unknown", "none", "notprovided", "na"]
      .includes(normalized)
  ) {
    return "";
  }
  if (
    normalized === "yes" ||
    normalized === "twoyearsfiled" ||
    normalized === "2yearsfiled" ||
    normalized === "two_years_filed" ||
    /(?:yes|filed).*(?:last|past|for)?(?:the)?(?:two|2)years/.test(normalized)
  ) {
    return "2 Years Filed";
  }
  if (
    normalized === "oneyear" ||
    normalized === "oneyearfiled" ||
    normalized === "1yearfiled" ||
    normalized === "one_year"
  ) {
    return "1 Year Filed";
  }
  if (
    normalized === "notfiled" ||
    normalized === "not_filed" ||
    /(?:have|has|did)?notfiled|haventfiled|didntfile/.test(normalized)
  ) {
    return "Not Filed";
  }
  if (normalized === "notsure" || normalized === "not_sure") {
    return "Not Sure";
  }
  return cleaned;
}

function normalizeInboundCreditScore(value) {
  const cleaned = cleanText(value, 100);
  if (!cleaned) return "";
  const matches = cleaned.match(/\b\d{3}\b/g) || [];
  if (matches.length !== 1) return "";
  const score = Number(matches[0]);
  return score >= 300 && score <= 850 ? String(score) : "";
}

function normalizeInboundAnnualIncome(value) {
  const cleaned = cleanText(value, 100);
  if (!cleaned) return "";
  const digitMatch = cleaned.match(
    /\$?\s*(\d[\d,]*(?:\.\d+)?)\s*(k|thousand|m|million)?/i
  );
  if (digitMatch) {
    const base = Number(digitMatch[1].replace(/,/g, ""));
    const suffix = String(digitMatch[2] || "").toLowerCase();
    const multiplier = ["k", "thousand"].includes(suffix)
      ? 1000
      : ["m", "million"].includes(suffix)
        ? 1000000
        : 1;
    const income = base * multiplier;
    return Number.isFinite(income) && income > 0 ? String(income) : "";
  }
  const numberWords = {
    one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7,
    eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12, thirteen: 13,
    fourteen: 14, fifteen: 15, sixteen: 16, seventeen: 17, eighteen: 18,
    nineteen: 19, twenty: 20, thirty: 30, forty: 40, fifty: 50,
    sixty: 60, seventy: 70, eighty: 80, ninety: 90
  };
  const tokens = cleaned.toLowerCase().replace(/-/g, " ").match(/[a-z]+/g) || [];
  let total = 0;
  let current = 0;
  let recognized = 0;
  for (const token of tokens) {
    if (Object.prototype.hasOwnProperty.call(numberWords, token)) {
      current += numberWords[token];
      recognized += 1;
    } else if (token === "hundred") {
      current = Math.max(1, current) * 100;
      recognized += 1;
    } else if (token === "thousand" || token === "million") {
      total += Math.max(1, current) * (token === "thousand" ? 1000 : 1000000);
      current = 0;
      recognized += 1;
    }
  }
  const income = total + current;
  return recognized && income > 0 ? String(income) : "";
}

function normalizeInboundEstimatedHomePrice(value) {
  return normalizeInboundAnnualIncome(value);
}

function cleanInboundContactValue(value, maximumLength = 320) {
  const cleaned = cleanText(value, maximumLength);
  if (!cleaned) return null;
  const sentinel = normalizeMondayKey(cleaned);
  return ["null", "undefined", "unknown", "notprovided", "na", "none"]
    .includes(sentinel)
    ? null
    : cleaned;
}

function firstInboundContactValue(maximumLength, ...values) {
  for (const value of values) {
    const cleaned = cleanInboundContactValue(value, maximumLength);
    if (cleaned) return cleaned;
  }
  return null;
}

function inboundValuesEqual(left, right) {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}

function inboundSessionFieldEqual(field, left, right) {
  if (field === "email") {
    return normalizeInboundEmail(left) === normalizeInboundEmail(right);
  }
  if (["full_name", "first_name", "last_name"].includes(field)) {
    return normalizeMondayKey(left) === normalizeMondayKey(right);
  }
  return inboundValuesEqual(left, right);
}

function normalizeInboundFullName(value) {
  const cleaned = cleanInboundContactValue(value, 160);
  if (!cleaned) return splitFullName("");
  const withoutIntroduction = cleaned
    .replace(/^(?:my name is|this is|i am|i'm)\s+/i, "")
    .replace(/[.!?,;:]+$/g, "")
    .trim();
  if (!withoutIntroduction || /[@\d]/.test(withoutIntroduction)) {
    return splitFullName("");
  }
  return splitFullName(withoutIntroduction);
}

function inboundCallerFirstName(call) {
  const result = call?.result || {};
  const direct = firstInboundContactValue(100, result.first_name);
  if (direct) return normalizeInboundFullName(direct).first_name || direct;
  return normalizeInboundFullName(result.full_name).first_name || null;
}

function inboundPurchaseLocationLabel(call) {
  const result = call?.result || {};
  const parsed = parseInboundPurchaseLocation(result.purchase_area);
  const city = firstInboundContactValue(
    160,
    result.purchase_city,
    parsed?.city
  );
  const state = firstInboundContactValue(
    100,
    result.purchase_state,
    parsed?.state
  );
  return city && state ? `${city}, ${state}` : null;
}

function buildInboundFinalClosing(call) {
  const firstName = inboundCallerFirstName(call);
  const scheduledFollowUp = formatInboundFollowUpConfirmation(
    call?.result?.inbound_follow_up
  );
  return scheduledFollowUp
    ? buildSbaScheduledClosing(firstName)
    : SBA_FINAL_THANK_YOU;
}

function normalizeSbaCreditRange(value) {
  const cleaned = cleanText(value, 100);
  if (!cleaned) return "";
  const normalized = normalizeMondayKey(cleaned);
  if (/notsure|unknown/.test(normalized)) return "Not sure";
  const scores = cleaned.match(/\b\d{3}\b/g)?.map(Number) || [];
  if (/below\s*580|under\s*580/i.test(cleaned)) return "Below 580";
  if (scores.length >= 2) {
    const low = Math.min(...scores);
    const high = Math.max(...scores);
    if (low <= 580 && high <= 619) return "580 - 619";
    if (low <= 620 && high <= 639) return "620 - 639";
    if (low <= 640 && high <= 679) return "640 - 679";
  }
  const score = scores[0];
  if (!Number.isFinite(score)) {
    if (/680plus|680\+/.test(normalized)) return "680+";
    return "";
  }
  if (score < 580) return "Below 580";
  if (score <= 619) return "580 - 619";
  if (score <= 639) return "620 - 639";
  if (score <= 679) return "640 - 679";
  return score <= 850 ? "680+" : "";
}

function normalizeSbaRevenueRange(value) {
  const cleaned = cleanText(value, 120);
  if (!cleaned) return "";
  const amounts = (cleaned.match(/\d[\d,]*/g) || []).map((amount) =>
    Number(amount.replace(/,/g, ""))
  );
  if (amounts.length >= 2) {
    const [low, high] = amounts;
    if (low === 0 && high === 5000) return "$0 - $5,000";
    if (low === 5000 && high === 25000) return "$5,000 - $25,000";
    if (low === 25000 && high === 150000) return "$25,000 - $150,000";
  }
  if (amounts[0] === 150000 && /(?:\+|plus|or more)/i.test(cleaned)) {
    return "$150,000+";
  }
  const amount = Number(normalizeInboundAnnualIncome(cleaned));
  if (!Number.isFinite(amount)) return "";
  if (amount <= 5000) return "$0 - $5,000";
  if (amount <= 25000) return "$5,000 - $25,000";
  if (amount <= 150000) return "$25,000 - $150,000";
  return "$150,000+";
}

function normalizeSbaEntityType(value) {
  const cleaned = cleanText(value, 120);
  if (!cleaned) return "";
  const normalized = normalizeMondayKey(cleaned);
  if (normalized === "llc" || normalized.includes("limitedliability")) return "LLC";
  if (normalized === "scorp" || normalized.includes("scorporation")) return "S-corp";
  if (normalized === "ccorp" || normalized.includes("ccorporation")) return "C-corp";
  if (normalized.includes("trust")) return "Trust";
  if (/notsetup|startup|notformed|none/.test(normalized)) return "Not Set Up Yet";
  return "";
}

function normalizeInboundEmail(value) {
  const cleaned = cleanInboundContactValue(value, 320);
  if (!cleaned) return null;
  const normalized = cleaned
    .replace(/[.,;:!?]+$/g, "")
    .replace(/^(?:my email(?: address)? is|the email(?: address)? is|it is|it's)\s+/i, "")
    .toLowerCase()
    .replace(/\s+at\s+/g, "@")
    .replace(/\s+dot\s+/g, ".")
    .replace(/\s+underscore\s+/g, "_")
    .replace(/\s+(?:dash|hyphen)\s+/g, "-")
    .replace(/\s*@\s*/g, "@")
    .replace(/\s*\.\s*/g, ".")
    .replace(/\s+/g, "");
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized)
    ? normalized
    : null;
}

function normalizeInboundLocalDate(value) {
  return normalizeLocalDate(cleanText(value, 30));
}

function normalizeInboundLocalTime(value) {
  return normalizeLocalTime(cleanText(value, 20));
}

function usefulInboundSummary(value) {
  const summary = cleanText(value, 4000);
  if (!summary || !cleanInboundContactValue(summary, 4000)) return null;
  const normalized = summary.toLowerCase().replace(/[.!]+$/g, "").trim();
  if (
    /^(?:inbound )?call (?:completed|ended|disconnected)(?: normally| successfully)?$/.test(normalized) ||
    /^(?:call )?summary$/.test(normalized) ||
    /^(?:no |not )?summary (?:available|provided|captured)$/.test(normalized) ||
    /^(?:caller|customer) called(?: for (?:help|assistance))?$/.test(normalized)
  ) {
    return null;
  }
  return summary;
}

function bestInboundDetailedSummary(call, candidate) {
  const result = call?.result || {};
  for (const value of [
    result.agent_call_summary,
    result.call_summary,
    candidate,
    call?.summary
  ]) {
    const summary = usefulInboundSummary(value);
    if (summary) return summary;
  }
  return null;
}

function mostCompleteInboundSummary(...values) {
  return values
    .map((value) => usefulInboundSummary(value))
    .filter(Boolean)
    .sort((left, right) => right.length - left.length)[0] || null;
}

function inboundCallerItemName(data = {}) {
  const fullName = cleanText(data.full_name || data.name, 160);
  if (fullName) return fullName;
  const phone = cleanText(data.phone, 100);
  if (phone) return phone;
  const dateCalled = cleanText(data.date_called, 30) ||
    new Date().toISOString().slice(0, 10);
  return `Inbound Caller ${dateCalled.slice(0, 10)}`;
}

function inboundDateCalled(value) {
  const date = new Date(value || Date.now());
  return Number.isNaN(date.getTime())
    ? new Date().toISOString().slice(0, 10)
    : date.toISOString().slice(0, 10);
}

async function mondayGraphql(query, variables = {}, diagnostic = null) {
  if (!INBOUND_MONDAY_CONNECTED) {
    throw new Error("Inbound monday.com is not configured.");
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), MONDAY_REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(MONDAY_API_URL, {
      method: "POST",
      headers: {
        Authorization: MONDAY_API_TOKEN,
        "Content-Type": "application/json",
        "API-Version": MONDAY_API_VERSION
      },
      body: JSON.stringify({ query, variables }),
      signal: controller.signal
    });
    const rawResponseBody = await response.text();
    let body = {};
    try {
      body = rawResponseBody ? JSON.parse(rawResponseBody) : {};
    } catch {
      body = { raw_response: cleanText(rawResponseBody, 4000) };
    }
    if (diagnostic?.always_log_raw_response) {
      const graphqlErrors = Array.isArray(body.errors)
        ? body.errors.map((entry) => ({
            message: cleanText(entry?.message, 500),
            error_code: cleanText(entry?.extensions?.error_code, 100) || null
          }))
        : [];
      inboundLog("[MONDAY_UPDATE]", "graphql_response", {
        call_id: diagnostic.call_id || null,
        monday_item_id: diagnostic.monday_item_id || null,
        board_id: diagnostic.board_id || MONDAY_BOARD_ID,
        http_status: response.status,
        graphql_errors: graphqlErrors,
        update_success: response.ok && graphqlErrors.length === 0
      });
    }
    if (diagnostic && !diagnostic.update_logging_only) {
      inboundLog("[MONDAY_DIAGNOSTIC]", "raw_api_response", {
        ...diagnostic,
        http_status: response.status,
        response_body: sanitizeMondayDiagnostic(body)
      });
      if (Array.isArray(body.errors) && body.errors.length) {
        inboundLog("[MONDAY_DIAGNOSTIC]", "graphql_errors", {
          ...diagnostic,
          errors: sanitizeMondayDiagnostic(body.errors, "graphql_errors")
        });
      }
    }
    const data = validateMondayEnvelope(response.status, body);
    inboundMondayConnectionHealthy = true;
    return data;
  } catch (error) {
    inboundMondayConnectionHealthy = false;
    if (diagnostic?.always_log_raw_response) {
      inboundLog("[MONDAY_UPDATE]", "graphql_request_failed", {
        call_id: diagnostic.call_id || null,
        monday_item_id: diagnostic.monday_item_id || null,
        board_id: diagnostic.board_id || MONDAY_BOARD_ID,
        http_status: error.httpStatus || null,
        graphql_errors: (error.mondayErrors || []).map((entry) => ({
          message: cleanText(entry?.message, 500),
          error_code: cleanText(entry?.extensions?.error_code, 100) || null
        })),
        update_success: false,
        error: cleanText(error.message, 500)
      });
    }
    if (diagnostic) {
      inboundLog("[MONDAY_DIAGNOSTIC]", "api_request_failed", {
        ...diagnostic,
        error: cleanText(error.message, 1000),
        graphql_errors: error.mondayErrors || null
      });
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function loadInboundMondayMetadata(force = false, diagnostic = null) {
  if (inboundMondayMetadataCache && !force) return inboundMondayMetadataCache;
  const data = await mondayGraphql(
    `query InboundBoardMetadata($boardIds: [ID!]!) {
      boards(ids: $boardIds) {
        id
        name
        groups { id title }
        columns { id title type settings }
      }
    }`,
    { boardIds: [MONDAY_BOARD_ID] },
    diagnostic
  );
  const board = data.boards?.[0];
  if (!board) throw new Error("Inbound monday.com board was not found.");
  inboundMondayMetadataCache = {
    ...board,
    columns: (board.columns || []).map((column) => ({
      ...column,
      settings: parseMondaySettings(column.settings)
    }))
  };
  return inboundMondayMetadataCache;
}

function inboundMondayColumn(metadata, columnId) {
  return metadata?.columns?.find((column) => column.id === columnId) || null;
}

function inboundMondayColumnByTitle(metadata, titles) {
  const desired = titles.map(normalizeMondayKey);
  return metadata?.columns?.find((column) =>
    desired.includes(normalizeMondayKey(column.title))
  ) || null;
}

function resolveInboundMondayLabel(column, desiredLabel) {
  if (!column || !desiredLabel) return null;
  const labels = column.settings?.labels;
  const values = Array.isArray(labels)
    ? labels.map((entry) => entry?.name || entry).filter(Boolean)
    : Object.values(labels || {}).map((entry) => entry?.name || entry).filter(Boolean);
  return values.find(
    (value) => normalizeMondayKey(value) === normalizeMondayKey(desiredLabel)
  ) || null;
}

function inboundMondayGroupByTitle(metadata, title) {
  const desired = normalizeMondayKey(title);
  return metadata?.groups?.find(
    (group) => normalizeMondayKey(group.title) === desired
  ) || null;
}

async function inboundMondayValues(data = {}, diagnostic = null) {
  const metadata = await loadInboundMondayMetadata(false, diagnostic);
  return buildSbaMondayUpdateValues({
    data,
    metadata,
    onSkippedColumn: ({ field, columnId, reason }) => {
      inboundLog("[MONDAY_WRITE]", "field_omitted", {
        call_id: diagnostic?.call_id || null,
        board_id: MONDAY_BOARD_ID,
        logical_field: field,
        column_id: columnId || null,
        reason
      });
    }
  });
}

function inboundMondayItemValue(item, columnId) {
  return item?.column_values?.find((column) => column.id === columnId)?.text || "";
}

function sbaProfileFromMondayItem(item) {
  if (!item?.id) return null;
  const firstName = cleanInboundContactValue(
    inboundMondayItemValue(item, INBOUND_MONDAY.columns.firstName),
    100
  );
  const lastName = cleanInboundContactValue(
    inboundMondayItemValue(item, INBOUND_MONDAY.columns.lastName),
    100
  );
  return {
    monday_item_id: String(item.id),
    full_name: [firstName, lastName].filter(Boolean).join(" ") || cleanText(item.name, 160),
    first_name: firstName,
    last_name: lastName,
    email: normalizeInboundEmail(inboundMondayItemValue(item, INBOUND_MONDAY.columns.email)),
    phone: normalizePhone(inboundMondayItemValue(item, INBOUND_MONDAY.columns.phoneNumber)),
    business_entity_type: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.businessEntityType), 120),
    entity_status: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.entityStatus), 100),
    taxes: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.taxes), 500),
    tax_status: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.taxStatus), 100),
    estimated_credit_score: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.estimatedCreditScore), 100),
    credit_status: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.creditStatus), 100),
    gross_monthly_revenue: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.grossMonthlyRevenue), 120),
    income_status: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.incomeStatus), 100),
    city: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.city), 160),
    zip: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.zip), 20),
    lead_id: cleanText(inboundMondayItemValue(item, INBOUND_MONDAY.columns.leadId), 160)
  };
}

async function findInboundCallersByPhone(phone, options = {}) {
  const normalized = normalizePhone(phone);
  inboundLog("[MONDAY_LINK]", "phone_search_started", {
    call_id: cleanText(options.callId, 100),
    normalized_caller_phone_last4: maskedPhoneLastFour(normalized),
    search_attempted: Boolean(normalized && INBOUND_MONDAY_CONNECTED)
  });
  if (!normalized || !INBOUND_MONDAY_CONNECTED) {
    inboundLog("[MONDAY_LINK]", "phone_search_completed", {
      call_id: cleanText(options.callId, 100),
      normalized_caller_phone_last4: maskedPhoneLastFour(normalized),
      match_count: 0,
      existing_item_id: null,
      multiple_matches: false,
      search_skipped: true
    });
    return [];
  }
  const phoneColumnId = INBOUND_MONDAY.columns.phoneNumber;
  const firstPage = await mondayGraphql(
    `query FindInboundCaller($boardIds: [ID!]!) {
      boards(ids: $boardIds) {
        items_page(limit: 500) {
          cursor
          items { id name created_at updated_at group { id } column_values { id text value } }
        }
      }
    }`,
    { boardIds: [MONDAY_BOARD_ID] }
  );
  let page = firstPage.boards?.[0]?.items_page || {};
  const matches = [];
  for (let pageNumber = 0; pageNumber < 20; pageNumber += 1) {
    matches.push(...(page.items || []).filter((item) => {
      const value = item.column_values?.find(
        (column) => column.id === phoneColumnId
      );
      return normalizePhone(value?.text) === normalized;
    }));
    const cursor = cleanText(page.cursor, 1000);
    if (!cursor) break;
    const nextPage = await mondayGraphql(
      `query FindInboundCallerNextPage($cursor: String!) {
        next_items_page(limit: 500, cursor: $cursor) {
          cursor
          items { id name created_at updated_at group { id } column_values { id text value } }
        }
      }`,
      { cursor }
    );
    page = nextPage.next_items_page || {};
  }
  inboundLog("[MONDAY_LINK]", "phone_search_completed", {
    call_id: cleanText(options.callId, 100),
    normalized_caller_phone_last4: maskedPhoneLastFour(normalized),
    match_count: matches.length,
    existing_item_id: matches.length === 1 ? String(matches[0].id) : null,
    multiple_matches: matches.length > 1
  });
  return matches;
}

async function lookupExistingSbaLead(call, identifiers = {}) {
  const matches = await findInboundCallersByPhone(call?.phone, {
    callId: call?.call_id
  });
  const email = normalizeInboundEmail(identifiers.email);
  const expectedName = normalizeMondayKey(
    [identifiers.first_name, identifiers.last_name].filter(Boolean).join(" ")
  );
  const narrowed = matches.filter((item) => {
    const profile = sbaProfileFromMondayItem(item);
    if (email && profile?.email === email) return true;
    return Boolean(expectedName && normalizeMondayKey(profile?.full_name) === expectedName);
  });
  const linkedItemId = cleanText(call?.monday_item_id, 100);
  const linkedItem = linkedItemId
    ? matches.find((item) => String(item?.id) === linkedItemId)
    : null;
  const candidates = linkedItem ? [linkedItem] : narrowed.length ? narrowed : matches;
  if (candidates.length === 0) {
    return {
      success: true,
      found: false,
      status: "not_found",
      match_count: 0
    };
  }
  const selection = selectBestSbaMondayMatch(candidates);
  if (!selection?.item?.id) {
    throw new Error("The SBA lead lookup did not resolve a monday.com item ID.");
  }
  const item = selection.item;
  const selectionReason = linkedItem
    ? "already_linked_item"
    : selection.selection_reason;
  if (candidates.length > 1) {
    inboundLog("[MONDAY_LINK]", "multiple_matches_resolved", {
      call_id: call.call_id,
      match_count: candidates.length,
      selected_item_id: String(item.id),
      selection_reason: selectionReason
    });
  }
  const profile = sbaProfileFromMondayItem(item);
  await pool.query(
    `UPDATE ai_calls SET monday_item_id = $2, monday_group_id = $3,
     result = result || $4::jsonb, updated_at = NOW() WHERE call_id = $1`,
    [
      call.call_id,
      String(item.id),
      item.group?.id || INBOUND_MONDAY.groups.newInboundCalls,
      JSON.stringify({
        ...profile,
        sba_lead_match_status: "matched",
        sba_lead_selection_reason: selectionReason,
        existing_profile_loaded: true
      })
    ]
  );
  return { success: true, found: true, status: "matched", profile };
}

async function createInboundCallerItem(data = {}, options = {}) {
  const phone = normalizePhone(data.phone);
  const itemName = inboundCallerItemName({ ...data, phone });
  const diagnostic = inboundMondayDiagnosticContext(options.callId);
  const targetGroupName = cleanText(options.targetGroupName, 100);
  const metadata = targetGroupName
    ? await loadInboundMondayMetadata(false)
    : null;
  const targetGroup = targetGroupName
    ? inboundMondayGroupByTitle(metadata, targetGroupName)
    : { id: INBOUND_MONDAY.groups.newInboundCalls, title: null };
  if (!targetGroup?.id) {
    throw new Error(`monday.com group not found: ${targetGroupName}.`);
  }
  const columnValues = await inboundMondayValues(
    { ...data, phone },
    diagnostic ? { ...diagnostic, operation: "board_metadata" } : null
  );
  if (diagnostic) {
    inboundLog("[MONDAY_CREATE]", "item_creation_request", {
      ...diagnostic,
      twilio_call_sid: cleanText(options.twilioCallSid, 80),
      board_id: MONDAY_BOARD_ID,
      group_id: targetGroup.id,
      item_name: maskMondayDiagnosticText(itemName),
      column_ids: Object.keys(columnValues),
      columns: Object.entries(columnValues).map(([columnId, value]) => ({
        column_id: columnId,
        value: sanitizeMondayDiagnosticColumn(columnId, value)
      }))
    });
  }
  const result = await mondayGraphql(
    `mutation CreateInboundCaller($boardId: ID!, $groupId: String!, $itemName: String!, $columnValues: JSON!) {
      create_item(board_id: $boardId, group_id: $groupId, item_name: $itemName, column_values: $columnValues) {
        id
        name
        board { id }
        group { id title }
      }
    }`,
    {
      boardId: MONDAY_BOARD_ID,
      groupId: targetGroup.id,
      itemName,
      columnValues: JSON.stringify(columnValues)
    },
    diagnostic ? { ...diagnostic, operation: "create_item" } : null
  );
  const item = result.create_item || null;
  if (!item?.id) {
    throw new Error("monday.com did not return an inbound item ID.");
  }
  inboundLog("[MONDAY]", "inbound_item_created", {
    item_id: item?.id || null,
    caller_phone: maskedPhoneLastFour(phone)
  });
  if (diagnostic) {
    inboundLog("[MONDAY_CREATE]", "item_creation_completed", {
      ...diagnostic,
      board_id: MONDAY_BOARD_ID,
      monday_item_id: String(item.id)
    });
  }
  for (const columnId of Object.keys(columnValues)) {
    inboundLog("[MONDAY_WRITE]", "field_written", {
      call_id: cleanText(options.callId, 100),
      monday_item_id: String(item.id),
      board_id: MONDAY_BOARD_ID,
      logical_field: sbaLogicalFieldForColumnId(columnId),
      column_id: columnId
    });
  }
  return item;
}

async function readInboundCallerItem(itemId) {
  const data = await mondayGraphql(
    `query ReadInboundCaller($itemIds: [ID!]!) {
      items(ids: $itemIds) {
        id
        name
        board { id }
        group { id title }
        column_values { id text value }
      }
    }`,
    { itemIds: [String(itemId)] }
  );
  return data.items?.find((item) => String(item.id) === String(itemId)) || null;
}

async function verifyWixSbaMondayItem(itemId, expected = {}) {
  const metadata = await loadInboundMondayMetadata(false);
  const expectedGroup = inboundMondayGroupByTitle(metadata, "New Leads");
  if (!expectedGroup?.id) {
    throw new Error('The "New Leads" group does not exist on SBA board 18414546873.');
  }
  const item = await readInboundCallerItem(itemId);
  if (!item) {
    throw new Error(`Monday item ${itemId} was not returned by the readback query.`);
  }
  if (String(item.board?.id || "") !== MONDAY_BOARD_ID) {
    throw new Error(
      `Monday item ${itemId} is on board ${item.board?.id || "unknown"}, expected ${MONDAY_BOARD_ID}.`
    );
  }
  if (String(item.group?.id || "") !== String(expectedGroup.id)) {
    throw new Error(
      `Monday item ${itemId} is in group ${item.group?.id || "unknown"}, expected ${expectedGroup.id} (New Leads).`
    );
  }
  if (normalizeMondayKey(item.group?.title) !== normalizeMondayKey("New Leads")) {
    throw new Error(
      `Monday item ${itemId} group is named ${item.group?.title || "unknown"}, expected New Leads.`
    );
  }

  const actual = {
    first_name: cleanInboundContactValue(
      inboundMondayItemValue(item, INBOUND_MONDAY.columns.firstName),
      100
    ),
    last_name: cleanInboundContactValue(
      inboundMondayItemValue(item, INBOUND_MONDAY.columns.lastName),
      100
    ),
    email: normalizeInboundEmail(
      inboundMondayItemValue(item, INBOUND_MONDAY.columns.email)
    ),
    phone: normalizePhone(
      inboundMondayItemValue(item, INBOUND_MONDAY.columns.phoneNumber)
    )
  };
  const expectedName = cleanText(expected.full_name || expected.name, 160);
  if (!cleanText(item.name, 160)) {
    throw new Error(`Monday item ${itemId} has a blank item name.`);
  }
  if (expectedName && cleanText(item.name, 160) !== expectedName) {
    throw new Error(`Monday item ${itemId} name does not match the submitted lead.`);
  }
  for (const field of ["first_name", "last_name"]) {
    if (!actual[field]) {
      throw new Error(`Monday item ${itemId} has a blank ${field} value.`);
    }
    if (cleanText(actual[field], 100) !== cleanText(expected[field], 100)) {
      throw new Error(`Monday item ${itemId} ${field} does not match the submitted value.`);
    }
  }
  if (!actual.email || actual.email !== normalizeInboundEmail(expected.email)) {
    throw new Error(`Monday item ${itemId} email does not match the submitted value.`);
  }
  if (!validE164Phone(actual.phone) || actual.phone !== normalizePhone(expected.phone)) {
    throw new Error(`Monday item ${itemId} phone does not match the submitted value.`);
  }

  return {
    item_id: String(item.id),
    actual_board_id: String(item.board.id),
    actual_group_id: String(item.group.id),
    actual_group_name: cleanText(item.group.title, 160),
    item_name: cleanText(item.name, 160),
    populated_fields: Object.entries(actual)
      .filter(([, value]) => Boolean(value))
      .map(([field]) => field)
  };
}

function findMondayInvalidColumnId(error) {
  const errors = Array.isArray(error?.mondayErrors) ? error.mondayErrors : [];
  const missingColumnError = errors.find((entry) => {
    const serialized = JSON.stringify(entry || {});
    return /missing_column|column ID doesn't exist|missing column/i.test(serialized);
  });
  if (!missingColumnError) return null;
  const queue = [missingColumnError];
  const visited = new Set();
  while (queue.length) {
    const value = queue.shift();
    if (!value || typeof value !== "object" || visited.has(value)) continue;
    visited.add(value);
    for (const [key, nested] of Object.entries(value)) {
      if (/^(?:column_id|columnId)$/i.test(key) && typeof nested === "string") {
        return cleanText(nested, 100);
      }
      if (nested && typeof nested === "object") queue.push(nested);
      if (typeof nested === "string" && nested.trim().startsWith("{")) {
        try {
          queue.push(JSON.parse(nested));
        } catch {}
      }
    }
  }
  return null;
}

async function updateInboundCallerItem(itemId, data = {}, options = {}) {
  if (!itemId) return null;
  const diagnostic = inboundMondayDiagnosticContext(options.callId);
  const metadata = await loadInboundMondayMetadata(
    false,
    diagnostic ? { ...diagnostic, operation: "board_metadata" } : null
  );
  const preparedColumnValues = buildSbaMondayUpdateValues({
    data,
    metadata,
    onSkippedColumn: ({ field, columnId, reason }) => {
      inboundLog("[MONDAY_WRITE]", "field_omitted", {
        call_id: cleanText(options.callId, 100),
        board_id: MONDAY_BOARD_ID,
        logical_field: field,
        column_id: columnId || null,
        reason
      });
    }
  });
  const columnValues = Object.fromEntries(
    Object.entries(preparedColumnValues).filter(([columnId]) => {
      const allowed = SBA_MAIN_BOARD_COLUMN_IDS.includes(columnId);
      if (!allowed) {
        inboundLog("[MONDAY_WRITE]", "field_omitted", {
          call_id: cleanText(options.callId, 100),
          board_id: MONDAY_BOARD_ID,
          logical_field: sbaLogicalFieldForColumnId(columnId),
          column_id: columnId,
          reason: "not_an_sba_main_board_mapping"
        });
      }
      return allowed;
    })
  );
  if (diagnostic) {
    inboundLog("[MONDAY_DIAGNOSTIC]", "update_payload", {
      ...diagnostic,
      monday_item_id: String(itemId),
      board_id: MONDAY_BOARD_ID,
      update_payload: sanitizeMondayDiagnostic(data),
      column_ids: Object.keys(columnValues),
      columns: Object.entries(columnValues).map(([columnId, value]) => ({
        column_id: columnId,
        value: sanitizeMondayDiagnosticColumn(columnId, value)
      })),
      item_name_column: data.full_name || data.name
        ? {
            column_id: INBOUND_MONDAY.columns.name,
            value: maskMondayDiagnosticText(data.full_name || data.name)
          }
        : null
    });
  }
  let updated = { id: String(itemId) };
  let writtenLogicalFields = [];
  const pendingColumnValues = { ...columnValues };
  while (Object.keys(pendingColumnValues).length) {
    const pendingColumnIds = Object.keys(pendingColumnValues);
    inboundLog("[MONDAY_WRITE]", "payload_prepared", {
      call_id: cleanText(options.callId, 100),
      board_id: MONDAY_BOARD_ID,
      item_id: String(itemId),
      logical_fields: pendingColumnIds.map(sbaLogicalFieldForColumnId).filter(Boolean),
      column_ids: pendingColumnIds
    });
    try {
      const result = await mondayGraphql(
        `mutation UpdateInboundCaller($boardId: ID!, $itemId: ID!, $columnValues: JSON!) {
          change_multiple_column_values(board_id: $boardId, item_id: $itemId, column_values: $columnValues) { id }
        }`,
        {
          boardId: MONDAY_BOARD_ID,
          itemId: String(itemId),
          columnValues: JSON.stringify(pendingColumnValues)
        },
        diagnostic
          ? {
              ...diagnostic,
              operation: "change_multiple_column_values",
              always_log_raw_response: true,
              monday_item_id: String(itemId),
              board_id: MONDAY_BOARD_ID
            }
          : {
              call_id: cleanText(options.callId, 100),
              monday_item_id: String(itemId),
              board_id: MONDAY_BOARD_ID,
              operation: "change_multiple_column_values",
              always_log_raw_response: true,
              update_logging_only: true
            }
      );
      const changed = result.change_multiple_column_values;
      if (!changed?.id) {
        throw new Error("monday.com did not return an item ID for the column update.");
      }
      updated = changed;
      writtenLogicalFields = pendingColumnIds
        .map(sbaLogicalFieldForColumnId)
        .filter(Boolean);
      inboundLog("[MONDAY_UPDATE]", "mutation_confirmed", {
        call_id: cleanText(options.callId, 100),
        monday_item_id: String(changed.id),
        board_id: MONDAY_BOARD_ID,
        column_ids: pendingColumnIds,
        update_success: true
      });
      for (const columnId of pendingColumnIds) {
        inboundLog("[MONDAY_WRITE]", "field_written", {
          call_id: cleanText(options.callId, 100),
          monday_item_id: String(changed.id),
          board_id: MONDAY_BOARD_ID,
          logical_field: sbaLogicalFieldForColumnId(columnId),
          column_id: columnId
        });
      }
      break;
    } catch (error) {
      const invalidColumnId = findMondayInvalidColumnId(error);
      if (!invalidColumnId || !Object.hasOwn(pendingColumnValues, invalidColumnId)) {
        throw error;
      }
      inboundLog("[MONDAY_WRITE]", "invalid_column", {
        logical_field: sbaLogicalFieldForColumnId(invalidColumnId),
        column_id: invalidColumnId,
        board_id: MONDAY_BOARD_ID
      });
      delete pendingColumnValues[invalidColumnId];
    }
  }
  const name = cleanText(data.full_name || data.name, 160);
  if (name) {
    const result = await mondayGraphql(
      `mutation RenameInboundCaller($boardId: ID!, $itemId: ID!, $name: String!) {
        change_simple_column_value(board_id: $boardId, item_id: $itemId, column_id: "name", value: $name) { id }
      }`,
      { boardId: MONDAY_BOARD_ID, itemId: String(itemId), name },
      diagnostic
        ? { ...diagnostic, operation: "change_simple_column_value" }
        : null
    );
    updated = result.change_simple_column_value || updated;
  }
  return { ...updated, sba_written_logical_fields: writtenLogicalFields };
}

async function updateInboundCallerFromSession(
  call,
  overrides = {},
  options = {}
) {
  if (!INBOUND_MONDAY_CONNECTED) {
    inboundLog("[MONDAY_UPDATE]", "update_skipped_not_configured", {
      call_id: call?.call_id || null,
      update_success: false
    });
    return null;
  }
  if (!call?.monday_item_id) {
    inboundLog("[MONDAY_LINK]", "MONDAY_ITEM_ID_MISSING", {
      call_id: call?.call_id || null,
      recovery_attempted: Boolean(call?.call_id)
    });
    if (call?.call_id) {
      await ensureInboundMondayCaller(call);
      call = (await getCallById(call.call_id)) || call;
    }
    if (!call?.monday_item_id) {
      inboundLog("[MONDAY_LINK]", "missing_item_recovery_failed", {
        call_id: call?.call_id || null,
        identity_disambiguation_required:
          call?.result?.sba_lead_match_status === "multiple_matches"
      });
      return null;
    }
    inboundLog("[MONDAY_LINK]", "missing_item_recovered", {
      call_id: call.call_id,
      monday_item_id: String(call.monday_item_id)
    });
  }
  const snapshot = inboundCallSnapshot(call, overrides);
  const previousSnapshot = call.result?.inbound_monday_snapshot || null;
  if (inboundValuesEqual(previousSnapshot, snapshot)) {
    return { id: String(call.monday_item_id), skipped: true };
  }
  const changedValues =
    options.forceFullState === true || !previousSnapshot
      ? snapshot
      : Object.fromEntries(
          Object.entries(snapshot).filter(
            ([field, value]) =>
              !inboundSessionFieldEqual(field, previousSnapshot[field], value)
          )
        );
  try {
    return await retryTransientOperation(
      async () => {
      const updated = await updateInboundCallerItem(
        call.monday_item_id,
        changedValues,
        { callId: call.call_id }
      );
      if (!updated?.id) {
        throw new Error("monday.com did not confirm the inbound item update.");
      }
      await pool.query(
        `UPDATE ai_calls SET monday_last_sync_at = NOW(),
         monday_last_error = NULL, result = result || $2::jsonb,
         updated_at = NOW() WHERE call_id = $1`,
        [
          call.call_id,
          JSON.stringify({
            inbound_monday_snapshot: snapshot,
            monday_item_id: String(call.monday_item_id)
          })
        ]
      );
      return updated;
      },
      {
        maxAttempts: options.maxAttempts || 1,
        onAttemptFailure: ({ attempt, maxAttempts, retryable, error }) => {
          inboundLog(
            retryable ? "[MONDAY_UPDATE]" : "[MONDAY_ERROR]",
            "inbound_update_attempt_failed",
            {
              call_id: call.call_id,
              item_id: String(call.monday_item_id),
              attempt,
              max_attempts: maxAttempts,
              retryable,
              error: cleanText(error.message, 300)
            }
          );
        }
      }
    );
  } catch (error) {
    await pool.query(
      `UPDATE ai_calls SET monday_last_error = $2, updated_at = NOW()
       WHERE call_id = $1`,
      [call.call_id, cleanText(error.message, 4000)]
    );
    throw error;
  }
}

async function moveInboundCallerToGroup(itemId, groupId) {
  if (!itemId || !groupId) return null;
  const result = await mondayGraphql(
    `mutation MoveInboundCaller($itemId: ID!, $groupId: String!) {
      move_item_to_group(item_id: $itemId, group_id: $groupId) { id }
    }`,
    { itemId: String(itemId), groupId }
  );
  return result.move_item_to_group || null;
}

async function createInboundFollowUpRecord(itemId, reason, followUpDate) {
  if (!itemId) return null;
  const itemName = `Funding follow-up - ${cleanText(reason, 100) || "Funding review"}`;
  const columnValues = {
    [SBA_BOARD.subitemColumns.status]: { label: "Working on it" },
    ...(followUpDate
      ? { [SBA_BOARD.subitemColumns.date]: { date: followUpDate } }
      : {})
  };
  const result = await mondayGraphql(
    `mutation CreateInboundFollowUp($parentItemId: ID!, $itemName: String!, $columnValues: JSON!) {
      create_subitem(parent_item_id: $parentItemId, item_name: $itemName, column_values: $columnValues) { id name }
    }`,
    { parentItemId: String(itemId), itemName, columnValues: JSON.stringify(columnValues) }
  );
  return result.create_subitem || null;
}

function inboundCallSnapshot(call, overrides = {}) {
  const payload = call?.payload || {};
  const result = call?.result || {};
  const suppliedName = normalizeInboundFullName(
    firstInboundContactValue(
      160,
      result.full_name,
      overrides.full_name,
      overrides.name
    )
  );
  const firstName = firstInboundContactValue(
    100,
    result.first_name,
    overrides.first_name,
    suppliedName.first_name
  );
  const lastName = firstInboundContactValue(
    100,
    result.last_name,
    overrides.last_name,
    suppliedName.last_name
  );
  const fullName = firstInboundContactValue(
    160,
    [firstName, lastName].filter(Boolean).join(" "),
    result.full_name,
    overrides.full_name,
    suppliedName.full_name
  );
  return {
    name: inboundCallerItemName({
      full_name: fullName,
      phone: overrides.phone || call?.phone || payload.phone_number ||
        payload.caller_phone,
      date_called: overrides.date_called || result.date_called ||
        call?.started_at || call?.created_at
    }),
    full_name: fullName,
    first_name: firstName,
    last_name: lastName,
    phone: normalizePhone(
      result.phone || result.phone_number || call?.phone || overrides.phone ||
        payload.phone_number || payload.caller_phone
    ),
    email: normalizeInboundEmail(
      result.email || overrides.email
    ),
    business_name: cleanText(result.business_name || overrides.business_name, 200),
    business_entity_type: normalizeSbaEntityType(result.business_entity_type || overrides.business_entity_type),
    entity_status: cleanText(result.entity_status || overrides.entity_status, 100),
    time_in_business: cleanText(result.time_in_business || overrides.time_in_business, 200),
    industry: cleanText(result.industry || overrides.industry, 200),
    city: cleanText(result.city || overrides.city, 160),
    state: cleanText(result.state || overrides.state, 100),
    zip: cleanText(result.zip || overrides.zip, 20),
    lead_id: cleanText(result.lead_id || overrides.lead_id, 160),
    funding_use: cleanText(result.funding_use || overrides.funding_use, 500),
    funding_amount: cleanText(result.funding_amount || overrides.funding_amount, 100),
    funding_timeline: cleanText(result.funding_timeline || overrides.funding_timeline, 300),
    estimated_credit_score: normalizeSbaCreditRange(result.estimated_credit_score || overrides.estimated_credit_score),
    credit_status: cleanText(result.credit_status || overrides.credit_status, 100),
    gross_monthly_revenue: normalizeSbaRevenueRange(result.gross_monthly_revenue || overrides.gross_monthly_revenue),
    income_status: cleanText(result.income_status || overrides.income_status, 100),
    monthly_business_expenses: cleanText(result.monthly_business_expenses || overrides.monthly_business_expenses, 100),
    existing_business_financing: cleanText(result.existing_business_financing || overrides.existing_business_financing, 500),
    taxes: cleanText(result.taxes || result.tax_returns_status || overrides.taxes || overrides.tax_returns_status, 500),
    tax_status: cleanText(result.tax_status || overrides.tax_status, 100),
    tax_years_available: cleanText(result.tax_years_available || overrides.tax_years_available, 100),
    bank_statements_available: result.bank_statements_available ?? overrides.bank_statements_available,
    financial_statements_available: result.financial_statements_available ?? overrides.financial_statements_available,
    preliminary_readiness: cleanText(result.preliminary_readiness || overrides.preliminary_readiness, 500),
    missing_items: Array.isArray(result.missing_items) ? result.missing_items : overrides.missing_items,
    next_action: cleanText(result.next_action || call?.next_action || overrides.next_action, 500),
    date_called: inboundDateCalled(
      overrides.date_called || result.date_called ||
        call?.started_at || call?.created_at
    ),
    summary: mostCompleteInboundSummary(
      result.call_summary,
      result.agent_call_summary,
      overrides.summary,
      result.summary,
      call?.summary
    ),
    caller_type: "Inbound Call",
    priority: cleanText(overrides.priority || call?.priority, 30),
    inbound_status: cleanText(
      overrides.inbound_status || result.inbound_status,
      100
    ),
    call_status: cleanText(
      overrides.call_status || result.call_status ||
        result.call_outcome || call?.outcome || call?.status,
      100
    ),
    lead_source: cleanText(
      overrides.lead_source || result.lead_source || payload.lead_source,
      160
    ),
    call_direction: cleanText(call?.direction || payload.direction, 20),
    completion_date: cleanText(
      result.call_ended_at || call?.completed_at ||
        overrides.completion_date,
      100
    )
  };
}

function buildInboundCompletionSummary(call, completionStatus, sourceSummary) {
  const result = call?.result || {};
  const detailedSummary = cleanText(sourceSummary, 1800);
  const normalCompletion =
    result.normal_completion_recorded === true ||
    result.completion_reason === "normal_completion";
  const terminalSummary = normalCompletion
    ? "Completed normally"
    : `Disconnected (${cleanText(completionStatus || call?.status, 50) || "unknown"})`;
  const reason = cleanText(
    result.reason_for_call || result.inbound_intent || call?.intent,
    200
  ) || "Not captured";
  return cleanText(
    [
      "Inbound SBA Funding Call",
      ...(detailedSummary ? [`Call details: ${detailedSummary}`] : []),
      `Intent: ${reason}`,
      `Funding request: ${cleanText(result.funding_amount, 100) || "Not provided"}`,
      `Use of funds: ${cleanText(result.funding_use, 500) || "Not provided"}`,
      `Business profile: ${[result.business_entity_type, result.industry].filter(Boolean).join(", ") || "Not provided"}`,
      `Location: ${[result.city, result.state].filter(Boolean).join(", ") || "Not provided"}`,
      `Time in business: ${cleanText(result.time_in_business, 200) || "Not provided"}`,
      `Monthly revenue: ${cleanText(result.gross_monthly_revenue, 120) || "Not provided"}`,
      `Estimated credit: ${cleanText(result.estimated_credit_score, 100) || "Not provided"}`,
      `Taxes/documents: ${cleanText(result.taxes || result.tax_returns_status, 500) || "Not provided"}`,
      `Preliminary readiness: ${cleanText(result.preliminary_readiness, 500) || "Not assessed"}`,
      `Missing items: ${Array.isArray(result.missing_items) ? result.missing_items.join(", ") : cleanText(result.missing_items, 500) || "None recorded"}`,
      `Next action: ${cleanText(call?.next_action || result.next_action, 500) || "None recorded"}`,
      terminalSummary
    ].join("; "),
    4000
  );
}

async function saveInboundCallSummary(data = {}) {
  const callId = cleanText(data.call_id, 100);
  if (!callId) return null;
  await pool.query(
    `UPDATE ai_calls SET intent = COALESCE($2, intent), outcome = COALESCE($3, outcome),
       summary = COALESCE($4, summary), next_action = COALESCE($5, next_action),
       updated_at = NOW() WHERE call_id = $1`,
    [callId, cleanText(data.intent, 80), cleanText(data.outcome, 80), cleanText(data.summary, 4000), cleanText(data.next_action, 2000)]
  );
  let call = await getCallById(callId);
  if (!call) return null;
  const sourceSummary = bestInboundDetailedSummary(call, data.summary);
  const summary = buildInboundCompletionSummary(
    call,
    data.completion_status,
    sourceSummary
  );
  const snapshot = inboundCallSnapshot(call, {
    ...data,
    summary,
    caller_type: "Inbound Call"
  });
  await pool.query(
    `UPDATE ai_calls SET summary = $2, result = result || $3::jsonb,
     updated_at = NOW() WHERE call_id = $1`,
    [
      callId,
      summary,
      JSON.stringify({
        full_name: snapshot.full_name,
        first_name: snapshot.first_name,
        last_name: snapshot.last_name,
        business_name: snapshot.business_name,
        business_entity_type: snapshot.business_entity_type,
        entity_status: snapshot.entity_status,
        time_in_business: snapshot.time_in_business,
        industry: snapshot.industry,
        city: snapshot.city,
        state: snapshot.state,
        zip: snapshot.zip,
        lead_id: snapshot.lead_id,
        funding_use: snapshot.funding_use,
        funding_amount: snapshot.funding_amount,
        funding_timeline: snapshot.funding_timeline,
        phone: snapshot.phone,
        email: snapshot.email,
        estimated_credit_score: snapshot.estimated_credit_score,
        credit_status: snapshot.credit_status,
        gross_monthly_revenue: snapshot.gross_monthly_revenue,
        income_status: snapshot.income_status,
        monthly_business_expenses: snapshot.monthly_business_expenses,
        existing_business_financing: snapshot.existing_business_financing,
        taxes: snapshot.taxes,
        tax_status: snapshot.tax_status,
        tax_years_available: snapshot.tax_years_available,
        bank_statements_available: snapshot.bank_statements_available,
        financial_statements_available: snapshot.financial_statements_available,
        preliminary_readiness: snapshot.preliminary_readiness,
        missing_items: snapshot.missing_items,
        date_called: snapshot.date_called,
        ...(sourceSummary ? { agent_call_summary: sourceSummary } : {}),
        summary,
        call_summary: sourceSummary || summary,
        caller_type: snapshot.caller_type,
        call_status: snapshot.call_status,
        lead_source: snapshot.lead_source,
        call_direction: snapshot.call_direction,
        completion_date: snapshot.completion_date,
        disconnect_reason: cleanText(
          data.disconnect_reason || call.result?.disconnect_reason,
          500
        ),
        monday_item_id: call.monday_item_id || null,
        call_sid: call.twilio_call_sid || null
      })
    ]
  );
  call = await getCallById(callId);
  if (!INBOUND_MONDAY_CONNECTED) return call;
  try {
    const persisted = await persistCallSessionToMonday(callId, {
      alreadySerialized: true,
      forceFullState: true,
      maxAttempts: 3,
      overrides: {
        ...data,
        summary
      }
    });
    if (persisted?.success !== true) {
      throw new Error("monday.com did not confirm the inbound item update.");
    }
    call = (await getCallById(callId)) || call;
    if (data.group_id) await moveInboundCallerToGroup(call.monday_item_id, data.group_id);
    await mergeCallResult(callId, {
      monday_item_id: String(call.monday_item_id)
    });
    inboundLog("[MONDAY]", "inbound_call_saved", {
      call_id: callId,
      item_id: String(call.monday_item_id)
    });
  } catch (error) {
    inboundLog("[MONDAY]", "summary_update_failed", {
      call_id: callId,
      error: cleanText(error.message, 300)
    });
    throw error;
  }
  return call;
}

async function persistFinalInboundSession(data = {}) {
  const callId = cleanText(data.call_id, 100);
  if (!callId) return null;
  const diagnostic = inboundMondayDiagnosticContext(callId);
  const previous =
    inboundFinalPersistenceChains.get(callId) || Promise.resolve();
  const operation = previous
    .catch(() => undefined)
    .then(() =>
      inboundSessionPersistenceQueue.run(callId, () =>
        saveInboundCallSummary(data)
      )
    );
  const pending = withTimeout(
    operation,
    INBOUND_FINAL_PERSIST_TIMEOUT_MS,
    `Final inbound monday.com persistence timed out after ${INBOUND_FINAL_PERSIST_TIMEOUT_MS}ms.`
  );
  inboundFinalPersistenceChains.set(callId, pending);
  try {
    const call = await pending;
    inboundLog("[MONDAY]", "final_inbound_persistence_completed", {
      call_id: callId,
      item_id: call?.monday_item_id || null,
      monday_connected: INBOUND_MONDAY_CONNECTED
    });
    if (diagnostic) {
      inboundLog("[MONDAY_FINAL_SAVE]", "final_save_result", {
        ...diagnostic,
        success: true,
        monday_item_id: call?.monday_item_id || null,
        final_session: sanitizeMondayDiagnostic(call?.result || {})
      });
    }
    return call;
  } catch (error) {
    if (diagnostic) {
      inboundLog("[MONDAY_ERROR]", "final_save_failed", {
        ...diagnostic,
        success: false,
        error: cleanText(error.message, 1000)
      });
    }
    throw error;
  } finally {
    if (
      diagnostic &&
      inboundMondayDiagnosticClaimedCallId === callId
    ) {
      inboundMondayDiagnosticCompleted = true;
    }
    if (inboundFinalPersistenceChains.get(callId) === pending) {
      inboundFinalPersistenceChains.delete(callId);
    }
  }
}

async function syncInboundMondayCaller(call) {
  if (!call || !INBOUND_MONDAY_CONNECTED) return null;
  const normalizedCallerPhone = normalizePhone(call.phone);
  inboundLog("[MONDAY_LINK]", "link_started", {
    call_id: call.call_id,
    normalized_caller_phone_last4: maskedPhoneLastFour(normalizedCallerPhone),
    current_monday_item_id: call.monday_item_id || null
  });
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const lockKey = `inbound-monday:${normalizePhone(call.phone) || call.call_id}`;
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [lockKey]);
    const locked = await client.query(
      "SELECT * FROM ai_calls WHERE call_id = $1 FOR UPDATE",
      [call.call_id]
    );
    const latestCall = locked.rows[0] || call;
    if (latestCall.monday_item_id) {
      await client.query("COMMIT");
      inboundLog("[MONDAY_LINK]", "link_already_present", {
        call_id: call.call_id,
        monday_item_id: String(latestCall.monday_item_id)
      });
      return { id: String(latestCall.monday_item_id) };
    }
    call = latestCall;
    const initialData = {
      ...inboundCallSnapshot(call, {
        date_called: call.started_at || call.created_at ||
          new Date().toISOString().slice(0, 10),
        call_status: "Inbound Call Started",
        caller_type: "Inbound Call",
        inbound_status: "New Inbound Call",
        follow_up_needed: "No"
      }),
      source: "Inbound - Phone"
    };
    const diagnostic = inboundMondayDiagnosticContext(call.call_id);
    if (diagnostic) {
      inboundLog("[MONDAY_DIAGNOSTIC]", "session_snapshot_before_create", {
        ...diagnostic,
        twilio_call_sid: call.twilio_call_sid || null,
        board_id: MONDAY_BOARD_ID,
        session: sanitizeMondayDiagnostic(initialData)
      });
    }
    const resolved = await retryTransientOperation(
      async () => {
        const matches = await findInboundCallersByPhone(call.phone, {
          callId: call.call_id
        });
        if (matches.length > 0) {
          const selection = selectBestSbaMondayMatch(matches);
          if (!selection?.item?.id) {
            throw new Error("A phone match was returned without a resolvable monday.com item ID.");
          }
          if (matches.length > 1) {
            inboundLog("[MONDAY_LINK]", "multiple_matches_resolved", {
              call_id: call.call_id,
              match_count: matches.length,
              selected_item_id: String(selection.item.id),
              selection_reason: selection.selection_reason
            });
          }
          inboundLog("[MONDAY_LINK]", "existing_item_matched", {
            call_id: call.call_id,
            match_count: matches.length,
            existing_item_id: String(selection.item.id)
          });
          return {
            item: selection.item,
            existing: true,
            profile: sbaProfileFromMondayItem(selection.item),
            selectionReason: selection.selection_reason
          };
        }
        const item = await createInboundCallerItem(initialData, {
          callId: call.call_id,
          twilioCallSid: call.twilio_call_sid
        });
        inboundLog("[MONDAY_LINK]", "new_item_created", {
          call_id: call.call_id,
          match_count: 0,
          newly_created_item_id: item?.id ? String(item.id) : null
        });
        return { item, existing: false };
      },
      {
        maxAttempts: 3,
        onAttemptFailure: ({ attempt, maxAttempts, retryable, error }) => {
          inboundLog(
            retryable ? "[MONDAY_CREATE]" : "[MONDAY_ERROR]",
            "item_creation_attempt_failed",
            {
              call_id: call.call_id,
              attempt,
              max_attempts: maxAttempts,
              retryable,
              error: cleanText(error.message, 300)
            }
          );
        }
      }
    );
    const { item, existing, profile, selectionReason } = resolved;
    if (!item?.id) {
      throw new Error("Monday linkage did not resolve or create an item ID.");
    }
    const resolvedItemId = String(item.id);
    if (existing) {
      await updateInboundCallerItem(resolvedItemId, {
        phone: initialData.phone,
        updated_date: new Date().toISOString()
      }, {
        callId: call.call_id
      });
    }
    await client.query(
      `UPDATE ai_calls SET monday_item_id = $2, monday_group_id = $3,
       monday_last_sync_at = NOW(), monday_last_error = NULL,
       result = result || $4::jsonb, updated_at = NOW()
       WHERE call_id = $1`,
      [
        call.call_id,
        resolvedItemId,
        item.group?.id || INBOUND_MONDAY.groups.newInboundCalls,
        JSON.stringify({
          ...(profile || {}),
          full_name: profile?.full_name || initialData.full_name,
          first_name: profile?.first_name || initialData.first_name,
          last_name: profile?.last_name || initialData.last_name,
          city: profile?.city || initialData.city,
          zip: profile?.zip || initialData.zip,
          phone: initialData.phone,
          email: profile?.email || initialData.email,
          date_called: initialData.date_called,
          summary: initialData.summary,
          caller_type: "Inbound Call",
          call_status: initialData.call_status,
          follow_up_needed: initialData.follow_up_needed,
          lead_source: initialData.lead_source,
          sba_lead_match_status: existing ? "matched" : "new",
          sba_lead_selection_reason: selectionReason || null,
          existing_profile_loaded: Boolean(existing && profile),
          inbound_monday_snapshot: initialData,
          monday_item_id: resolvedItemId,
          call_sid: call.twilio_call_sid || null
        })
      ]
    );
    inboundLog("[MONDAY_LINK]", "item_id_persisted", {
      call_id: call.call_id,
      monday_item_id: resolvedItemId,
      existing_item: Boolean(existing),
      written_to_ai_calls: true
    });
    await client.query("COMMIT");
    return item;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    inboundLog("[MONDAY_LINK]", "link_failed", {
      call_id: call.call_id,
      caller_phone: maskedPhoneLastFour(call.phone),
      error: cleanText(error.message, 300)
    });
    return null;
  } finally {
    client.release();
  }
}

async function ensureInboundMondayCaller(call) {
  if (!call || !INBOUND_MONDAY_CONNECTED) return null;
  const callId = cleanText(call.call_id, 100);
  if (!callId) return null;
  const existing = inboundMondayCallerPromises.get(callId);
  if (existing) return existing;
  const pending = syncInboundMondayCaller(call);
  inboundMondayCallerPromises.set(callId, pending);
  try {
    return await pending;
  } finally {
    if (inboundMondayCallerPromises.get(callId) === pending) {
      inboundMondayCallerPromises.delete(callId);
    }
  }
}

async function persistCallSessionToMonday(callId, options = {}) {
  const normalizedCallId = cleanText(callId, 100);
  if (!normalizedCallId) {
    return { success: false, error: "A call ID is required." };
  }
  if (!INBOUND_MONDAY_CONNECTED) {
    return {
      success: false,
      configured: false,
      error: "Inbound monday.com is not configured."
    };
  }
  const persisted = await persistLatestSession({
    callId: normalizedCallId,
    queue: inboundSessionPersistenceQueue,
    alreadySerialized: options.alreadySerialized === true,
    loadSession: getCallById,
    ensureItem: ensureInboundMondayCaller,
    buildPayload: async (call) => {
      const payload = inboundCallSnapshot(call, options.overrides || {});
      const diagnostic = inboundMondayDiagnosticContext(normalizedCallId);
      if (diagnostic) {
        inboundLog("[MONDAY_DIAGNOSTIC]", "session_snapshot", {
          ...diagnostic,
          twilio_call_sid: call.twilio_call_sid || null,
          monday_item_id: String(call.monday_item_id),
          board_id: MONDAY_BOARD_ID,
          session: sanitizeMondayDiagnostic(call.result || {}),
          normalized_fields: sanitizeMondayDiagnostic(payload)
        });
      }
      return payload;
    },
    updateItem: async (_itemId, payload, call) => {
      const updated = await updateInboundCallerFromSession(call, payload, {
        maxAttempts: options.maxAttempts || 3,
        forceFullState: options.forceFullState === true
      });
      if (!updated?.id) {
        throw new Error("monday.com did not confirm the session update.");
      }
      return updated;
    }
  });
  return {
    success: true,
    call_id: normalizedCallId,
    monday_item_id: persisted.itemId,
    payload: persisted.payload,
    skipped: persisted.result?.skipped === true
  };
}

async function persistSbaQualificationFieldsToMonday(callId, fields = {}) {
  let call = await getCallById(callId);
  if (!call) throw new Error("The call session was not found.");
  if (!call.monday_item_id) {
    inboundLog("[MONDAY_LINK]", "MONDAY_ITEM_ID_MISSING", {
      call_id: callId,
      recovery_attempted: true,
      source: "save_inbound_caller_context"
    });
    await ensureInboundMondayCaller(call);
    call = await getCallById(callId);
  }
  const itemId = cleanText(call?.monday_item_id, 100);
  inboundLog("[MONDAY_LINK]", "save_tool_item_result", {
    call_id: callId,
    monday_item_id: itemId || null,
    found: Boolean(itemId)
  });
  if (!itemId) throw new Error("The monday.com item ID is unavailable.");

  const qualificationPatch = {
    ...buildSbaQualificationSessionPatch(fields),
    updated_date: new Date().toISOString()
  };
  const logicalFields = Object.keys(qualificationPatch).filter(
    (field) => field !== "updated_date"
  );
  const expectedColumnIds = [...new Set([
    ...logicalFields
      .map((field) => SBA_QUALIFICATION_COLUMN_IDS[field])
      .filter(Boolean),
    SBA_QUALIFICATION_COLUMN_IDS.updated_date
  ])];
  inboundLog("[MONDAY_UPDATE]", "qualification_update_attempt", {
    call_id: callId,
    monday_item_id: itemId,
    logical_fields: logicalFields,
    column_ids: expectedColumnIds
  });

  const updated = await retryTransientOperation(
    () => updateInboundCallerItem(itemId, qualificationPatch, {
      callId,
      source: "save_inbound_caller_context"
    }),
    { maxAttempts: 3 }
  );
  if (!updated?.id) throw new Error("monday.com did not confirm the qualification update.");
  const writtenLogicalFields = Array.isArray(updated.sba_written_logical_fields)
    ? updated.sba_written_logical_fields
    : [];

  call = (await getCallById(callId)) || call;
  const snapshot = inboundCallSnapshot(call, qualificationPatch);
  await pool.query(
    `UPDATE ai_calls SET monday_last_sync_at = NOW(), monday_last_error = NULL,
     result = result || $2::jsonb, updated_at = NOW() WHERE call_id = $1`,
    [
      callId,
      JSON.stringify({
        inbound_monday_snapshot: snapshot,
        monday_item_id: itemId
      })
    ]
  );
  inboundLog("[MONDAY_UPDATE]", "qualification_update_succeeded", {
    call_id: callId,
    monday_item_id: itemId,
    logical_fields: writtenLogicalFields,
    captured_logical_fields: logicalFields,
    column_ids: expectedColumnIds
  });
  return {
    success: true,
    monday_item_id: itemId,
    logical_fields: writtenLogicalFields,
    captured_logical_fields: logicalFields
  };
}

function isInboundCallSession(call) {
  return Boolean(
    call &&
      (call.direction === "inbound" || call.payload?.direction === "inbound")
  );
}

async function persistInboundMilestone(call, event, overrides = {}) {
  if (
    !INBOUND_MONDAY_CONNECTED ||
    !isInboundCallSession(call)
  ) {
    return false;
  }
  try {
    const persisted = await persistCallSessionToMonday(call.call_id, {
      forceFullState: true,
      maxAttempts: 3,
      overrides
    });
    inboundLog("[MONDAY]", "inbound_milestone_persisted", {
      call_id: call.call_id,
      event,
      monday_item_id: persisted?.monday_item_id || null,
      success: persisted?.success === true
    });
    return persisted?.success === true;
  } catch (error) {
    inboundLog("[MONDAY_ERROR]", "inbound_milestone_failed", {
      call_id: call.call_id,
      event,
      error: cleanText(error.message, 500)
    });
    return false;
  }
}

function buildMondayBoardMetadata(board) {
  const columns = Array.isArray(board.columns) ? board.columns : [];
  const groups = Array.isArray(board.groups) ? board.groups : [];

  const columnByTitle = new Map();
  for (const column of columns) {
    const key = normalizeMondayKey(column.title);
    if (key && !columnByTitle.has(key)) {
      columnByTitle.set(key, {
        ...column,
        settings: parseMondaySettings(column.settings)
      });
    }
  }

  const groupByTitle = new Map();
  for (const group of groups) {
    const key = normalizeMondayKey(group.title);
    if (key && !groupByTitle.has(key)) groupByTitle.set(key, group);
  }

  return {
    id: String(board.id),
    name: board.name,
    columns,
    groups,
    columnByTitle,
    groupByTitle
  };
}

async function loadMondayMetadata(options = {}) {
  if (!MONDAY_SYNC_ENABLED) return null;

  const force = options.force === true;
  if (
    !force &&
    mondayMetadataCache &&
    Date.now() < mondayMetadataExpiresAt
  ) {
    return mondayMetadataCache;
  }

  const query = `
    query HeluxMondayMetadata($mainIds: [ID!], $subitemIds: [ID!]) {
      main: boards(ids: $mainIds) {
        id
        name
        groups { id title }
        columns { id title type settings }
      }
      subitems: boards(ids: $subitemIds) {
        id
        name
        groups { id title }
        columns { id title type settings }
      }
    }
  `;

  const data = await mondayRequest(query, {
    mainIds: [MONDAY_BOARD_ID],
    subitemIds: [MONDAY_SUBITEM_BOARD_ID]
  });

  const mainBoard = Array.isArray(data.main) ? data.main[0] : null;
  const subitemBoard = Array.isArray(data.subitems) ? data.subitems[0] : null;

  if (!mainBoard) {
    throw new Error(`monday.com main board ${MONDAY_BOARD_ID} was not found.`);
  }

  if (!subitemBoard) {
    throw new Error(
      `monday.com subitem board ${MONDAY_SUBITEM_BOARD_ID} was not found.`
    );
  }

  mondayMetadataCache = {
    loadedAt: new Date().toISOString(),
    main: buildMondayBoardMetadata(mainBoard),
    subitems: buildMondayBoardMetadata(subitemBoard)
  };
  mondayMetadataExpiresAt = Date.now() + MONDAY_METADATA_CACHE_MS;

  return mondayMetadataCache;
}

function findMondayColumn(boardMetadata, aliases) {
  const list = Array.isArray(aliases) ? aliases : [aliases];

  for (const alias of list) {
    const exact = boardMetadata.columnByTitle.get(normalizeMondayKey(alias));
    if (exact) return exact;
  }

  for (const alias of list) {
    const normalizedAlias = normalizeMondayKey(alias);
    for (const column of boardMetadata.columns) {
      const normalizedTitle = normalizeMondayKey(column.title);
      if (
        normalizedAlias &&
        (normalizedTitle.includes(normalizedAlias) ||
          normalizedAlias.includes(normalizedTitle))
      ) {
        return {
          ...column,
          settings: parseMondaySettings(column.settings)
        };
      }
    }
  }

  return null;
}

function findMondayColumnById(boardMetadata, columnId) {
  const column = boardMetadata.columns.find(
    (candidate) => String(candidate.id) === String(columnId)
  );
  return column
    ? { ...column, settings: parseMondaySettings(column.settings) }
    : null;
}

function findMondayGroup(boardMetadata, aliases) {
  const list = Array.isArray(aliases) ? aliases : [aliases];
  for (const alias of list) {
    const exact = boardMetadata.groupByTitle.get(normalizeMondayKey(alias));
    if (exact) return exact;
  }
  return null;
}

function mondayDateValue(value) {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const iso = date.toISOString();
  return {
    date: iso.slice(0, 10),
    time: iso.slice(11, 19)
  };
}

function mondayPhoneCountry(phone) {
  const normalized = String(phone || "");
  if (normalized.startsWith("+55")) return "BR";
  if (normalized.startsWith("+57")) return "CO";
  if (normalized.startsWith("+52")) return "MX";
  if (normalized.startsWith("+44")) return "GB";
  if (normalized.startsWith("+61")) return "AU";
  if (normalized.startsWith("+1")) return "US";
  return "US";
}

function mondayStatusValue(column, desiredLabel) {
  if (!desiredLabel) return null;
  const settings = parseMondaySettings(column.settings);
  const labels = Array.isArray(settings.labels)
    ? settings.labels
    : Object.entries(settings.labels || {}).map(([id, label]) => ({
        id,
        label: typeof label === "object" ? label?.label || label?.name : label
      }));
  const wanted = normalizeMondayKey(desiredLabel);
  const found = labels.find(
    (label) => normalizeMondayKey(label.label) === wanted
  );

  if (!found) return null;
  return { index: Number(found.id) };
}

function mondayStatusLabels(column) {
  const settings = parseMondaySettings(column?.settings);
  return Array.isArray(settings.labels)
    ? settings.labels
    : Object.entries(settings.labels || {}).map(([id, label]) => ({
        id,
        label: typeof label === "object" ? label?.label || label?.name : label
      }));
}

function timeFrameMeaning(value) {
  const normalized = normalizeMondayKey(value);
  if (!normalized) return null;
  if (/justlooking|lookingonly/.test(normalized)) {
    return { normalized, category: "just_looking", months: null };
  }
  if (/notsure|unsure|dontknow|donotknow/.test(normalized)) {
    return { normalized, category: "not_sure", months: null };
  }
  if (/3060/.test(normalized)) {
    return { normalized, category: "timeframe", months: 2 };
  }
  if (/6090/.test(normalized)) {
    return { normalized, category: "timeframe", months: 3 };
  }
  const wordMonths = [
    [/(?:two|2)months?/, 2],
    [/(?:four|4)months?/, 4],
    [/(?:six|6)months?/, 6],
    [/(?:one|1)year|(?:twelve|12)months?/, 12]
  ];
  const matched = wordMonths.find(([pattern]) => pattern.test(normalized));
  if (matched) {
    return { normalized, category: "timeframe", months: matched[1] };
  }
  return null;
}

function mondayTimeFrameValue(column, customerAnswer, callId) {
  const answerMeaning = timeFrameMeaning(customerAnswer);
  const labels = mondayStatusLabels(column);
  let best = null;

  if (answerMeaning) {
    for (const label of labels) {
      const labelText = cleanText(label.label, 160);
      const labelMeaning = timeFrameMeaning(labelText);
      if (!labelText || !labelMeaning) continue;
      let score = 0;
      if (normalizeMondayKey(labelText) === answerMeaning.normalized) score = 100;
      else if (
        answerMeaning.category !== "timeframe" &&
        answerMeaning.category === labelMeaning.category
      ) score = 95;
      else if (
        answerMeaning.months !== null &&
        answerMeaning.months === labelMeaning.months
      ) score = 90;
      else if (
        answerMeaning.months !== null &&
        labelMeaning.months !== null &&
        /within/.test(normalizeMondayKey(labelText)) &&
        answerMeaning.months <= labelMeaning.months
      ) score = Math.max(60, 80 - (labelMeaning.months - answerMeaning.months));
      if (score > (best?.score || 0)) best = { ...label, label: labelText, score };
    }
  }

  const success = Boolean(best && best.score >= 60);
  console.log(JSON.stringify({
    event: "monday_time_frame_mapping",
    call_id: callId,
    customer_answer: cleanText(customerAnswer, 160),
    normalized_answer: answerMeaning?.normalized || null,
    matched_label: success ? best.label : null,
    matched_index: success ? String(best.id) : null,
    success
  }));
  return success ? { index: Number(best.id) } : null;
}

function mondayColumnValue(column, value) {
  if (value === undefined || value === null || value === "") return null;

  const type = String(column.type || "").toLowerCase();

  if (["status", "color"].includes(type)) {
    return mondayStatusValue(column, value);
  }

  if (type === "dropdown") {
    return { labels: [String(value)] };
  }

  if (["numbers", "numeric"].includes(type)) {
    const numberValue = Number(value);
    return Number.isFinite(numberValue) ? String(numberValue) : null;
  }

  if (["date", "timeline"].includes(type)) {
    return mondayDateValue(value);
  }

  if (["checkbox", "boolean"].includes(type)) {
    return value === true ? { checked: "true" } : null;
  }

  if (type === "phone") {
    return {
      phone: String(value),
      countryShortName: mondayPhoneCountry(value)
    };
  }

  if (["people", "multiple_person", "person"].includes(type)) {
    const id = Number(value);
    if (!Number.isInteger(id) || id <= 0) return null;
    return { personsAndTeams: [{ id, kind: "person" }] };
  }

  return String(value).slice(0, 10000);
}

function assignMondayValue(target, boardMetadata, aliases, value) {
  const column = findMondayColumn(boardMetadata, aliases);
  if (!column) return;
  const formatted = mondayColumnValue(column, value);
  if (formatted === null || formatted === undefined) return;
  target[column.id] = formatted;
}

function assignMondayValueById(target, boardMetadata, columnId, value) {
  const column = findMondayColumnById(boardMetadata, columnId);
  if (!column) return;
  const formatted = mondayColumnValue(column, value);
  if (formatted === null || formatted === undefined) return;
  target[column.id] = formatted;
}

function sequenceStatusLabel(call) {
  if (call.do_not_call) return "Do Not Call";
  if (call.wrong_number) return "Wrong Number";
  if (call.invalid_number) return "Invalid Number";

  const map = {
    ready: "Ready",
    calling: "Calling",
    human_action: "Human Action",
    completed: "Completed",
    exhausted: "Exhausted",
    paused: "Paused",
    suppressed: "Do Not Call"
  };

  return map[String(call.sequence_status || "").toLowerCase()] || null;
}

function lastCallResultLabel(call, latestAttempt) {
  const status = String(
    latestAttempt?.technical_status || call.status || ""
  ).toLowerCase();
  const answeredBy = String(latestAttempt?.answered_by || "").toLowerCase();

  if (answeredBy === "voicemail") return "Voicemail";
  if (status === "no-answer") return "No Answer";
  if (status === "busy") return "Busy";
  if (["failed", "canceled"].includes(status)) return "Failed";
  if (status === "disconnected") return "Disconnected";
  if (["answered", "in-progress", "completed"].includes(status)) {
    return "Connected";
  }
  return null;
}

function businessOutcomeLabel(outcome) {
  const map = {
    qualified: "Qualified",
    hot_transfer: "Hot Transfer",
    specialist_handoff: "Specialist Handoff",
    application_link_sent: "Application Sent",
    dti_calculator_sent: "DTI Sent",
    agent_notified: "Agent Notified",
    needs_review: "Needs Review",
    nurture: "Nurture",
    not_interested: "Not Interested",
    opt_out: "Opt-Out"
  };
  return map[String(outcome || "").toLowerCase()] || null;
}

function technicalStatusLabel(status) {
  const map = {
    created: "Queued",
    placing: "Queued",
    queued: "Queued",
    initiated: "Queued",
    ringing: "Ringing",
    answered: "In Progress",
    "in-progress": "In Progress",
    completed: "Completed",
    busy: "Busy",
    "no-answer": "No Answer",
    failed: "Failed",
    canceled: "Canceled"
  };
  return map[String(status || "").toLowerCase()] || null;
}

function answeredByLabel(answeredBy) {
  const map = {
    human: "Human",
    voicemail: "Voicemail",
    unknown: "Unknown"
  };
  return map[String(answeredBy || "unknown").toLowerCase()] || "Unknown";
}

function targetMondayGroupTitle(call) {
  const outcome = String(call.outcome || "").toLowerCase();

  if (
    call.do_not_call ||
    call.wrong_number ||
    call.invalid_number ||
    ["not_interested", "wrong_number", "opt_out"].includes(outcome)
  ) {
    return ["Do Not Call List", "Closed or Suppressed"];
  }

  const status = String(call.sequence_status || "").toLowerCase();
  if (
    ["ready", "calling", "paused"].includes(
      status
    )
  ) {
    return ["New Leads", "Active Sequences", "Ready to Call"];
  }
  if (["human_action", "exhausted"].includes(status)) {
    return ["Agent Needed", "Human Action Needed"];
  }
  if (status === "completed") return ["Completed"];
  if (status === "suppressed") {
    return ["Do Not Call List", "Closed or Suppressed"];
  }
  return ["New Leads", "Active Sequences", "Ready to Call"];
}

function leadDisplayName(call) {
  const payload = call.payload || {};
  const fullName = [
    cleanText(payload.first_name, 80),
    cleanText(payload.last_name, 80)
  ]
    .filter(Boolean)
    .join(" ");

  return (
    fullName ||
    cleanText(payload.name, 160) ||
    call.case_id ||
    call.lead_id ||
    call.call_id
  );
}

function terminalCompletionValidation(call, sessionCallPhase) {
  const result = normalizeDaisyAnswers(call.result || {});
  const lender = normalizeExplicitYesNo(
    result.applied_with_lender ?? result.has_lender
  );
  const realtor = normalizeExplicitYesNo(result.has_realtor);
  const missing = [];

  if (sessionCallPhase === "CALL_ONE") {
    if (!cleanText(result.purchase_timeline_detail || result.time_frame, 220)) {
      missing.push("timeline");
    }
    if (lender === null) missing.push("lender");
    if (realtor === null) missing.push("realtor");
    if (!cleanText(result.purchase_area, 220)) missing.push("purchase area");
  }

  return { complete: missing.length === 0, missing, lender, realtor };
}

function buildStructuredMondayCallSummary(call, sessionCallPhase) {
  const result = normalizeDaisyAnswers(call.result || {});
  const validation = terminalCompletionValidation(call, sessionCallPhase);
  const actions = Array.isArray(call.actions) ? call.actions : [];
  const terminalRecorded =
    actions.some((action) =>
      action?.action === "complete_call" && action?.success === true
    ) || call.result?.normal_completion_recorded === true;
  const completed = terminalRecorded && validation.complete;

  const timeline = cleanText(
    result.purchase_timeline_detail || result.time_frame,
    220
  ) || "Not confirmed";
  const purchaseArea = cleanText(result.purchase_area, 220) || "Not confirmed";
  return cleanText(
    `Call One ${completed ? "completed" : "incomplete"}. ` +
    `Timeline: ${timeline}. ` +
    `Lender: ${validation.lender === null ? "Not confirmed" : (validation.lender ? "Yes" : "No")}. ` +
    `Realtor: ${validation.realtor === null ? "Not confirmed" : (validation.realtor ? "Yes" : "No")}. ` +
    `Purchase area: ${purchaseArea}.`,
    800
  );
}

function buildMainMondayValues(call, latestAttempt, metadata) {
  const values = {};
  const board = metadata.main;
  const result = normalizeDaisyAnswers(call.result || {});
  const recordedSessionPhase = [...(Array.isArray(call.actions) ? call.actions : [])]
    .reverse()
    .find((action) =>
      ["CALL_ONE", "RECONNECT", "SPECIALIST_NOTIFICATION"].includes(
        action?.session_call_phase
      )
    )?.session_call_phase;

  assignMondayValue(values, board, ["Lead ID"], call.lead_id);
  assignMondayValue(values, board, ["Case ID"], call.case_id);
  assignMondayValue(values, board, ["Phone"], call.phone);
  assignMondayValue(values, board, ["Time Zone", "Timezone"], call.timezone);
  assignMondayValue(values, board, ["AI Agent", "Agent"], "Daisy");
  assignMondayValue(
    values,
    board,
    ["Sequence Status"],
    sequenceStatusLabel(call)
  );
  assignMondayValue(values, board, ["Attempts Used"], call.attempts);
  assignMondayValue(
    values,
    board,
    ["Last Call Result"],
    lastCallResultLabel(call, latestAttempt)
  );
  assignMondayValue(
    values,
    board,
    ["Last Call"],
    call.last_attempt_at || latestAttempt?.dialed_at
  );
  assignMondayValue(
    values,
    board,
    ["Business Outcome"],
    businessOutcomeLabel(call.outcome)
  );
  assignMondayValue(
    values,
    board,
    ["Priority"],
    String(call.priority || "normal").replace(/^./, (c) => c.toUpperCase())
  );
  assignMondayValue(
    values,
    board,
    ["Consent"],
    call.consent_status === "confirmed"
      ? "Confirmed"
      : call.consent_status === "unverified"
        ? "Pending Review"
        : "Not Authorized"
  );
  assignMondayValue(values, board, ["Do Not Call"], call.do_not_call === true);
  assignMondayValue(values, board, ["Next Action"], call.next_action);
  assignMondayValue(values, board, ["Customer Timezone", "Customer Time Zone"], result.customer_timezone);
  assignMondayValue(values, board, ["Customer Timezone Label", "Customer Time Zone Label"], result.customer_timezone_label);
  assignMondayValue(values, board, ["Callback At", "Appointment At"], result.callback_at);
  assignMondayValue(values, board, ["Callback Local Date", "Appointment Date"], result.callback_local_date);
  assignMondayValue(values, board, ["Callback Local Time", "Appointment Time"], result.callback_local_time);
  assignMondayValue(values, board, ["Callback Timezone", "Appointment Timezone"], result.callback_timezone);
  assignMondayValue(values, board, ["Callback Timezone Label"], result.callback_timezone_label);
  assignMondayValue(values, board, ["Callback Reason", "Appointment Reason"], result.callback_reason);
  assignMondayValue(values, board, ["Callback Type", "Appointment Type"], result.callback_type);
  assignMondayValue(values, board, ["Appointment ID"], result.appointment_id);
  assignMondayValue(
    values,
    board,
    ["Call Summary"],
    buildStructuredMondayCallSummary(
      call,
      recordedSessionPhase || resolveSessionCallPhase(call, latestAttempt)
    )
  );
  assignMondayValue(values, board, ["Owner"], call.human_owner_id);
  assignMondayValueById(
    values,
    board,
    MONDAY_CALL_CONTROL_COLUMNS.has_realtor,
    result.has_realtor
  );
  assignMondayValueById(
    values,
    board,
    MONDAY_CALL_CONTROL_COLUMNS.applied_with_lender,
    result.applied_with_lender
  );
  assignMondayValueById(
    values,
    board,
    MONDAY_CALL_CONTROL_COLUMNS.app_started_confirmation,
    result.app_started_confirmation
  );
  const timeFrameColumn = findMondayColumnById(
    board,
    MONDAY_CALL_CONTROL_COLUMNS.time_frame
  );
  const timeFrameAnswer =
    result.purchase_timeline_detail ||
    call.result?.time_frame ||
    result.time_frame;
  if (timeFrameColumn && timeFrameAnswer) {
    const timeFrameValue = mondayTimeFrameValue(
      timeFrameColumn,
      timeFrameAnswer,
      call.call_id
    );
    if (timeFrameValue) values[timeFrameColumn.id] = timeFrameValue;
  }

  return values;
}

function buildAttemptMondayValues(attempt, metadata) {
  const values = {};
  const board = metadata.subitems;

  assignMondayValue(values, board, ["Attempt Number"], attempt.attempt_number);
  assignMondayValue(values, board, ["Call Leg"], attempt.call_leg);
  assignMondayValue(
    values,
    board,
    ["Duration Seconds"],
    attempt.duration_seconds
  );
  assignMondayValue(
    values,
    board,
    ["Technical Status"],
    technicalStatusLabel(attempt.technical_status)
  );
  assignMondayValue(
    values,
    board,
    ["Answered By"],
    answeredByLabel(attempt.answered_by)
  );
  assignMondayValue(values, board, ["Dialed At"], attempt.dialed_at);
  assignMondayValue(values, board, ["Answered At"], attempt.answered_at);
  assignMondayValue(values, board, ["Twilio SID"], attempt.twilio_call_sid);
  assignMondayValue(values, board, ["Call ID"], attempt.call_id);
  assignMondayValue(values, board, ["Outcome"], attempt.business_outcome);
  assignMondayValue(values, board, ["Attempt Summary"], attempt.summary);
  assignMondayValue(values, board, ["Last Error"], attempt.last_error);

  return values;
}

async function changeMondayValuesResilient(
  boardId,
  itemId,
  columnValues,
  idempotencyPrefix
) {
  const entries = Object.entries(columnValues || {});
  if (!entries.length) return { updated: 0, failed: [] };

  const mutation = `
    mutation HeluxChangeValues(
      $boardId: ID!,
      $itemId: ID!,
      $columnValues: JSON!
    ) {
      change_multiple_column_values(
        board_id: $boardId,
        item_id: $itemId,
        column_values: $columnValues,
        create_labels_if_missing: true
      ) { id }
    }
  `;

  try {
    await mondayRequest(
      mutation,
      {
        boardId: String(boardId),
        itemId: String(itemId),
        columnValues: JSON.stringify(columnValues)
      },
      {
        idempotencyKey: `${idempotencyPrefix}:batch:${JSON.stringify(
          columnValues
        )}`
      }
    );

    return { updated: entries.length, failed: [] };
  } catch (batchError) {
    const failed = [];
    let updated = 0;

    for (const [columnId, value] of entries) {
      try {
        await mondayRequest(
          mutation,
          {
            boardId: String(boardId),
            itemId: String(itemId),
            columnValues: JSON.stringify({ [columnId]: value })
          },
          {
            idempotencyKey: `${idempotencyPrefix}:column:${columnId}:${JSON.stringify(
              value
            )}`
          }
        );
        updated += 1;
      } catch (error) {
        failed.push({
          columnId,
          error: cleanText(error.message, 1000)
        });
      }
    }

    if (!updated && failed.length) {
      const error = new Error(
        `monday.com could not update any columns: ${failed
          .map((entry) => `${entry.columnId}: ${entry.error}`)
          .join(" | ")}`
      );
      error.failedColumns = failed;
      throw error;
    }

    if (failed.length) {
      console.warn(
        `monday.com partial column update for item ${itemId}:`,
        failed
      );
    }

    return { updated, failed };
  }
}

async function ensureMondayMainItem(call, metadata) {
  if (call.monday_item_id) return String(call.monday_item_id);

  const targetGroup = findMondayGroup(metadata.main, targetMondayGroupTitle(call));
  const mutationWithGroup = `
    mutation HeluxCreateCallItem(
      $boardId: ID!,
      $groupId: String!,
      $itemName: String!
    ) {
      create_item(
        board_id: $boardId,
        group_id: $groupId,
        item_name: $itemName
      ) { id }
    }
  `;
  const mutationWithoutGroup = `
    mutation HeluxCreateCallItem($boardId: ID!, $itemName: String!) {
      create_item(board_id: $boardId, item_name: $itemName) { id }
    }
  `;

  const data = targetGroup
    ? await mondayRequest(
        mutationWithGroup,
        {
          boardId: MONDAY_BOARD_ID,
          groupId: targetGroup.id,
          itemName: leadDisplayName(call)
        },
        { idempotencyKey: `create-main:${call.call_id}` }
      )
    : await mondayRequest(
        mutationWithoutGroup,
        {
          boardId: MONDAY_BOARD_ID,
          itemName: leadDisplayName(call)
        },
        { idempotencyKey: `create-main:${call.call_id}` }
      );

  const itemId = data.create_item?.id;
  if (!itemId) throw new Error("monday.com did not return a main item ID.");

  await pool.query(
    `
      UPDATE ai_calls
      SET
        monday_item_id = $2,
        monday_group_id = COALESCE($3, monday_group_id),
        monday_last_error = NULL,
        updated_at = NOW()
      WHERE call_id = $1
    `,
    [call.call_id, String(itemId), targetGroup?.id || null]
  );

  return String(itemId);
}
async function ensureMondayAttemptSubitem(attempt, parentItemId) {
  if (attempt.monday_subitem_id) return String(attempt.monday_subitem_id);

  const mutation = `
    mutation HeluxCreateAttemptSubitem(
      $parentItemId: ID!,
      $itemName: String!
    ) {
      create_subitem(
        parent_item_id: $parentItemId,
        item_name: $itemName
      ) { id }
    }
  `;

  const suffix = Number(attempt.call_leg || 1) > 1
    ? ` — Leg ${attempt.call_leg}`
    : "";
  const data = await mondayRequest(
    mutation,
    {
      parentItemId: String(parentItemId),
      itemName: `Attempt ${attempt.attempt_number}${suffix}`
    },
    { idempotencyKey: `create-subitem:${attempt.attempt_id}` }
  );

  const subitemId = data.create_subitem?.id;
  if (!subitemId) throw new Error("monday.com did not return a subitem ID.");

  await pool.query(
    `
      UPDATE call_attempts
      SET
        monday_subitem_id = $2,
        monday_last_error = NULL,
        updated_at = NOW()
      WHERE attempt_id = $1
    `,
    [attempt.attempt_id, String(subitemId)]
  );

  return String(subitemId);
}

async function moveMondayMainItem(call, itemId, metadata) {
  const targetGroup = findMondayGroup(metadata.main, targetMondayGroupTitle(call));
  if (!targetGroup || String(call.monday_group_id || "") === String(targetGroup.id)) {
    return;
  }

  const mutation = `
    mutation HeluxMoveCallItem($itemId: ID!, $groupId: String!) {
      move_item_to_group(item_id: $itemId, group_id: $groupId) { id }
    }
  `;

  await mondayRequest(
    mutation,
    { itemId: String(itemId), groupId: String(targetGroup.id) },
    {
      idempotencyKey: `move-main:${call.call_id}:${targetGroup.id}:${call.updated_at}`
    }
  );

  await pool.query(
    `
      UPDATE ai_calls
      SET monday_group_id = $2, monday_last_error = NULL, updated_at = NOW()
      WHERE call_id = $1
    `,
    [call.call_id, String(targetGroup.id)]
  );
}

async function syncCallSequenceToMonday(callId, reason = "unspecified") {
  if (!MONDAY_SYNC_ENABLED) return;

  const call = await getCallById(callId);
  if (!call) return;

  try {
    const metadata = await loadMondayMetadata();
    const itemId = await ensureMondayMainItem(call, metadata);
    const attempts = await getAttemptsForCall(callId);
    const latestAttempt = attempts.length ? attempts[attempts.length - 1] : null;
    const refreshedCall = (await getCallById(callId)) || call;

    const mainValues = buildMainMondayValues(
      refreshedCall,
      latestAttempt,
      metadata
    );

    await changeMondayValuesResilient(
      MONDAY_BOARD_ID,
      itemId,
      mainValues,
      `update-main:${callId}:${reason}:${refreshedCall.updated_at}`
    );

    for (const attempt of attempts) {
      try {
        const subitemId = await ensureMondayAttemptSubitem(attempt, itemId);
        const refreshedAttempt =
          (await getAttemptById(attempt.attempt_id)) || attempt;
        const attemptValues = buildAttemptMondayValues(
          refreshedAttempt,
          metadata
        );

        await changeMondayValuesResilient(
          MONDAY_SUBITEM_BOARD_ID,
          subitemId,
          attemptValues,
          `update-subitem:${attempt.attempt_id}:${reason}:${refreshedAttempt.updated_at}`
        );

        await pool.query(
          `
            UPDATE call_attempts
            SET
              monday_last_sync_at = NOW(),
              monday_last_error = NULL,
              updated_at = NOW()
            WHERE attempt_id = $1
          `,
          [attempt.attempt_id]
        );
      } catch (attemptError) {
        await pool.query(
          `
            UPDATE call_attempts
            SET monday_last_error = $2, updated_at = NOW()
            WHERE attempt_id = $1
          `,
          [attempt.attempt_id, cleanText(attemptError.message, 4000)]
        );
        console.error(
          `monday.com attempt sync failed for ${attempt.attempt_id}:`,
          attemptError.message
        );
      }
    }

    const finalCall = (await getCallById(callId)) || refreshedCall;
    await moveMondayMainItem(finalCall, itemId, metadata);

    await pool.query(
      `
        UPDATE ai_calls
        SET
          monday_last_sync_at = NOW(),
          monday_last_error = NULL,
          updated_at = NOW()
        WHERE call_id = $1
      `,
      [callId]
    );
  } catch (error) {
    await pool.query(
      `
        UPDATE ai_calls
        SET monday_last_error = $2, updated_at = NOW()
        WHERE call_id = $1
      `,
      [callId, cleanText(error.message, 4000)]
    );

    console.error(`monday.com sync failed for ${callId}:`, error.message);
  }
}

function queueMondaySync(callId, reason = "state_change") {
  if (!MONDAY_SYNC_ENABLED || !callId) return;

  const existingTimer = mondaySyncTimers.get(callId);
  if (existingTimer) clearTimeout(existingTimer);

  const timer = setTimeout(() => {
    mondaySyncTimers.delete(callId);

    const previous = mondaySyncChains.get(callId) || Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(() => syncCallSequenceToMonday(callId, reason))
      .catch((error) => {
        console.error(`monday.com queued sync failed for ${callId}:`, error);
      })
      .finally(() => {
        if (mondaySyncChains.get(callId) === next) {
          mondaySyncChains.delete(callId);
        }
      });

    mondaySyncChains.set(callId, next);
  }, MONDAY_SYNC_DEBOUNCE_MS);

  mondaySyncTimers.set(callId, timer);
}

function mondayWebhookUrl() {
  const url = new URL(`${PUBLIC_BASE_URL}/api/v1/monday/webhook`);
  url.searchParams.set("secret", MONDAY_WEBHOOK_SECRET);
  return url.toString();
}

function mondayEventStatusLabel(value) {
  if (!value) return null;
  return cleanText(
    value.label?.text ||
      value.label?.label ||
      value.label ||
      value.text ||
      value.name,
    100
  );
}

function mondayEventBoolean(value) {
  if (value === true || value === false) return value;
  const checked = value?.checked ?? value?.check ?? value;
  return normalizeBoolean(checked);
}

async function discoverDpaDepartmentBoard() {
  const cached = await getIntegrationState("dpa_department_board");
  const configuredId = DPA_BOARD_ID || cleanText(cached?.board_id, 100);
  let boards = [];

  if (configuredId) {
    const data = await mondayRequest(
      `query DaisyDpaBoard($ids: [ID!]) {
        boards(ids: $ids) { id name columns { id title type settings } }
      }`,
      { ids: [configuredId] }
    );
    boards = data.boards || [];
  } else {
    const data = await mondayRequest(
      `query DaisyFindDpaBoard {
        boards(limit: 100) { id name columns { id title type settings } }
      }`
    );
    boards = (data.boards || []).filter(
      (board) => normalizeMondayKey(board.name) === "dpadepartment"
    );
  }

  const board = boards.find(
    (candidate) =>
      configuredId || normalizeMondayKey(candidate.name) === "dpadepartment"
  );
  if (!board) {
    if (DPA_BOARD_ID) {
      throw new Error(`Configured DPA board ${DPA_BOARD_ID} was not found.`);
    }
    console.warn('monday.com board named "DPA Department" was not found.');
    return null;
  }
  if (!findMondayColumnById(buildMondayBoardMetadata(board), DPA_DEPARTMENT_COLUMNS.app_started)) {
    throw new Error(
      `DPA Department board ${board.id} is missing App_started column ${DPA_DEPARTMENT_COLUMNS.app_started}.`
    );
  }
  await setIntegrationState("dpa_department_board", {
    board_id: String(board.id),
    board_name: board.name,
    discovered_at: new Date().toISOString()
  });
  return buildMondayBoardMetadata(board);
}

function mondayRawColumnValue(columnValue) {
  if (!columnValue) return null;
  if (columnValue.text) return cleanText(columnValue.text, 2000);
  try {
    const parsed = typeof columnValue.value === "string"
      ? JSON.parse(columnValue.value)
      : columnValue.value;
    return cleanText(
      parsed?.phone || parsed?.text || parsed?.label?.text || parsed?.label,
      2000
    );
  } catch {
    return cleanText(columnValue.value, 2000);
  }
}

async function createMondayItemUpdate(itemId, body) {
  await mondayRequest(
    `mutation DaisyDpaUpdate($itemId: ID!, $body: String!) {
      create_update(item_id: $itemId, body: $body) { id }
    }`,
    { itemId: String(itemId), body: cleanText(body, 5000) },
    { idempotencyKey: `dpa-update:${itemId}:${body}` }
  );
}

async function claimIntegrationEvent(stateKey, value) {
  const result = await pool.query(
    `
      INSERT INTO integration_state (state_key, state_value, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (state_key) DO NOTHING
      RETURNING state_key
    `,
    [stateKey, JSON.stringify(value || {})]
  );
  return result.rowCount === 1;
}

async function createDpaAgentNotification(itemId, statusLabel) {
  const data = await mondayRequest(
    `query DaisyDpaItem($ids: [ID!]) {
      items(ids: $ids) {
        id name
        column_values {
          id text value
          column { title }
        }
      }
    }`,
    { ids: [String(itemId)] }
  );
  const item = data.items?.[0];
  if (!item) throw new Error(`DPA Department item ${itemId} was not found.`);
  const byId = new Map((item.column_values || []).map((column) => [column.id, column]));
  const agentName = mondayRawColumnValue(byId.get(DPA_DEPARTMENT_COLUMNS.realtor_name));
  const agentPhone = normalizePhone(
    mondayRawColumnValue(byId.get(DPA_DEPARTMENT_COLUMNS.realtor_phone))
  );
  const byTitle = (aliases) => {
    const wanted = aliases.map(normalizeMondayKey);
    return (item.column_values || []).find((column) =>
      wanted.some((alias) => normalizeMondayKey(column.column?.title).includes(alias))
    );
  };
  const timeFrame = normalizeTimeFrame(
    mondayRawColumnValue(byTitle(["Time frame", "Purchase timeframe", "Timeline"]))
  );
  if (!agentPhone || !validE164Phone(agentPhone)) {
    await createMondayItemUpdate(
      item.id,
      `Daisy could not create the application-started notification call because the assigned DPA Realtor phone is missing or invalid. Please correct ${DPA_DEPARTMENT_COLUMNS.realtor_phone} and notify the specialist manually.`
    );
    return { created: false, reason: "invalid_agent_phone" };
  }

  const eventKey = `dpa-agent-notification:${item.id}:${normalizeMondayKey(statusLabel)}`;
  const claimed = await claimIntegrationEvent(eventKey, {
    status: "claimed",
    item_id: String(item.id),
    app_started_status: statusLabel,
    claimed_at: new Date().toISOString()
  });
  if (!claimed) return { created: false, reason: "duplicate" };

  const callId = createPublicId("DPA-NOTIFY");
  const streamToken = createStreamToken();
  const payload = {
    call_type: "dpa_agent_notification",
    first_name: agentName,
    agent_name: agentName,
    customer_name: cleanText(item.name, 160) || "the customer",
    dpa_item_id: String(item.id),
    time_frame: timeFrame,
    app_started_status: statusLabel
  };
  const inserted = await pool.query(
    `
      INSERT INTO ai_calls (
        call_id, request_key, phone, status, stream_token, payload,
        timezone, consent_status, current_state, next_state,
        agent_version, prompt_version, tool_version, knowledge_version,
        routing_version, priority
      ) VALUES (
        $1, $2, $3, 'created', $4, $5::jsonb, $6, 'confirmed',
        'dpa_agent_notification', 'identity_confirmation',
        $7, $8, $9, $10, $11, 'urgent'
      ) RETURNING *
    `,
    [
      callId,
      eventKey,
      agentPhone,
      streamToken,
      JSON.stringify(payload),
      DEFAULT_TIMEZONE,
      DOUG_CONFIG.agentVersion,
      DOUG_CONFIG.promptVersion,
      DOUG_CONFIG.toolVersion,
      DOUG_CONFIG.knowledgeVersion,
      DOUG_CONFIG.routingVersion
    ]
  );
  const notificationCall = inserted.rows[0];
  const notificationAttempt = await createCallAttempt(notificationCall.call_id, {
    attemptType: "specialist_notification",
    idempotencyKey: `specialist_notification:${notificationCall.call_id}:1`
  });
  logOutboundCallRejected(
    notificationCall,
    "specialist_notification",
    "specialist_notification_phone_calls_disabled"
  );
  await pool.query(
    `UPDATE call_attempts
     SET technical_status = 'canceled', completed_at = NOW(),
         cancellation_reason = 'specialist_notification_phone_calls_disabled',
         updated_at = NOW()
     WHERE attempt_id = $1`,
    [notificationAttempt.attempt_id]
  );
  await pool.query(
    `UPDATE ai_calls
     SET sequence_status = 'human_action',
         result = result || $2::jsonb, updated_at = NOW()
     WHERE call_id = $1`,
    [
      notificationCall.call_id,
      JSON.stringify({
        specialist_notification_phone_calls_disabled: true
      })
    ]
  );
  await setIntegrationState(eventKey, {
    status: "notification_created",
    item_id: String(item.id),
    call_id: notificationCall.call_id,
    app_started_status: statusLabel,
    created_at: new Date().toISOString()
  });
  return { created: true, call_id: notificationCall.call_id };
}

async function processDpaDepartmentEvent(event) {
  if (!event) return false;
  const state = await getIntegrationState("dpa_department_board");
  const boardId = DPA_BOARD_ID || state?.board_id;
  if (!boardId || String(event.boardId) !== String(boardId)) return false;
  if (String(event.columnId) !== DPA_DEPARTMENT_COLUMNS.app_started) return true;
  const status = mondayEventStatusLabel(event.value);
  if (!["yes", "started", "confirmed", "complete", "completed"].includes(
    normalizeMondayKey(status)
  )) return true;
  const itemId = event.pulseId || event.itemId;
  if (itemId) await createDpaAgentNotification(itemId, status);
  return true;
}

async function ensureMondayWebhook(eventName, stateKey, boardId = MONDAY_BOARD_ID) {
  const currentState = await getIntegrationState(stateKey);
  const currentId = currentState?.webhook_id
    ? String(currentState.webhook_id)
    : null;

  if (currentId) {
    const data = await mondayRequest(
      `query HeluxWebhooks($boardId: ID!) {
        webhooks(board_id: $boardId) { id event board_id config }
      }`,
      { boardId },
      { maxRetries: 1 }
    );

    const exists = (data.webhooks || []).some(
      (webhook) =>
        String(webhook.id) === currentId && webhook.event === eventName
    );

    if (exists) return currentId;
  }

  const mutation = `
    mutation HeluxCreateWebhook($boardId: ID!, $url: String!) {
      create_webhook(
        board_id: $boardId,
        url: $url,
        event: ${eventName}
      ) { id board_id }
    }
  `;

  const data = await mondayRequest(
    mutation,
    { boardId, url: mondayWebhookUrl() },
    { idempotencyKey: `create-webhook:${eventName}:${boardId}` }
  );

  const webhookId = data.create_webhook?.id;
  if (!webhookId) {
    throw new Error(`monday.com did not return a ${eventName} webhook ID.`);
  }

  await setIntegrationState(stateKey, {
    webhook_id: String(webhookId),
    event: eventName,
    board_id: boardId,
    url: mondayWebhookUrl(),
    created_at: new Date().toISOString()
  });

  return String(webhookId);
}

async function ensureMondayInboundWebhooks() {
  if (!MONDAY_SYNC_ENABLED || !MONDAY_INBOUND_SYNC_ENABLED) return [];

  const results = [];
  results.push(
    await ensureMondayWebhook(
      "change_column_value",
      "monday_webhook_change_column_value"
    )
  );
  results.push(
    await ensureMondayWebhook(
      "item_moved_to_any_group",
      "monday_webhook_item_moved_to_any_group"
    )
  );
  const dpaBoard = await discoverDpaDepartmentBoard();
  if (dpaBoard) {
    results.push(
      await ensureMondayWebhook(
        "change_column_value",
        "monday_webhook_dpa_app_started",
        dpaBoard.id
      )
    );
  }
  return results;
}

async function applyMondayGroupControl(call, event) {
  const groupName = cleanText(event.groupName || event.groupTitle, 150);
  if (!groupName) return false;

  const normalized = normalizeMondayKey(groupName);
  const expectedGroups = targetMondayGroupTitle(call).map(normalizeMondayKey);

  // Ignore the webhook generated by HELUX moving the item to its expected group.
  if (expectedGroups.includes(normalized)) return false;

  if (["agentneeded", "humanactionneeded"].includes(normalized)) {
    await pool.query(
      `
        UPDATE ai_calls
        SET sequence_status = 'human_action',
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [call.call_id]
    );
    return true;
  }

  if (normalized === "completed") {
    if (await sequenceHasUnresolvedWork(call.call_id)) return false;
    await pool.query(
      `
        UPDATE ai_calls
        SET sequence_status = 'completed',
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [call.call_id]
    );
    return true;
  }

  if (["donotcalllist", "closedorsuppressed"].includes(normalized)) {
    await pool.query(
      `
        UPDATE ai_calls
        SET
          do_not_call = TRUE,
          sequence_status = 'suppressed',
          outcome = COALESCE(outcome, 'opt_out'),
          updated_at = NOW()
        WHERE call_id = $1
      `,
      [call.call_id]
    );
    return true;
  }

  return false;
}

async function applyMondayColumnControl(call, event) {
  const title = normalizeMondayKey(event.columnTitle || event.columnId);
  const value = event.value;
  const columnId = String(event.columnId || "");

  const structuredKey = Object.entries(MONDAY_CALL_CONTROL_COLUMNS).find(
    ([, configuredId]) => configuredId === columnId
  )?.[0];
  if (structuredKey) {
    let structuredValue = mondayEventStatusLabel(value);
    if (structuredKey === "time_frame") {
      structuredValue = normalizeTimeFrame(structuredValue);
    }
    if (!structuredValue) return false;
    const patch = normalizeDaisyAnswers({ [structuredKey]: structuredValue });
    const existing = normalizeDaisyAnswers(call.result || {});
    if (JSON.stringify(patch) === JSON.stringify(
      Object.fromEntries(Object.keys(patch).map((key) => [key, existing[key]]))
    )) return false;
    await mergeCallResult(call.call_id, patch);
    return true;
  }

  if (title === "sequencestatus") {
    const label = normalizeMondayKey(mondayEventStatusLabel(value));
    const expectedLabel = normalizeMondayKey(sequenceStatusLabel(call));

    // Ignore status changes written by HELUX itself.
    if (label && label === expectedLabel) return false;

    if (label === "paused") {
      await pool.query(
        `UPDATE ai_calls SET sequence_status = 'paused',
         updated_at = NOW() WHERE call_id = $1`,
        [call.call_id]
      );
      return true;
    }

    if (["humanaction", "agentneeded", "exhausted"].includes(label)) {
      await pool.query(
        `UPDATE ai_calls SET sequence_status = 'human_action',
         updated_at = NOW() WHERE call_id = $1`,
        [call.call_id]
      );
      return true;
    }

    if (label === "completed") {
      if (await sequenceHasUnresolvedWork(call.call_id)) return false;
      await pool.query(
        `UPDATE ai_calls SET sequence_status = 'completed',
         updated_at = NOW() WHERE call_id = $1`,
        [call.call_id]
      );
      return true;
    }

    if (["donotcall", "wrongnumber", "invalidnumber"].includes(label)) {
      await pool.query(
        `
          UPDATE ai_calls
          SET
            do_not_call = do_not_call OR $2,
            wrong_number = wrong_number OR $3,
            invalid_number = invalid_number OR $4,
            sequence_status = 'suppressed',
            updated_at = NOW()
          WHERE call_id = $1
        `,
        [
          call.call_id,
          label === "donotcall",
          label === "wrongnumber",
          label === "invalidnumber"
        ]
      );
      return true;
    }
  }

  if (title === "donotcall") {
    const checked = mondayEventBoolean(value);
    if (checked !== true || call.do_not_call) return false;

    await pool.query(
      `
        UPDATE ai_calls
        SET do_not_call = TRUE, sequence_status = 'suppressed',
            outcome = COALESCE(outcome, 'opt_out'),
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [call.call_id]
    );
    return true;
  }

  if (title === "priority") {
    const label = cleanText(mondayEventStatusLabel(value), 30);
    if (!label || label.toLowerCase() === String(call.priority || "").toLowerCase()) {
      return false;
    }
    await pool.query(
      `UPDATE ai_calls SET priority = $2, updated_at = NOW() WHERE call_id = $1`,
      [call.call_id, label.toLowerCase()]
    );
    return true;
  }

  if (title === "owner") {
    const ownerId = value?.personsAndTeams?.[0]?.id || value?.persons?.[0]?.id;
    if (!ownerId || String(ownerId) === String(call.human_owner_id || "")) {
      return false;
    }
    await pool.query(
      `UPDATE ai_calls SET human_owner_id = $2, updated_at = NOW()
       WHERE call_id = $1`,
      [call.call_id, String(ownerId)]
    );
    return true;
  }

  return false;
}
async function processMondayInboundEvent(event) {
  if (await processDpaDepartmentEvent(event)) return;
  if (!event || String(event.boardId) !== String(MONDAY_BOARD_ID)) return;

  const itemId = event.pulseId || event.itemId;
  if (!itemId) return;

  const call = await getCallByMondayItemId(itemId);
  if (!call) return;

  const changed = event.columnId
    ? await applyMondayColumnControl(call, event)
    : await applyMondayGroupControl(call, event);

  if (!changed) return;

  await appendAction(call.call_id, {
    action: "monday_manual_control",
    success: true,
    column_title: cleanText(event.columnTitle, 150),
    group_name: cleanText(event.groupName, 150),
    changed_at: event.changedAt || event.triggerTime || null,
    monday_user_id: event.userId || null
  });

  queueMondaySync(call.call_id, "monday_manual_control");
}

async function updateCallStatus(callId, status, extra = {}) {
  const statusValue = cleanText(status, 50) || "unknown";
  const lastError = cleanText(extra.last_error, 4000);
  const twilioCallSid = cleanText(extra.twilio_call_sid, 80);

  await pool.query(
    `
      UPDATE ai_calls
      SET
        status = $2::VARCHAR(50),
        twilio_call_sid = COALESCE($3::VARCHAR(80), twilio_call_sid),
        last_error = CASE
          WHEN $4::TEXT IS NOT NULL THEN $4::TEXT
          WHEN $2::VARCHAR(50) IN (
            'queued', 'initiated', 'ringing', 'answered',
            'in-progress', 'completed'
          ) THEN NULL
          ELSE last_error
        END,
        started_at = CASE
          WHEN $2::VARCHAR(50) IN (
            'queued', 'initiated', 'ringing', 'answered', 'in-progress'
          ) THEN COALESCE(started_at, NOW())
          ELSE started_at
        END,
        answered_at = CASE
          WHEN $2::VARCHAR(50) IN ('answered', 'in-progress')
            THEN COALESCE(answered_at, NOW())
          ELSE answered_at
        END,
        completed_at = CASE
          WHEN $2::VARCHAR(50) IN (
            'completed', 'busy', 'failed', 'no-answer', 'canceled'
          ) THEN COALESCE(completed_at, NOW())
          ELSE completed_at
        END,
        updated_at = NOW()
      WHERE call_id = $1::VARCHAR(100)
    `,
    [callId, statusValue, twilioCallSid, lastError]
  );

  const call = await getCallById(callId);
  if (call && call.last_attempt_id) {
    await pool.query(
      `
        UPDATE call_attempts
        SET
          technical_status = $2::VARCHAR(50),
          twilio_call_sid = COALESCE($3::VARCHAR(80), twilio_call_sid),
          last_error = CASE
            WHEN $4::TEXT IS NOT NULL THEN $4::TEXT
            WHEN $2::VARCHAR(50) IN (
              'queued', 'initiated', 'ringing', 'answered',
              'in-progress', 'completed'
            ) THEN NULL
            ELSE last_error
          END,
          answered_at = CASE
            WHEN $2::VARCHAR(50) IN ('answered', 'in-progress')
              THEN COALESCE(answered_at, NOW())
            ELSE answered_at
          END,
          completed_at = CASE
            WHEN $2::VARCHAR(50) IN (
              'completed', 'busy', 'failed', 'no-answer', 'canceled'
            ) THEN COALESCE(completed_at, NOW())
            ELSE completed_at
          END,
          updated_at = NOW()
        WHERE attempt_id = $1
      `,
      [call.last_attempt_id, statusValue, twilioCallSid, lastError]
    );
  }

  queueMondaySync(callId, `call_status_${statusValue}`);
}

function publicCallResult(result = {}) {
  const fields = [
    "inbound_intent",
    "first_name",
    "last_name",
    "business_name",
    "business_entity_type",
    "entity_status",
    "time_in_business",
    "industry",
    "city",
    "state",
    "zip",
    "lead_id",
    "phone_number",
    "email",
    "funding_use",
    "funding_amount",
    "funding_timeline",
    "estimated_credit_score",
    "credit_status",
    "gross_monthly_revenue",
    "income_status",
    "monthly_business_expenses",
    "existing_business_financing",
    "taxes",
    "tax_status",
    "tax_years_available",
    "bank_statements_available",
    "financial_statements_available",
    "preliminary_readiness",
    "missing_items",
    "next_action",
    "follow_up_date",
    "follow_up_time",
    "follow_up_timezone",
    "follow_up_reason",
    "follow_up_declined",
    "follow_up_at",
    "call_summary",
    "call_outcome",
    "recording_url",
    "transcript_url",
    "call_started_at",
    "call_ended_at",
    "sba_lead_lookup",
    "sba_lead_match_status",
    "existing_profile_loaded",
    "preliminary_dti_classification",
    "application_link_sent",
    "final_outcome",
    "next_action",
    "summary",
    "normal_completion_recorded",
    "final_hangup_requested",
    "final_hangup_completed",
    "completion_reason",
    "contact_restriction",
    "customer_timezone", "customer_timezone_label", "customer_timezone_confirmed",
    "customer_timezone_confirmed_at", "callback_at", "callback_local_date",
    "callback_local_time", "callback_timezone", "callback_timezone_label",
    "callback_reason", "callback_type", "callback_confirmed", "callback_created_at",
    "callback_source_call_id", "appointment_id"
  ];
  return Object.fromEntries(
    fields
      .filter((field) => result[field] !== undefined)
      .map((field) => [field, result[field]])
  );
}

function publicCallActions(actions = []) {
  const fields = [
    "action",
    "success",
    "outcome",
    "restriction_type",
    "resource_type",
    "transfer_status",
    "priority",
    "reason",
    "technical_failure",
    "attempt_number",
    "twilio_call_sid",
    "session_call_phase",
    "created_at"
  ];
  return (Array.isArray(actions) ? actions : []).map((action) =>
    Object.fromEntries(
      fields
        .filter((field) => action?.[field] !== undefined)
        .map((field) => [field, action[field]])
    )
  );
}

async function notifyHelux(call) {
  if (!call) return;

  try {
    const attempts = await getAttemptsForCall(call.call_id);
    const response = await fetch(`${HELUX_BASE_URL}${HELUX_RESULTS_PATH}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-helux-key": HELUX_API_KEY
      },
      body: JSON.stringify({
        case_id: call.case_id,
        lead_id: call.lead_id,
        call_id: call.call_id,
        twilio_call_sid: call.twilio_call_sid,
        status: call.status,
        sequence_status: call.sequence_status,
        attempts_used: call.attempts,
        outcome: call.outcome,
        sentiment: call.sentiment,
        awaiting_customer_response: call.awaiting_customer_response,
        pending_question_type: call.pending_question_type,
        pending_question_text: call.pending_question_text,
        question_asked_at: call.question_asked_at,
        response_reminder_count: call.response_reminder_count,
        next_action: call.next_action,
        summary: call.summary,
        transcript: call.transcript || [],
        actions: publicCallActions(call.actions),
        result: publicCallResult(call.result),
        monday: {
          enabled: MONDAY_SYNC_ENABLED,
          board_id: MONDAY_BOARD_ID,
          item_id: call.monday_item_id,
          group_id: call.monday_group_id,
          last_sync_at: call.monday_last_sync_at,
          last_error: call.monday_last_error,
          attempt_subitems: attempts.map((attempt) => ({
            attempt_id: attempt.attempt_id,
            monday_subitem_id: attempt.monday_subitem_id,
            last_sync_at: attempt.monday_last_sync_at,
            last_error: attempt.monday_last_error
          }))
        },
        versions: {
          agent: call.agent_version,
          prompt: call.prompt_version,
          tools: call.tool_version,
          knowledge: call.knowledge_version,
          routing: call.routing_version,
          monday_adapter: DOUG_CONFIG.mondayAdapterVersion,
          realtime_model: OPENAI_REALTIME_MODEL,
          voice: OPENAI_VOICE
        }
      })
    });

    if (!response.ok) {
      const body = await response.text();
      console.error(
        `HELUX result callback failed with ${response.status}: ${body.slice(
          0,
          500
        )}`
      );
    }
  } catch (error) {
    console.error("HELUX result callback failed:", error.message);
  }
}

async function reconnectAfterUnexpectedDisconnect(callId) {
  const call = await getCallById(callId);
  if (!call || call.payload?.call_type === "dpa_agent_notification") return false;
  if (call.direction === "inbound" || call.payload?.direction === "inbound") {
    inboundLog("[INBOUND]", "reconnect_skipped", {
      call_id: call.call_id,
      reason: "inbound_calls_are_not_redialed"
    });
    return false;
  }
  if (!call.last_attempt_id) return false;

  const attempt = await getAttemptById(call.last_attempt_id);
  const transcript = Array.isArray(attempt?.transcript) ? attempt.transcript : [];
  const actions = Array.isArray(attempt?.actions) ? attempt.actions : [];
  const completedByTool = actions.some(
    (action) => action?.action === "complete_call" && action?.success === true
  );
  const intentionallyEnded = actions.some(
    (action) =>
      [
        "twilio_call_hangup",
        "twilio_final_hangup",
        "twilio_physical_hangup"
      ].includes(action?.action) &&
      action?.success === true
  );
  const normalCompletion = Boolean(
    completedByTool ||
      intentionallyEnded ||
      call.result?.normal_completion_recorded === true ||
      call.result?.final_hangup_requested === true ||
      call.result?.final_hangup_completed === true ||
      call.result?.completion_reason === "normal_completion" ||
      String(call.status || "").toLowerCase() === "completed"
  );
  if (normalCompletion || transcript.length < 2) {
    console.log(JSON.stringify({
      event: "unexpected_reconnect_skipped",
      call_id: call.call_id,
      reason: normalCompletion ? "normal_terminal_call" : "insufficient_connected_conversation"
    }));
    return false;
  }

  const sourceTwilioCallSid = call.twilio_call_sid;
  const savedSummary = cleanText(
    call.summary ||
      transcript
        .slice(-6)
        .map((entry) => `${entry.speaker}: ${entry.text}`)
        .join(" | "),
    4000
  );
  const updated = await pool.query(
    `
      UPDATE ai_calls
      SET current_state = 'reconnect_pending',
          next_state = COALESCE(next_state, 'resume_conversation'),
          sequence_status = 'ready',
          status = 'disconnected',
          twilio_call_sid = NULL,
          summary = COALESCE(summary, $2),
          next_action = COALESCE(next_action, 'Resume after unexpected disconnect'),
          completed_at = NULL,
          result = result || $3::jsonb,
          updated_at = NOW()
      WHERE call_id = $1
        AND COALESCE(result->>'unexpected_disconnect_reconnect_attempted', 'false') <> 'true'
        AND COALESCE(result->>'normal_completion_recorded', 'false') <> 'true'
        AND COALESCE(result->>'final_hangup_requested', 'false') <> 'true'
        AND COALESCE(result->>'final_hangup_completed', 'false') <> 'true'
        AND COALESCE(result->>'completion_reason', '') <> 'normal_completion'
        AND status IN ('answered', 'in-progress')
        AND twilio_call_sid = $4
        AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(actions) AS action
          WHERE action->>'action' = 'complete_call'
            AND COALESCE((action->>'success')::BOOLEAN, FALSE) = TRUE
        )
      RETURNING *
    `,
    [
      callId,
      savedSummary,
      JSON.stringify({
        unexpected_disconnect_reconnect_attempted: true,
        unexpected_disconnect_at: new Date().toISOString(),
        reconnect_source_call_id: callId,
        reconnect_source_twilio_call_sid: sourceTwilioCallSid,
        outbound_call_reason: "unexpected_disconnect_reconnect"
      }),
      sourceTwilioCallSid
    ]
  );
  if (!updated.rowCount) return false;

  const reconnectCall = updated.rows[0];
  const reconnectAttempt = await createCallAttempt(callId, {
    attemptType: "disconnect_reconnect",
    idempotencyKey: `disconnect_reconnect:${callId}`
  });
  if (!reconnectAttempt) return false;

  await appendAction(callId, {
    action: "unexpected_disconnect_reconnect",
    success: true,
    reconnect_source_call_id: callId,
    reconnect_source_twilio_call_sid: sourceTwilioCallSid,
    outbound_call_reason: "unexpected_disconnect_reconnect"
  });
  await placeTwilioCall(reconnectCall, {
    attemptId: reconnectAttempt.attempt_id,
    source: "unexpected_reconnect",
    callReason: "unexpected_disconnect_reconnect"
  });
  return true;
}
async function finalizeAfterTerminal(callId, technicalStatus) {
  const call = await getCallById(callId);
  if (!call) return;

  const transcriptCount = Array.isArray(call.transcript)
    ? call.transcript.length
    : 0;
  const outcome = String(call.outcome || "").toLowerCase();

  if (
    call.result?.unexpected_disconnect_reconnect_attempted &&
    !call.result?.unexpected_disconnect_reconnect_completed
  ) {
    await pool.query(
      `
        UPDATE ai_calls
        SET sequence_status = 'human_action',
            current_state = 'reconnect_attempt_completed',
            result = result || $2::jsonb,
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [
        callId,
        JSON.stringify({ unexpected_disconnect_reconnect_completed: true })
      ]
    );
    queueMondaySync(callId, "reconnect_attempt_completed");
    return;
  }

  if (call.do_not_call || call.wrong_number || call.invalid_number) {
    await pool.query(
      `UPDATE ai_calls SET sequence_status = 'suppressed', updated_at = NOW()
       WHERE call_id = $1`,
      [callId]
    );
    queueMondaySync(callId, "contact_suppressed");
    return;
  }

  if (stopOutcome(outcome)) {
    await pool.query(
      `UPDATE ai_calls SET sequence_status = 'completed', updated_at = NOW()
       WHERE call_id = $1`,
      [callId]
    );
    queueMondaySync(callId, "call_completed");
    return;
  }

  if (
    String(technicalStatus).toLowerCase() === "completed" &&
    transcriptCount > 1
  ) {
    await pool.query(
      `
        UPDATE ai_calls
        SET sequence_status = 'human_action',
            next_action = COALESCE(next_action, 'Review connected call'),
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [callId]
    );
    queueMondaySync(callId, "connected_call_review");
    return;
  }

  await pool.query(
    `
      UPDATE ai_calls
      SET sequence_status = 'human_action',
          next_action = COALESCE(next_action, 'Manual review required'),
          updated_at = NOW()
      WHERE call_id = $1
    `,
    [callId]
  );
  queueMondaySync(callId, "manual_review_required");
}
async function placeTwilioCall(call, options = {}) {
  let refreshedCall = await getCallById(call.call_id);
  if (!refreshedCall) throw new Error("Call sequence not found.");

  const resolvedCallReason = resolveOutboundCallReason(
    refreshedCall,
    null,
    options
  );
  const source = outboundCallSource(null, options.source);

  if (!OUTBOUND_CALLS_ENABLED) {
    blockDisabledOutboundCall(refreshedCall, source);
    logOutboundCallRejected(
      refreshedCall,
      resolvedCallReason,
      "outbound_calls_disabled"
    );
    throw new HttpError(409, "Outbound calls are disabled.");
  }
  if (internalNotificationCallReason(resolvedCallReason)) {
    logOutboundCallRejected(
      refreshedCall,
      resolvedCallReason,
      "specialist_notification_phone_calls_disabled"
    );
    throw new HttpError(409, "Specialist notification phone calls are disabled.");
  }
  if (!permittedOutboundCallReason(resolvedCallReason)) {
    logOutboundCallRejected(
      refreshedCall,
      resolvedCallReason,
      "missing_permitted_call_reason"
    );
    throw new HttpError(409, "A permitted outbound call reason is required.");
  }
  if (
    refreshedCall.do_not_call ||
    refreshedCall.wrong_number ||
    refreshedCall.invalid_number
  ) {
    throw new HttpError(409, "This contact is suppressed from calls.");
  }
  if (
    ENFORCE_CALL_CONSENT &&
    refreshedCall.consent_status !== "confirmed" &&
    options.force !== true
  ) {
    throw new HttpError(409, "Confirmed AI voice consent is required.");
  }

  let attempt = options.attemptId
    ? await getAttemptById(options.attemptId)
    : await createCallAttempt(refreshedCall.call_id, {
        attemptType: options.attemptType
      });
  if (!attempt || !pendingAttemptStatus(attempt.technical_status)) {
    throw new HttpError(409, "No call attempt is available.");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (resolvedCallReason === "initial_lead_call") {
      const leadIdentity = outboundLeadId(refreshedCall);
      if (!leadIdentity) {
        throw new HttpError(409, "Initial call lead identity is unavailable.");
      }
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext($1))",
        [`initial_lead_call:${leadIdentity}`]
      );
      const priorInitialCall = await client.query(
        `
          SELECT call_id FROM ai_calls
          WHERE call_id <> $1
            AND COALESCE(NULLIF(lead_id, ''), NULLIF(payload->>'lead_id', ''),
                         NULLIF(case_id, ''), request_key) = $2
            AND COALESCE(
              result->>'outbound_call_reason',
              payload->>'outbound_call_reason',
              ''
            ) = 'initial_lead_call'
            AND (
              attempts > 0 OR last_attempt_id IS NOT NULL
              OR twilio_call_sid IS NOT NULL
              OR result->>'initial_call_claimed_at' IS NOT NULL
            )
          LIMIT 1
        `,
        [refreshedCall.call_id, leadIdentity]
      );
      if (priorInitialCall.rowCount) {
        throw new HttpError(409, "An initial call already exists for this lead.");
      }
    }

    const claimedAt = new Date().toISOString();
    const claimPatch = {
      outbound_call_reason: resolvedCallReason,
      outbound_call_claimed_at: claimedAt,
      ...(resolvedCallReason === "initial_lead_call"
        ? { initial_call_claimed_at: claimedAt }
        : { unexpected_disconnect_reconnect_attempted: true })
    };
    const claimedCall = await client.query(
      `
        UPDATE ai_calls
        SET status = 'placing',
            sequence_status = 'calling',
            current_state = CASE
              WHEN $4 = 'unexpected_disconnect_reconnect'
                THEN 'reconnect_in_progress'
              ELSE current_state
            END,
            stream_token = $3,
            twilio_call_sid = NULL,
            attempts = attempts + 1,
            last_attempt_id = $2,
            last_attempt_at = NOW(),
            last_error = NULL,
            completed_at = NULL,
            result = result || $5::jsonb,
            updated_at = NOW()
        WHERE call_id = $1
          AND do_not_call = FALSE
          AND wrong_number = FALSE
          AND invalid_number = FALSE
          AND status NOT IN ('placing', 'queued', 'initiated', 'ringing', 'answered', 'in-progress')
          AND ($4 <> 'initial_lead_call' OR (
            attempts = 0
            AND last_attempt_id IS NULL
            AND result->>'initial_call_claimed_at' IS NULL
          ))
        RETURNING *
      `,
      [
        refreshedCall.call_id,
        attempt.attempt_id,
        createStreamToken(),
        resolvedCallReason,
        JSON.stringify(claimPatch)
      ]
    );
    if (!claimedCall.rowCount) {
      throw new HttpError(409, "Call is no longer eligible to be placed.");
    }

    const claimedAttempt = await client.query(
      `
        UPDATE call_attempts
        SET technical_status = 'placing', dialed_at = NOW(), updated_at = NOW()
        WHERE attempt_id = $1
          AND call_id = $2
          AND technical_status IN ('pending', 'created')
          AND completed_at IS NULL
        RETURNING *
      `,
      [attempt.attempt_id, refreshedCall.call_id]
    );
    if (!claimedAttempt.rowCount) {
      throw new HttpError(409, "Call attempt is no longer available.");
    }

    refreshedCall = claimedCall.rows[0];
    attempt = claimedAttempt.rows[0];
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  const voiceUrl = new URL(`${PUBLIC_BASE_URL}/api/v1/twilio/voice`);
  voiceUrl.searchParams.set("call_id", refreshedCall.call_id);
  voiceUrl.searchParams.set("token", refreshedCall.stream_token);
  const statusUrl = new URL(`${PUBLIC_BASE_URL}/api/v1/twilio/status`);
  statusUrl.searchParams.set("call_id", refreshedCall.call_id);
  statusUrl.searchParams.set("token", refreshedCall.stream_token);

  await appendAction(refreshedCall.call_id, {
    action: "outbound_call_claimed",
    success: true,
    call_reason: resolvedCallReason,
    attempt_id: attempt.attempt_id
  });
  logOutboundCallFinalEligibility(refreshedCall, resolvedCallReason);

  try {
    const twilioCall = await twilioClient.calls.create({
      to: refreshedCall.phone,
      from: TWILIO_FROM_NUMBER,
      url: voiceUrl.toString(),
      method: "POST",
      statusCallback: statusUrl.toString(),
      statusCallbackMethod: "POST",
      statusCallbackEvent: ["initiated", "ringing", "answered", "completed"]
    });

    await updateCallStatus(
      refreshedCall.call_id,
      twilioCall.status || "queued",
      { twilio_call_sid: twilioCall.sid }
    );
    await mergeCallResult(refreshedCall.call_id, {
      outbound_call_placed_at: new Date().toISOString(),
      outbound_call_reason: resolvedCallReason
    });
    await appendAction(refreshedCall.call_id, {
      action: "outbound_call_placed",
      success: true,
      call_reason: resolvedCallReason,
      attempt_number: Number(attempt.attempt_number),
      twilio_call_sid: twilioCall.sid
    });
    queueMondaySync(refreshedCall.call_id, "twilio_call_placed");
    return twilioCall;
  } catch (error) {
    const safeError =
      cleanText(error.message, 4000) || "Twilio call creation failed.";
    await pool.query(
      `
        UPDATE call_attempts
        SET technical_status = 'failed', completed_at = NOW(),
            last_error = $2, updated_at = NOW()
        WHERE attempt_id = $1
      `,
      [attempt.attempt_id, safeError]
    );
    await pool.query(
      `
        UPDATE ai_calls
        SET status = 'failed', sequence_status = 'human_action',
            last_error = $2, updated_at = NOW()
        WHERE call_id = $1
      `,
      [refreshedCall.call_id, safeError]
    );
    await appendAction(refreshedCall.call_id, {
      action: "outbound_call_placed",
      success: false,
      call_reason: resolvedCallReason,
      technical_failure: true,
      customer_attempt_consumed: false,
      attempt_number: Number(attempt.attempt_number),
      error: safeError
    });
    queueMondaySync(refreshedCall.call_id, "twilio_call_failed");
    throw error;
  }
}
function smsStatusCallbackUrl(call) {
  const url = new URL(`${PUBLIC_BASE_URL}/api/v1/twilio/sms-status`);
  url.searchParams.set("call_id", call.call_id);
  url.searchParams.set("token", call.stream_token);
  return url.toString();
}

async function trackSmsMessage(callId, message, messageType) {
  await pool.query(
    `
      INSERT INTO sms_deliveries (
        message_sid, call_id, message_type, status, updated_at
      )
      VALUES ($1, $2, $3, $4, NOW())
      ON CONFLICT (message_sid)
      DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
    `,
    [
      message.sid,
      callId,
      messageType,
      cleanText(message.status, 50) || "accepted"
    ]
  );

}

async function lookupExistingOutboundApplicant({ phone, email, name }) {
  const normalizedPhone = normalizePhone(phone);
  if (!OUTBOUND_LOOKUP_URL) {
    return {
      success: true,
      found: false,
      status: "not_configured",
      message: "The outbound applicant lookup is not configured."
    };
  }
  const attempts = [
    normalizedPhone ? { matched_by: "phone", phone: normalizedPhone } : null,
    cleanText(email, 320) ? { matched_by: "email", email: cleanText(email, 320) } : null,
    cleanText(name, 200) ? { matched_by: "name", name: cleanText(name, 200) } : null
  ].filter(Boolean);
  if (!attempts.length) {
    return { success: true, found: false, status: "insufficient_identifiers" };
  }

  for (const attempt of attempts) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const headers = { "Content-Type": "application/json" };
      if (OUTBOUND_LOOKUP_API_KEY) {
        headers.Authorization = `Bearer ${OUTBOUND_LOOKUP_API_KEY}`;
      }
      const response = await fetch(OUTBOUND_LOOKUP_URL, {
        method: "POST",
        headers,
        body: JSON.stringify(attempt),
        signal: controller.signal
      });
      if (!response.ok) continue;
      const body = await response.json();
      if (body?.found !== true) continue;
      return {
        success: true,
        found: true,
        matched_by: attempt.matched_by,
        status: cleanText(body.customer_facing_status || body.status, 500) || "Information received",
        safe_next_step: cleanText(body.customer_facing_next_step || body.next_step, 500),
        form_submitted: body.form_submitted === true,
        call_one_status: cleanText(body.call_one_status, 200),
        call_two_status: cleanText(body.call_two_status, 200),
        application_completed_date: cleanText(body.application_completed_date, 100),
        follow_up_needed: body.follow_up_needed === true
      };
    } catch (error) {
      inboundLog("[INBOUND]", "outbound_lookup_attempt_failed", {
        matched_by: attempt.matched_by,
        caller_phone: maskedPhoneLastFour(normalizedPhone),
        error: cleanText(error.message, 300)
      });
    } finally {
      clearTimeout(timeout);
    }
  }
  return {
    success: true,
    found: false,
    status: "not_found",
    message: "No matching applicant record was returned."
  };
}

const INBOUND_TOOL_NAMES = new Set([
  "save_inbound_caller_context",
  "lookup_existing_sba_lead",
  "create_inbound_follow_up",
  "create_funding_specialist_handoff"
]);

const SBA_QUALIFICATION_TOOL_FIELDS = Object.freeze([
  "city",
  "state",
  "business_entity_type",
  "time_in_business",
  "estimated_credit_score",
  "gross_monthly_revenue",
  "first_name",
  "last_name",
  "email",
  "phone_number",
  "phone_verified"
]);

async function executeInboundTool(call, name, args) {
  const callId = cleanText(call?.call_id, 100);
  if (!callId) return { success: false, error: "The call session is unavailable." };
  return inboundSessionPersistenceQueue.run(callId, async () => {
    const latestCall = (await getCallById(callId)) || call;
    return executeInboundToolUnlocked(latestCall, name, args);
  });
}

async function executeInboundToolUnlocked(call, name, args) {
  const safeArgs = args && typeof args === "object" ? args : {};

  if (name === "save_inbound_caller_context") {
    const receivedQualificationFields = SBA_QUALIFICATION_TOOL_FIELDS.filter(
      (field) => safeArgs[field] !== undefined && safeArgs[field] !== null &&
        (typeof safeArgs[field] !== "string" || safeArgs[field].trim() !== "")
    );
    inboundLog("[MONDAY_SAVE_TOOL]", "tool_invoked", {
      call_id: call.call_id,
      tool: "save_inbound_caller_context",
      logical_fields: receivedQualificationFields,
      current_monday_item_id: call.monday_item_id || null
    });
    const suppliedIntent = cleanText(safeArgs.intent, 80);
    if (suppliedIntent && !SUPPORTED_INBOUND_INTENTS.includes(suppliedIntent)) {
      return { success: false, error: "A supported inbound intent is required." };
    }
    const savedIntent = savedInboundIntent(call);
    const intentChangeConfirmed = safeArgs.intent_change_confirmed === true;
    const intent = savedIntent && !intentChangeConfirmed
      ? savedIntent
      : suppliedIntent || savedIntent || null;
    const suppliedPhone = cleanText(safeArgs.phone_number, 100);
    const phoneNumber = suppliedPhone ? normalizePhone(suppliedPhone) : null;
    if (suppliedPhone && !validE164Phone(phoneNumber)) {
      return { success: false, error: "The mobile phone number is invalid." };
    }
    const suppliedEmail = cleanText(safeArgs.email, 320);
    const email = normalizeInboundEmail(suppliedEmail);
    if (suppliedEmail && !email) {
      return { success: false, error: "A valid email address is required." };
    }
    const suppliedFullName = cleanInboundContactValue(
      safeArgs.full_name ||
        (!safeArgs.last_name && /\s/.test(String(safeArgs.first_name || ""))
          ? safeArgs.first_name
          : ""),
      160
    );
    const splitName = normalizeInboundFullName(suppliedFullName);
    const firstName = cleanInboundContactValue(
      splitName.first_name || safeArgs.first_name,
      100
    );
    const lastName = cleanInboundContactValue(
      splitName.last_name || safeArgs.last_name,
      100
    );
    const existingFirstName = cleanInboundContactValue(
      call.result?.first_name,
      100
    );
    const existingLastName = cleanInboundContactValue(
      call.result?.last_name,
      100
    );
    const fullName = cleanInboundContactValue(
      suppliedFullName ||
        (firstName || lastName
          ? [
              firstName || existingFirstName,
              lastName || existingLastName
            ].filter(Boolean).join(" ")
          : null),
      160
    );
    const creditScore = normalizeSbaCreditRange(
      safeArgs.estimated_credit_score
    );
    const revenueRange = normalizeSbaRevenueRange(safeArgs.gross_monthly_revenue);
    const entityType = normalizeSbaEntityType(safeArgs.business_entity_type);
    const taxDetails = cleanText(safeArgs.tax_returns_status, 500);
    const taxStatus = taxDetails
      ? /not filed|behind|delinquent|missing|unavailable/i.test(taxDetails)
        ? "Stuck"
        : /filed|current|available|complete/i.test(taxDetails)
          ? "Done"
          : "Working on it"
      : cleanText(safeArgs.tax_status, 100);
    const candidateFields = {
      full_name: fullName,
      first_name: firstName,
      last_name: lastName,
      phone_number: phoneNumber,
      phone: phoneNumber || normalizePhone(call.phone),
      phone_verified: safeArgs.phone_verified === true ? true : null,
      email,
      lead_source: cleanText(safeArgs.lead_source, 160),
      business_name: cleanText(safeArgs.business_name, 200),
      business_entity_type: entityType,
      entity_status: entityType ? "Complete" : cleanText(safeArgs.entity_status, 100),
      time_in_business: cleanText(safeArgs.time_in_business, 200),
      industry: cleanText(safeArgs.industry, 200),
      city: cleanText(safeArgs.city, 160),
      state: cleanText(safeArgs.state, 100),
      zip: cleanText(safeArgs.zip, 20),
      lead_id: cleanText(safeArgs.lead_id, 160),
      funding_use: cleanText(safeArgs.funding_use, 500),
      funding_amount: normalizeInboundAnnualIncome(safeArgs.funding_amount),
      funding_timeline: cleanText(safeArgs.funding_timeline, 300),
      estimated_credit_score: creditScore,
      credit_status: creditScore ? (creditScore === "Not sure" ? "Working on it" : "Done") : null,
      gross_monthly_revenue: revenueRange,
      income_status: revenueRange ? "Done" : null,
      monthly_business_expenses: normalizeInboundAnnualIncome(safeArgs.monthly_business_expenses),
      existing_business_financing: cleanText(safeArgs.existing_business_financing, 500),
      taxes: taxDetails,
      tax_status: taxStatus,
      tax_years_available: cleanText(safeArgs.tax_years_available, 100),
      bank_statements_available: safeArgs.bank_statements_available,
      financial_statements_available: safeArgs.financial_statements_available,
      website_funding_preview_completed: safeArgs.website_funding_preview_completed,
      existing_profile_confirmed: safeArgs.existing_profile_confirmed,
      preliminary_readiness: cleanText(safeArgs.preliminary_readiness, 500),
      missing_items: Array.isArray(safeArgs.missing_items)
        ? safeArgs.missing_items.map((item) => cleanText(item, 200)).filter(Boolean)
        : null,
      next_action: cleanText(safeArgs.next_action, 500),
      call_summary: usefulInboundSummary(safeArgs.call_summary),
      call_outcome: cleanText(safeArgs.call_outcome, 100),
      date_called: call.started_at || call.created_at ||
        new Date().toISOString().slice(0, 10),
      caller_type: "Inbound Call",
      call_status: cleanText(safeArgs.call_outcome, 100),
      monday_item_id: call.monday_item_id,
      call_sid: call.twilio_call_sid
    };
    const savedFields = Object.fromEntries(
      Object.entries(candidateFields).filter(([, value]) =>
        value !== undefined && value !== null && value !== ""
      )
    );
    const currentResult = call.result || {};
    const resultPatch = Object.fromEntries(
      Object.entries({
        ...savedFields,
        ...(intent ? { inbound_intent: intent } : {})
      }).filter(
        ([field, value]) =>
          !inboundSessionFieldEqual(field, currentResult[field], value)
      )
    );
    const currentPayload = call.payload || {};
    const payloadCandidates = {
      phone_number: savedFields.phone_number,
      lead_source: savedFields.lead_source,
      ...(intent ? { inbound_intent: intent } : {})
    };
    const payloadPatch = Object.fromEntries(
      Object.entries(payloadCandidates).filter(
        ([field, value]) =>
          value !== undefined &&
          value !== null &&
          value !== "" &&
          !inboundValuesEqual(currentPayload[field], value)
      )
    );
    const stateChanged =
      Object.keys(resultPatch).length > 0 ||
      Object.keys(payloadPatch).length > 0 ||
      (phoneNumber && normalizePhone(call.phone) !== phoneNumber);
    if (stateChanged) {
      await pool.query(
        `UPDATE ai_calls SET intent = COALESCE($2, intent),
         phone = COALESCE($3, phone),
         payload = payload || $4::jsonb, result = result || $5::jsonb,
         summary = COALESCE($6, summary), outcome = COALESCE($7, outcome),
         updated_at = NOW() WHERE call_id = $1`,
        [
          call.call_id,
          intent,
          phoneNumber,
          JSON.stringify(payloadPatch),
          JSON.stringify(resultPatch),
          savedFields.call_summary || null,
          savedFields.call_outcome || null
        ]
      );
    }
    const capturedSemanticFields = receivedQualificationFields.filter((field) =>
      savedFields[field] !== undefined && savedFields[field] !== null &&
      savedFields[field] !== ""
    );
    if (capturedSemanticFields.length) {
      inboundLog("[SBA_FIELD_CAPTURED]", "fields_captured", {
        call_id: call.call_id,
        monday_item_id: call.monday_item_id || null,
        fields: capturedSemanticFields
      });
    }
    let mondayPersisted = !INBOUND_MONDAY_CONNECTED;
    if (INBOUND_MONDAY_CONNECTED) {
      try {
        inboundLog("[SBA_MONDAY_WRITE_ATTEMPT]", "qualification_fields", {
          call_id: call.call_id,
          monday_item_id: call.monday_item_id || null,
          fields: receivedQualificationFields
        });
        const persisted = await persistSbaQualificationFieldsToMonday(
          call.call_id,
          savedFields
        );
        mondayPersisted = persisted?.success === true;
        inboundLog("[SBA_MONDAY_WRITE_SUCCESS]", "qualification_fields", {
          call_id: call.call_id,
          monday_item_id: persisted?.monday_item_id || call.monday_item_id || null,
          fields: persisted?.logical_fields || receivedQualificationFields
        });
      } catch (error) {
        await pool.query(
          `UPDATE ai_calls SET monday_last_error = $2, updated_at = NOW()
           WHERE call_id = $1`,
          [call.call_id, cleanText(error.message, 1000)]
        );
        inboundLog("[MONDAY_UPDATE]", "qualification_update_failed", {
          call_id: call.call_id,
          logical_fields: receivedQualificationFields,
          error: cleanText(error.message, 300)
        });
        inboundLog("[SBA_MONDAY_WRITE_FAILED]", "qualification_fields", {
          call_id: call.call_id,
          monday_item_id: call.monday_item_id || null,
          fields: receivedQualificationFields,
          error: cleanText(error.message, 300)
        });
      }
    }
    return {
      success: true,
      intent,
      saved_fields: Object.keys(resultPatch),
      state_changed: stateChanged,
      monday_persisted: mondayPersisted,
      caller_phone_available: Boolean(phoneNumber || normalizePhone(call.phone))
    };
  }

  if (name === "lookup_existing_sba_lead") {
    const recognizedEmail = normalizeInboundEmail(safeArgs.email);
    const recognizedFirstName = cleanInboundContactValue(
      safeArgs.first_name,
      100
    );
    const recognizedLastName = cleanInboundContactValue(
      safeArgs.last_name,
      100
    );
    if (recognizedEmail || recognizedFirstName || recognizedLastName) {
      const savedContact = await executeInboundToolUnlocked(
        call,
        "save_inbound_caller_context",
        {
          email: recognizedEmail,
          first_name: recognizedFirstName,
          last_name: recognizedLastName
        }
      );
      if (savedContact?.success !== true) return savedContact;
      call = (await getCallById(call.call_id)) || call;
    }
    const result = await lookupExistingSbaLead(call, {
      email: recognizedEmail || normalizeInboundEmail(call.result?.email),
      first_name: recognizedFirstName || call.result?.first_name,
      last_name: recognizedLastName || call.result?.last_name
    });
    await mergeCallResult(call.call_id, {
      sba_lead_lookup: result,
      sba_lead_match_status: result.status
    });
    await appendAction(call.call_id, {
      action: name,
      success: result.success,
      found: result.found,
      status: result.status
    });
    return result;
  }

  if (name === "create_inbound_follow_up") {
    const followUpReason = cleanText(safeArgs.follow_up_reason, 100);
    if (followUpReason !== "funding_review") {
      return { success: false, error: "The follow-up reason must be funding_review." };
    }
    const followUpDeclined = safeArgs.follow_up_declined === true;
    const suppliedFollowUpDate = cleanText(safeArgs.follow_up_date, 20);
    const followUpDate = normalizeInboundLocalDate(suppliedFollowUpDate);
    const followUpTime = cleanText(safeArgs.follow_up_time, 20);
    const followUpTimezoneInput = cleanText(safeArgs.follow_up_timezone, 100);
    const followUpTimezoneAliases = {
      eastern: "Eastern",
      et: "Eastern",
      central: "Central",
      ct: "Central",
      mountain: "Mountain",
      mt: "Mountain",
      pacific: "Pacific",
      pt: "Pacific"
    };
    const normalizedFollowUpTimezoneInput =
      followUpTimezoneAliases[normalizeMondayKey(followUpTimezoneInput)] ||
      followUpTimezoneInput;
    let followUpTimezone = null;
    let nextFollowUp = null;
    if (!followUpDeclined) {
      if (!followUpDate || !followUpTime || !followUpTimezoneInput) {
        return {
          success: false,
          error: "An exact follow-up date, time, and timezone are required."
        };
      }
      try {
        followUpTimezone = inlineNormalizeAppointmentTimezone(
          normalizedFollowUpTimezoneInput,
          null
        ).timezone;
        const converted = localDateTimeToUtc(
          followUpDate,
          followUpTime,
          followUpTimezone
        );
        if (converted.callbackAt.getTime() <= Date.now()) {
          return { success: false, error: "The follow-up must be in the future." };
        }
        nextFollowUp = converted.callbackAt.toISOString();
      } catch (error) {
        return {
          success: false,
          error: error instanceof SchedulingError
            ? error.message
            : "The follow-up date, time, or timezone is invalid."
        };
      }
    }
    const callSummary = usefulInboundSummary(safeArgs.call_summary);
    const callOutcome = followUpDeclined
      ? "sba_follow_up_declined"
      : "sba_follow_up_scheduled";
    const followUpRecord = {
      follow_up_date: followUpDeclined ? null : followUpDate,
      follow_up_time: followUpDeclined ? null : followUpTime,
      follow_up_timezone: followUpDeclined ? null : followUpTimezone,
      follow_up_reason: followUpReason,
      follow_up_declined: followUpDeclined,
      follow_up_at: nextFollowUp
    };
    const existingFollowUp = call.result?.inbound_follow_up || null;
    const followUpStateChanged = !inboundValuesEqual(
      existingFollowUp,
      followUpRecord
    );
    const followUpSessionChanged =
      followUpStateChanged ||
      call.result?.call_outcome !== callOutcome ||
      (callSummary &&
        !inboundSessionFieldEqual(
          "call_summary",
          call.result?.call_summary,
          callSummary
        ));
    if (followUpSessionChanged) {
      await pool.query(
        `UPDATE ai_calls SET outcome = $2, priority = 'normal',
         next_action = $3, summary = COALESCE($4, summary),
         result = result || $5::jsonb, updated_at = NOW()
         WHERE call_id = $1`,
        [
          call.call_id,
          callOutcome,
          followUpDeclined
            ? "Caller declined funding-review follow-up"
            : `Funding-review follow-up scheduled for ${nextFollowUp}`,
          callSummary,
          JSON.stringify({
            inbound_follow_up: followUpRecord,
            ...followUpRecord,
            next_follow_up: followUpDeclined ? null : followUpDate,
            next_follow_up_date: followUpDeclined ? null : followUpDate,
            follow_up_needed: followUpDeclined ? "No" : "Yes",
            call_outcome: callOutcome,
            call_status: callOutcome,
            ...(callSummary ? { call_summary: callSummary } : {})
          })
        ]
      );
      await appendAction(call.call_id, {
        action: name,
        success: true,
        ...followUpRecord
      });
    }
    const existingFollowUpItemId = cleanText(
      call.result?.inbound_follow_up_item_id,
      100
    );
    let followUpItem = existingFollowUpItemId
      ? { id: existingFollowUpItemId }
      : null;
    let updatedCall = followUpSessionChanged
      ? (await getCallById(call.call_id)) || call
      : call;
    let mondayPersisted = !INBOUND_MONDAY_CONNECTED;
    if (INBOUND_MONDAY_CONNECTED) {
      try {
        const persisted = await persistCallSessionToMonday(call.call_id, {
          alreadySerialized: true,
          overrides: {
            inbound_status: followUpDeclined ? "Follow-Up Declined" : "Follow-Up Scheduled",
            follow_up_needed: followUpDeclined ? "No" : "Yes",
            next_follow_up: followUpDeclined ? null : followUpDate,
            follow_up_time: followUpDeclined ? null : followUpTime,
            follow_up_timezone: followUpDeclined ? null : followUpTimezone,
            follow_up_at: followUpDeclined ? null : nextFollowUp,
            summary: callSummary,
            call_status: callOutcome
          }
        });
        mondayPersisted = persisted?.success === true;
        updatedCall = (await getCallById(call.call_id)) || updatedCall;
        if (!followUpDeclined && !followUpItem) {
          followUpItem = await retryTransientOperation(
            () =>
              createInboundFollowUpRecord(
                updatedCall.monday_item_id,
                "Funding review",
                followUpDate
              ),
            { maxAttempts: 3 }
          );
          if (followUpItem?.id) {
            await mergeCallResult(call.call_id, {
              inbound_follow_up_item_id: String(followUpItem.id)
            });
          }
        }
      } catch (error) {
        inboundLog("[MONDAY]", "follow_up_update_failed", {
          call_id: call.call_id,
          error: cleanText(error.message, 300)
        });
      }
    }
    return {
      success: true,
      follow_up_created: !followUpDeclined,
      follow_up_declined: followUpDeclined,
      follow_up_item_id: followUpItem?.id || null,
      follow_up_date: followUpRecord.follow_up_date,
      follow_up_time: followUpRecord.follow_up_time,
      follow_up_timezone: followUpRecord.follow_up_timezone,
      next_follow_up: followUpRecord.follow_up_date,
      follow_up_at: followUpRecord.follow_up_at,
      call_outcome: callOutcome,
      monday_persisted: mondayPersisted
    };
  }

  if (name === "create_funding_specialist_handoff") {
    const reason = cleanText(safeArgs.reason, 2000);
    if (!reason) return { success: false, error: "A transfer reason is required." };
    const priority = ["normal", "high", "urgent"].includes(safeArgs.priority)
      ? safeArgs.priority
      : "normal";
    await pool.query(
      `UPDATE ai_calls SET outcome = 'specialist_handoff', priority = $2,
       next_action = $3, result = result || $4::jsonb, updated_at = NOW()
       WHERE call_id = $1`,
      [
        call.call_id,
        priority,
        reason,
        JSON.stringify({ funding_specialist_handoff: true, handoff_reason: reason })
      ]
    );
    await appendAction(call.call_id, {
      action: name,
      success: true,
      priority
    });
    let mondayPersisted = !INBOUND_MONDAY_CONNECTED;
    if (INBOUND_MONDAY_CONNECTED) {
      try {
        const persisted = await persistCallSessionToMonday(call.call_id, {
          alreadySerialized: true,
          forceFullState: true,
          overrides: {
            inbound_status: "Funding Specialist Handoff",
            follow_up_needed: "Yes",
            priority
          }
        });
        mondayPersisted = persisted?.success === true;
        call = (await getCallById(call.call_id)) || call;
        await moveInboundCallerToGroup(
          call.monday_item_id,
          INBOUND_MONDAY.groups.transferredToOutbound
        );
      } catch (error) {
        inboundLog("[MONDAY]", "funding_specialist_handoff_update_failed", {
          call_id: call.call_id,
          error: cleanText(error.message, 300)
        });
      }
    }
    return {
      success: true,
      funding_specialist_handoff: true,
      priority,
      monday_persisted: mondayPersisted
    };
  }

  return { success: false, error: `Unknown inbound tool: ${name}` };
}

async function executeDougTool(call, name, args, sessionCallPhase) {
  const safeArgs = args && typeof args === "object" ? args : {};
  const permittedWaitingEnd =
    name === "mark_contact_restriction" ||
    (name === "complete_call" &&
      ["opt_out", "wrong_number", "disconnected", "technical_failure"].includes(
        cleanText(safeArgs.outcome, 80)
      ));
  if (call.awaiting_customer_response && !permittedWaitingEnd) {
    return {
      success: false,
      awaiting_customer_response: true,
      pending_question_type: call.pending_question_type,
      pending_question_text: call.pending_question_text,
      error:
        "Wait for a meaningful completed customer answer before advancing the workflow or saving structured fields."
    };
  }
  if (call.awaiting_customer_response && permittedWaitingEnd) {
    await clearAwaitingCustomerResponse(
      call.call_id,
      name === "mark_contact_restriction"
        ? "call_ended_contact_restriction"
        : `call_ended_${cleanText(safeArgs.outcome, 80) || "complete_call"}`
    );
  }

  if (name === "create_confirmed_appointment") {
    if (cleanText(safeArgs.source_call_id, 100) !== call.call_id) {
      return { success: false, error: "The appointment must use the current source call." };
    }
    const suppliedCallbackAt = new Date(safeArgs.callback_at);
    if (Number.isNaN(suppliedCallbackAt.getTime())) {
      return { success: false, error: "A valid UTC callback timestamp is required." };
    }
    try {
      const calculatedCallback = localDateTimeToUtc(
        safeArgs.customer_local_date,
        safeArgs.customer_local_time,
        safeArgs.timezone
      );
      if (calculatedCallback.callbackAt.getTime() !== suppliedCallbackAt.getTime()) {
        return { success: false, error: "The UTC callback timestamp does not match the confirmed local appointment." };
      }
    } catch (error) {
      if (error instanceof SchedulingError) {
        return { success: false, error: error.message, error_code: error.code };
      }
      throw error;
    }
    try {
      const appointment = await createConfirmedAppointment({
        pool,
        input: { ...safeArgs, source_call_id: call.call_id }
      });
      queueMondaySync(call.call_id, "confirmed_appointment_created");
      const mondayPersisted = await persistInboundMilestone(
        call,
        "confirmed_appointment",
        {
          next_follow_up: safeArgs.customer_local_date,
          follow_up_time: safeArgs.customer_local_time,
          follow_up_timezone: safeArgs.timezone,
          follow_up_at: safeArgs.callback_at,
          appointment_type: safeArgs.callback_type,
          follow_up_needed: "Yes"
        }
      );
      return {
        ...appointment,
        ...(isInboundCallSession(call) ? { monday_persisted: mondayPersisted } : {})
      };
    } catch (error) {
      if (error instanceof SchedulingError) {
        return { success: false, error: error.message, error_code: error.code };
      }
      throw error;
    }
  }

  if (name === "save_call_progress") {
    const currentState = cleanText(safeArgs.current_state, 80) || "unknown";
    const nextState = cleanText(safeArgs.next_state, 80);
    const sentiment = cleanText(safeArgs.sentiment, 50);
    const answers = normalizeDaisyAnswers(safeArgs.answers || {});
    const existingAnswers = normalizeDaisyAnswers(call.result || {});
    const confirmedPurchaseArea = Object.prototype.hasOwnProperty.call(
      safeArgs.answers || {},
      "purchase_area"
    )
      ? exactMeaningfulPurchaseArea(safeArgs.answers.purchase_area)
      : null;
    if (confirmedPurchaseArea) {
      answers.purchase_area = confirmedPurchaseArea;
    } else if (Object.prototype.hasOwnProperty.call(answers, "purchase_area")) {
      delete answers.purchase_area;
    }
    for (const key of ["has_realtor", "applied_with_lender", "has_lender"]) {
      if (existingAnswers[key] !== undefined && answers[key] !== undefined) {
        answers[key] = existingAnswers[key];
      }
    }

    await pool.query(
      `
        UPDATE ai_calls
        SET
          current_state = $2,
          next_state = $3,
          sentiment = COALESCE($4, sentiment),
          result = result || $5::jsonb,
          updated_at = NOW()
        WHERE call_id = $1
      `,
      [
        call.call_id,
        currentState,
        nextState,
        sentiment,
        JSON.stringify({
          ...answers,
          progress_notes: cleanText(safeArgs.notes, 2000),
          conversation_state: {
            current_stage: currentState,
            current_objective: cleanText(safeArgs.current_objective, 1000),
            last_confirmed_fact: cleanText(safeArgs.last_confirmed_fact, 2000),
            pending_question: cleanText(safeArgs.pending_question, 1000),
            next_best_action: cleanText(safeArgs.next_best_action, 1000) || nextState
          }
        })
      ]
    );

    if (confirmedPurchaseArea) {
      console.log(JSON.stringify({
        event: "purchase_area_confirmed",
        call_id: call.call_id,
        purchase_area: confirmedPurchaseArea
      }));
    }

    await appendAction(call.call_id, {
      action: name,
      success: true,
      current_state: currentState,
      next_state: nextState
    });

    return {
      success: true,
      current_state: currentState,
      next_state: nextState,
      saved_fields: Object.keys(answers),
      confirmed_purchase_area: confirmedPurchaseArea
    };
  }

  if (name === "calculate_preliminary_dti") {
    const income = Number(safeArgs.gross_monthly_household_income);
    const debt = Number(safeArgs.monthly_recurring_debt);

    if (!Number.isFinite(income) || income <= 0) {
      return { success: false, error: "Monthly income must be greater than zero." };
    }
    if (!Number.isFinite(debt) || debt < 0) {
      return { success: false, error: "Monthly debt cannot be negative." };
    }

    const dti = Number(((debt / income) * 100).toFixed(2));
    let classification = "strong_preliminary_range";
    if (dti > 57) classification = "needs_dei_review";
    else if (dti > 50) classification = "higher_range_lender_review";
    else if (dti > 45) classification = "review_range";

    const result = {
      gross_monthly_household_income: income,
      monthly_recurring_debt: debt,
      preliminary_dti_percent: dti,
      preliminary_dti_classification: classification
    };

    await mergeCallResult(call.call_id, result);
    await appendAction(call.call_id, { action: name, success: true, ...result });

    return {
      success: true,
      ...result,
      disclaimer: "This is a preliminary estimate, not an underwriting result."
    };
  }

  if (name === "send_resource_link") {
    if (safeArgs.consent_confirmed !== true) {
      return {
        success: false,
        error: "Customer confirmation is required before sending SMS."
      };
    }

    const resourceType = cleanText(safeArgs.resource_type, 50);
    const resource = DAISY_RESOURCE_LIBRARY[resourceType];
    if (!resource) {
      return { success: false, error: "Unsupported resource type." };
    }

    try {
      const message = await twilioClient.messages.create({
        to: call.phone,
        from: TWILIO_FROM_NUMBER,
        body: `Here is the ${resource.description} Daisy mentioned: ${resource.url}`,
        statusCallback: smsStatusCallbackUrl(call)
      });

      await trackSmsMessage(call.call_id, message, resourceType);

      console.log(
        JSON.stringify({
          event: "outbound_sms_accepted",
          call_id: call.call_id,
          message_type: resourceType,
          message_sid: message.sid,
          message_status: message.status || "accepted",
          destination_last_four: String(call.phone || "").slice(-4)
        })
      );

      const patch = normalizeDaisyAnswers({
        [`${resourceType}_sent`]: true,
        last_resource_sent: resourceType,
        last_resource_url: resource.url,
        ...(resourceType === "application"
          ? {
              application_link_sent: true,
              app_started_confirmation: "Agreed to Start",
              application_sms_sid: message.sid,
              application_sms_status:
                cleanText(message.status, 50) || "accepted"
            }
          : {})
      });

      await mergeCallResult(call.call_id, patch);
      if (resourceType === "application") {
        await pool.query(
          `
            UPDATE ai_calls
            SET current_state = 'application_link_sent',
                next_state = 'closing',
                next_action = 'Complete the current call',
                updated_at = NOW()
            WHERE call_id = $1
          `,
          [call.call_id]
        );
      }
      await appendAction(call.call_id, {
        action: name,
        success: true,
        resource_type: resourceType,
        resource_url: resource.url,
        message_sid: message.sid
      });

      if (call.last_attempt_id) {
        await pool.query(
          `
            UPDATE call_attempts
            SET sms_sent = TRUE, updated_at = NOW()
            WHERE attempt_id = $1
          `,
          [call.last_attempt_id]
        );
      }

      queueMondaySync(call.call_id, `resource_${resourceType}_sent`);

      return {
        success: true,
        resource_type: resourceType,
        resource_url: resource.url,
        destination: call.phone.replace(/.(?=.{4})/g, "*"),
        message_sid: message.sid,
        message_status: cleanText(message.status, 50) || "accepted"
      };
    } catch (error) {
      await appendAction(call.call_id, {
        action: name,
        success: false,
        resource_type: resourceType,
        error: cleanText(error.message, 1000)
      });
      return {
        success: false,
        error: "The SMS could not be sent. Create a specialist follow-up instead."
      };
    }
  }

  if (name === "create_specialist_handoff") {
    const handoff = {
      reason: cleanText(safeArgs.reason, 1000),
      priority: cleanText(safeArgs.priority, 30) || "normal",
      summary: cleanText(safeArgs.summary, 4000)
    };

    await pool.query(
      `
        UPDATE ai_calls
        SET
          outcome = 'specialist_handoff',
          sequence_status = 'human_action',
          priority = $4,
          next_action = 'DPA specialist follow-up',
          summary = COALESCE($2, summary),
          result = result || $3::jsonb,
          updated_at = NOW()
        WHERE call_id = $1
      `,
      [
        call.call_id,
        handoff.summary,
        JSON.stringify({ specialist_handoff: handoff }),
        handoff.priority
      ]
    );

    await appendAction(call.call_id, { action: name, success: true, ...handoff });
    queueMondaySync(call.call_id, "specialist_handoff");
    const mondayPersisted = await persistInboundMilestone(
      call,
      "specialist_handoff",
      {
        inbound_status: "Transferred to Outbound",
        follow_up_needed: "Yes",
        priority: handoff.priority,
        summary: handoff.summary,
        call_status: "specialist_handoff"
      }
    );

    return {
      success: true,
      handoff_status: "created",
      priority: handoff.priority,
      next_action: "DPA specialist follow-up",
      ...(isInboundCallSession(call) ? { monday_persisted: mondayPersisted } : {})
    };
  }

  if (name === "transfer_to_specialist") {
    if (safeArgs.prospect_confirmed !== true) {
      return {
        success: false,
        transfer_status: "not_confirmed",
        error: "Customer agreement is required before a live transfer."
      };
    }

    if (!SPECIALIST_PHONE_NUMBER) {
      return {
        success: false,
        transfer_status: "specialist_unavailable",
        fallback: "Create a specialist handoff."
      };
    }

    const current = await getCallById(call.call_id);
    if (!current || !current.twilio_call_sid) {
      return {
        success: false,
        transfer_status: "transfer_failed",
        fallback: "Create a specialist handoff."
      };
    }

    try {
      const transferResponse = new twilio.twiml.VoiceResponse();
      const dial = transferResponse.dial({
        callerId: TWILIO_FROM_NUMBER,
        answerOnBridge: true
      });
      dial.number(SPECIALIST_PHONE_NUMBER);

      await twilioClient.calls(current.twilio_call_sid).update({
        twiml: transferResponse.toString()
      });

      await pool.query(
        `
          UPDATE ai_calls
          SET
            outcome = 'hot_transfer',
            sequence_status = 'active',
            priority = $2,
            next_action = 'Live specialist transfer',
            updated_at = NOW()
          WHERE call_id = $1
        `,
        [call.call_id, cleanText(safeArgs.priority, 30) || "high"]
      );

      await appendAction(call.call_id, {
        action: name,
        success: true,
        transfer_status: "initiated",
        priority: cleanText(safeArgs.priority, 30),
        reason: cleanText(safeArgs.reason, 1000)
      });

      queueMondaySync(call.call_id, "hot_transfer");
      const mondayPersisted = await persistInboundMilestone(
        call,
        "hot_transfer",
        {
          inbound_status: "Transferred to Outbound",
          follow_up_needed: "Yes",
          priority: cleanText(safeArgs.priority, 30) || "high",
          call_status: "hot_transfer"
        }
      );
      return {
        success: true,
        transfer_status: "initiated",
        ...(isInboundCallSession(call) ? { monday_persisted: mondayPersisted } : {})
      };
    } catch (error) {
      await appendAction(call.call_id, {
        action: name,
        success: false,
        transfer_status: "transfer_failed",
        error: cleanText(error.message, 1000)
      });
      return {
        success: false,
        transfer_status: "transfer_failed",
        fallback: "Create a specialist handoff."
      };
    }
  }

  if (name === "mark_contact_restriction") {
    const restrictionType = cleanText(safeArgs.restriction_type, 50);
    if (
      ![
        "wrong_number",
        "invalid_number",
        "do_not_call",
        "not_interested"
      ].includes(restrictionType)
    ) {
      return { success: false, error: "Unsupported restriction type." };
    }

    const wrongNumber = restrictionType === "wrong_number";
    const invalidNumber = restrictionType === "invalid_number";
    const doNotCall = restrictionType === "do_not_call";
    const notInterested = restrictionType === "not_interested";
    const reason = cleanText(safeArgs.reason, 1000);

    await pool.query(
      `
        UPDATE ai_calls
        SET
          wrong_number = wrong_number OR $2,
          invalid_number = invalid_number OR $3,
          do_not_call = do_not_call OR $4,
          outcome = $5,
          sequence_status = CASE
            WHEN $2 OR $3 OR $4 THEN 'suppressed'
            ELSE 'active'
          END,
          next_action = $6,
          awaiting_customer_response = FALSE,
          pending_question_type = NULL,
          pending_question_text = NULL,
          question_asked_at = NULL,
          response_reminder_count = 0,
          updated_at = NOW()
        WHERE call_id = $1
      `,
      [
        call.call_id,
        wrongNumber,
        invalidNumber,
        doNotCall,
        doNotCall ? "opt_out" : restrictionType,
        reason
      ]
    );

    await mergeCallResult(call.call_id, {
      contact_restriction: {
        type: restrictionType,
        reason,
        stop_voice: safeArgs.stop_voice === true,
        stop_sms: safeArgs.stop_sms === true,
        stop_email: safeArgs.stop_email === true
      }
    });

    await appendAction(call.call_id, {
      action: name,
      success: true,
      restriction_type: restrictionType
    });

    queueMondaySync(call.call_id, `restriction_${restrictionType}`);

    return {
      success: true,
      restriction_type: restrictionType,
      future_voice_calls_stopped:
        wrongNumber || invalidNumber || doNotCall || notInterested
    };
  }
  if (name === "complete_call") {
    const outcome = cleanText(safeArgs.outcome, 80) || "disconnected";
    const nextAction = cleanText(safeArgs.next_action, 2000);
    const suppliedSummary = cleanText(safeArgs.summary, 4000);
    const inboundCall =
      call.direction === "inbound" || call.payload?.direction === "inbound";
    const summary = inboundCall
      ? bestInboundDetailedSummary(call, suppliedSummary) || suppliedSummary
      : suppliedSummary;
    const stopSequence = safeArgs.stop_sequence === true;
    const pauseSequence = safeArgs.pause_sequence === true;
    const completionValidation = terminalCompletionValidation(
      call,
      sessionCallPhase
    );
    const sequenceStatus = pauseSequence ? "paused" : "completed";

    await pool.query(
      `
        UPDATE ai_calls
        SET outcome = $2,
            next_action = $3,
            summary = $4,
            sequence_status = $5,
            awaiting_customer_response = FALSE,
            pending_question_type = NULL,
            pending_question_text = NULL,
            question_asked_at = NULL,
            response_reminder_count = 0,
            updated_at = NOW()
        WHERE call_id = $1
      `,
      [call.call_id, outcome, nextAction, summary, sequenceStatus]
    );

    if (call.last_attempt_id) {
      await pool.query(
        `
          UPDATE call_attempts
          SET business_outcome = $2, summary = $3, updated_at = NOW()
          WHERE attempt_id = $1
        `,
        [call.last_attempt_id, outcome, summary]
      );
    }

    await mergeCallResult(call.call_id, {
      final_outcome: outcome,
      next_action: nextAction,
      summary,
      stop_sequence_requested: stopSequence,
      pause_sequence_requested: pauseSequence,
      actual_sequence_status: sequenceStatus,
      completion_validation: completionValidation
    });
    await appendAction(call.call_id, {
      action: name,
      success: true,
      outcome,
      stop_sequence_requested: stopSequence,
      pause_sequence_requested: pauseSequence,
      actual_sequence_status: sequenceStatus,
      session_call_phase: sessionCallPhase,
      completion_validation: completionValidation
    });
    queueMondaySync(call.call_id, `complete_call_${outcome}`);

    return {
      success: true,
      outcome,
      sequence_status: sequenceStatus,
      completion_validation: completionValidation
    };
  }
  return { success: false, error: `Unknown tool: ${name}` };
}

const wixSbaIntakeHandlers = createWixSbaIntakeHandlers({
  createItem: (data) => createInboundCallerItem(data, {
    targetGroupName: "New Leads"
  }),
  verifyItem: verifyWixSbaMondayItem,
  log: inboundLog,
  boardId: MONDAY_BOARD_ID,
  cleanText,
  normalizeEmail: normalizeInboundEmail,
  normalizePhone,
  validPhone: validE164Phone,
  normalizeEntityType: normalizeSbaEntityType,
  normalizeCreditScore: normalizeSbaCreditRange,
  normalizeRevenueRange: normalizeSbaRevenueRange
});

app.options(WIX_SBA_INTAKE_PATH, wixSbaIntakeHandlers.options);
app.post(WIX_SBA_INTAKE_PATH, wixSbaIntakeHandlers.post);

app.get("/", (req, res) => {
  res.json({
    message: "HELUX AI inbound-call demo is online.",
    service: "helux-ai-inbound-calls",
    mode: "inbound-demo",
    outbound_calls_enabled: false,
    version: DOUG_CONFIG.agentVersion,
    worker: "Daisy - SBA Help Center virtual funding assistant",
    realtime_model: OPENAI_REALTIME_MODEL,
    voice: OPENAI_VOICE,
    monday_sync: MONDAY_SYNC_ENABLED ? "enabled" : "disabled",
    monday_adapter: DOUG_CONFIG.mondayAdapterVersion
  });
});

app.get("/health", async (req, res) => {
  try {
    const database = await pool.query("SELECT NOW() AS database_time");
    res.json({
      status: "healthy",
      service: "helux-ai-inbound-calls",
      mode: "inbound-demo",
      outbound_calls_enabled: false,
      monday_connected: inboundMondayConnectionHealthy,
      database_connected: true,
      version: DOUG_CONFIG.agentVersion,
      database: "connected",
      database_time: database.rows[0].database_time,
      openai: Boolean(OPENAI_API_KEY),
      twilio: Boolean(
        TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN && TWILIO_FROM_NUMBER
      ),
      consent_enforcement: ENFORCE_CALL_CONSENT,
      monday: {
        requested: MONDAY_SYNC_REQUESTED,
        enabled: MONDAY_SYNC_ENABLED,
        inbound_connected: inboundMondayConnectionHealthy,
        token_present: Boolean(MONDAY_API_TOKEN),
        board_id: MONDAY_BOARD_ID,
        subitem_board_id: MONDAY_SUBITEM_BOARD_ID,
        api_version: MONDAY_API_VERSION,
        metadata_cached: Boolean(mondayMetadataCache),
        inbound_sync_enabled: MONDAY_INBOUND_SYNC_ENABLED
      }
    });
  } catch (error) {
    res.status(503).json({
      status: "unhealthy",
      service: "helux-ai-inbound-calls",
      mode: "inbound-demo",
      outbound_calls_enabled: false,
      monday_connected: inboundMondayConnectionHealthy,
      database_connected: false,
      database: "disconnected",
      error: error.message
    });
  }
});

app.get(
  "/api/v1/monday/health",
  authenticateHelux,
  async (req, res, next) => {
    try {
      if (!INBOUND_MONDAY_CONNECTED) {
        throw new HttpError(
          409,
          "Inbound monday.com is not fully configured."
        );
      }

      const metadata = await loadInboundMondayMetadata(true);
      res.json({
        success: true,
        connected: true,
        api_version: MONDAY_API_VERSION,
        main_board: {
          id: metadata.id,
          name: metadata.name,
          groups: metadata.groups.map((group) => ({
            id: group.id,
            title: group.title
          })),
          columns: metadata.columns.map((column) => ({
            id: column.id,
            title: column.title,
            type: column.type
          }))
        },
        subitem_board: {
          id: MONDAY_SUBITEM_BOARD_ID
        }
      });
    } catch (error) {
      next(error);
    }
  }
);

app.post("/api/v1/monday/webhook", (req, res) => {
  if (String(req.query.secret || "") !== MONDAY_WEBHOOK_SECRET) {
    return res.status(401).json({ success: false, error: "Unauthorized." });
  }

  if (req.body && req.body.challenge) {
    return res.status(200).json({ challenge: req.body.challenge });
  }

  const event = req.body?.event || null;
  res.status(200).json({ success: true });

  if (MONDAY_INBOUND_SYNC_ENABLED && event) {
    void processMondayInboundEvent(event).catch((error) => {
      console.error("monday.com inbound event failed:", error);
    });
  }
});

app.post(
  "/api/v1/monday/register-webhooks",
  authenticateHelux,
  async (req, res, next) => {
    try {
      const webhookIds = await ensureMondayInboundWebhooks();
      res.json({
        success: true,
        inbound_sync_enabled: MONDAY_INBOUND_SYNC_ENABLED,
        webhook_ids: webhookIds
      });
    } catch (error) {
      next(error);
    }
  }
);

app.post(
  "/api/v1/scheduling/confirmed-appointments",
  authenticateHelux,
  async (req, res, next) => {
    try {
      const body = req.body || {};
      const acceptedFields = new Set([
        "customer_local_date",
        "customer_local_time",
        "timezone",
        "timezone_label",
        "callback_type",
        "callback_reason",
        "prospect_confirmed",
        "source_call_id",
        "discussion_summary"
      ]);
      const unexpected = Object.keys(body).filter((field) => !acceptedFields.has(field));
      if (unexpected.length) {
        throw new HttpError(422, `Unsupported scheduling fields: ${unexpected.join(", ")}.`);
      }
      const appointment = await createConfirmedAppointment({ pool, input: body });
      queueMondaySync(body.source_call_id, "confirmed_appointment_api_created");
      res.status(201).json(appointment);
    } catch (error) {
      next(error);
    }
  }
);

app.post(
  "/api/v1/calls",
  authenticateHelux,
  async (req, res, next) => {
    try {
      if (!OUTBOUND_CALLS_ENABLED) {
        throw new HttpError(409, "Outbound call initiation is disabled in the inbound service.");
      }
      const incoming = req.body || {};
      const acceptedLeadFields = [
        "case_id",
        "lead_id",
        "phone",
        "timezone",
        "consent_confirmed",
        "ai_voice_consent",
        "consent_status",
        "consent_timestamp",
        "consent_source",
        "first_name",
        "last_name",
        "name",
        "customer_name",
        "estimated_dpa",
        "credit_score",
        "mid_fico",
        "fico_score",
        "fico",
        "household_income",
        "income",
        "employment_history",
        "employment",
        "tax_history_submitted",
        "tax_return_history",
        "taxes_filed",
        "readiness_score",
        "has_lender",
        "has_realtor",
        "realtor_status",
        "applied_with_lender",
        "applied_other_lender",
        "purchase_timeframe",
        "time_frame",
        "timeframe",
        "priority",
        "human_owner_id",
        "owner_id",
        "app_started_confirmation",
        "application_status"
      ];
      const payload = Object.fromEntries(
        acceptedLeadFields
          .filter((field) => incoming[field] !== undefined)
          .map((field) => [field, incoming[field]])
      );
      const requestKey = callRequestKey(payload);
      const phone = normalizePhone(payload.phone);
      if (!phone) {
        throw new HttpError(422, "A valid phone number is required.");
      }

      const existing = await getCallByRequestKey(requestKey);
      if (existing) {
        queueMondaySync(existing.call_id, "duplicate_request_refresh");
        return res.status(200).json({
          success: true,
          duplicate: true,
          call_id: existing.call_id,
          status: existing.status,
          sequence_status: existing.sequence_status,
          attempts_used: existing.attempts,
          twilio_call_sid: existing.twilio_call_sid,
          monday_item_id: existing.monday_item_id
        });
      }

      const callId = createPublicId("CALL");
      const streamToken = createStreamToken();
      const timezone = normalizeTimezone(payload.timezone);
      const consentConfirmed = confirmedConsent(payload);
      const consentStatus = consentConfirmed ? "confirmed" : "unverified";
      if (ENFORCE_CALL_CONSENT && !consentConfirmed) {
        throw new HttpError(422, "Confirmed AI voice consent is required.");
      }

      const consentTimestamp = payload.consent_timestamp
        ? new Date(payload.consent_timestamp)
        : null;
      const safeConsentTimestamp =
        consentTimestamp && !Number.isNaN(consentTimestamp.getTime())
          ? consentTimestamp
          : null;

      const insertResult = await pool.query(
        `
          INSERT INTO ai_calls (
            call_id, request_key, case_id, lead_id, phone, status,
            sequence_status, stream_token, payload,
            timezone, consent_status, consent_timestamp, consent_source,
            agent_version, prompt_version, tool_version, knowledge_version,
            routing_version, priority, human_owner_id
          )
          VALUES (
            $1, $2, $3, $4, $5, 'created', 'ready', $6, $7::jsonb,
            $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
          )
          RETURNING *
        `,
        [
          callId,
          requestKey,
          cleanText(payload.case_id, 150),
          cleanText(payload.lead_id, 150),
          phone,
          streamToken,
          JSON.stringify({
            ...payload,
            outbound_call_reason: "initial_lead_call"
          }),
          timezone,
          consentStatus,
          safeConsentTimestamp,
          cleanText(payload.consent_source, 255),
          DOUG_CONFIG.agentVersion,
          DOUG_CONFIG.promptVersion,
          DOUG_CONFIG.toolVersion,
          DOUG_CONFIG.knowledgeVersion,
          DOUG_CONFIG.routingVersion,
          cleanText(payload.priority, 30) || "normal",
          cleanText(payload.human_owner_id || payload.owner_id, 100)
        ]
      );

      const call = insertResult.rows[0];
      await mergeCallResult(
        call.call_id,
        normalizeDaisyAnswers({
          has_realtor: payload.has_realtor ?? payload.realtor_status ?? null,
          applied_with_lender:
            payload.applied_with_lender ?? payload.applied_other_lender ?? null,
          app_started_confirmation:
            payload.app_started_confirmation ?? payload.application_status ?? null,
          time_frame: payload.time_frame ?? payload.timeframe ?? null,
          interest_level: null,
          application_link_sent: false,
          outbound_call_reason: "initial_lead_call",
          initial_call_source_lead_id:
            cleanText(payload.lead_id || payload.case_id, 150) || requestKey
        })
      );
      queueMondaySync(call.call_id, "sequence_created");

      const firstAttempt = await createCallAttempt(call.call_id, {
        attemptType: "initial_lead_call",
        idempotencyKey: `initial_lead_call:${call.call_id}:1`
      });
      if (!firstAttempt) {
        throw new HttpError(409, "The initial call could not be created.");
      }
      const twilioCall = await placeTwilioCall(call, {
        attemptId: firstAttempt.attempt_id,
        source: "initial",
        callReason: "initial_lead_call"
      });

      res.status(201).json({
        success: true,
        duplicate: false,
        call_id: call.call_id,
        status: twilioCall.status || "queued",
        sequence_status: "calling",
        attempts_used: 1,
        twilio_call_sid: twilioCall.sid,
        monday_sync_queued: MONDAY_SYNC_ENABLED
      });
    } catch (error) {
      next(error);
    }
  }
);
app.post(
  "/api/v1/calls/:callId/sync-monday",
  authenticateHelux,
  async (req, res, next) => {
    try {
      if (!MONDAY_SYNC_ENABLED) {
        throw new HttpError(
          409,
          "monday.com sync is disabled or not fully configured."
        );
      }

      const call = await getCallById(req.params.callId);
      if (!call) throw new HttpError(404, "Call not found.");

      await syncCallSequenceToMonday(call.call_id, "manual_sync");
      const refreshed = await getCallById(call.call_id);
      const attempts = await getAttemptsForCall(call.call_id);

      res.json({
        success: !refreshed.monday_last_error,
        call_id: call.call_id,
        monday_item_id: refreshed.monday_item_id,
        monday_group_id: refreshed.monday_group_id,
        monday_last_sync_at: refreshed.monday_last_sync_at,
        monday_last_error: refreshed.monday_last_error,
        attempt_subitems: attempts.map((attempt) => ({
          attempt_id: attempt.attempt_id,
          monday_subitem_id: attempt.monday_subitem_id,
          monday_last_sync_at: attempt.monday_last_sync_at,
          monday_last_error: attempt.monday_last_error
        }))
      });
    } catch (error) {
      next(error);
    }
  }
);

app.get(
  "/api/v1/calls/:callId",
  authenticateHelux,
  async (req, res, next) => {
    try {
      const call = await getCallById(req.params.callId);
      if (!call) throw new HttpError(404, "Call not found.");
      const attempts = await getAttemptsForCall(call.call_id);

      res.json({
        success: true,
        call: {
          call_id: call.call_id,
          case_id: call.case_id,
          lead_id: call.lead_id,
          phone: call.phone,
          status: call.status,
          sequence_status: call.sequence_status,
          twilio_call_sid: call.twilio_call_sid,
          attempts_used: call.attempts,
          timezone: call.timezone,
          current_state: call.current_state,
          next_state: call.next_state,
          awaiting_customer_response: call.awaiting_customer_response,
          pending_question_type: call.pending_question_type,
          pending_question_text: call.pending_question_text,
          question_asked_at: call.question_asked_at,
          response_reminder_count: call.response_reminder_count,
          sentiment: call.sentiment,
          outcome: call.outcome,
          next_action: call.next_action,
          summary: call.summary,
          transcript: call.transcript,
          result: {
            ...publicCallResult(call.result || {}),
            purchase_timeline_detail: call.result?.purchase_timeline_detail,
            time_frame: call.result?.time_frame,
            interest_level: call.result?.interest_level,
            applied_with_lender: call.result?.applied_with_lender,
            has_realtor: call.result?.has_realtor,
            purchase_area: call.result?.purchase_area,
            preliminary_dti_percent: call.result?.preliminary_dti_percent,
            preliminary_dti_classification:
              call.result?.preliminary_dti_classification,
            application_link_sent: call.result?.application_link_sent,
            final_outcome: call.result?.final_outcome,
            next_action: call.result?.next_action,
            summary: call.result?.summary,
            normal_completion_recorded:
              call.result?.normal_completion_recorded,
            final_hangup_requested: call.result?.final_hangup_requested,
            final_hangup_completed: call.result?.final_hangup_completed,
            completion_reason: call.result?.completion_reason,
            contact_restriction: call.result?.contact_restriction
          },
          last_error: call.last_error,
          monday_item_id: call.monday_item_id,
          monday_group_id: call.monday_group_id,
          monday_last_sync_at: call.monday_last_sync_at,
          monday_last_error: call.monday_last_error,
          created_at: call.created_at,
          started_at: call.started_at,
          answered_at: call.answered_at,
          completed_at: call.completed_at,
          attempt_records: attempts.map((attempt) => ({
            attempt_id: attempt.attempt_id,
            attempt_number: attempt.attempt_number,
            call_leg: attempt.call_leg,
            dialed_at: attempt.dialed_at,
            answered_at: attempt.answered_at,
            completed_at: attempt.completed_at,
            twilio_call_sid: attempt.twilio_call_sid,
            technical_status: attempt.technical_status,
            business_outcome: attempt.business_outcome,
            answered_by: attempt.answered_by,
            voicemail_left: attempt.voicemail_left,
            sms_sent: attempt.sms_sent,
            duration_seconds: attempt.duration_seconds,
            disconnect_reason: attempt.disconnect_reason,
            summary: attempt.summary,
            last_error: attempt.last_error,
            created_at: attempt.created_at,
            updated_at: attempt.updated_at
          }))
        }
      });
    } catch (error) {
      next(error);
    }
  }
);

app.post(["/incoming-call", "/api/v1/twilio/inbound"], async (req, res, next) => {
  try {
    const callerPhone = normalizePhone(req.body.From);
    const calledNumber = normalizePhone(req.body.To) || cleanText(req.body.To, 50);
    const twilioCallSid = cleanText(req.body.CallSid, 80);
    const leadSource = cleanText(
      req.body.lead_source || req.body.LeadSource || req.query.lead_source,
      160
    );
    const campaign = cleanText(
      req.body.campaign || req.body.Campaign || req.query.campaign,
      160
    );
    inboundLog("[TWILIO_WS]", "incoming_call_received", {
      path: req.path,
      host: req.get("host") || null,
      twilio_call_sid: twilioCallSid || null,
      caller_phone: maskedPhoneLastFour(callerPhone)
    });
    if (!twilioCallSid) throw new HttpError(422, "CallSid is required.");

    const requestKey = `inbound:${twilioCallSid}`;
    let call = await getCallByRequestKey(requestKey);
    if (!call) {
      const callId = createPublicId("INBOUND");
      const streamToken = createStreamToken();
      const payload = {
        direction: "inbound",
        caller_phone: callerPhone,
        called_number: calledNumber,
        twilio_call_sid: twilioCallSid,
        lead_source: leadSource,
        campaign
      };
      const result = await pool.query(
        `INSERT INTO ai_calls (
          call_id, request_key, phone, status, sequence_status, stream_token,
          twilio_call_sid, payload, result, direction, timezone, consent_status,
          agent_version, prompt_version, tool_version, knowledge_version,
          routing_version, started_at, answered_at
        ) VALUES (
          $1, $2, $3, 'answered', 'active', $4, $5, $6::jsonb, $7::jsonb,
          'inbound', $8, 'unverified', $9, $10, $11, $12, $13, NOW(), NOW()
        ) RETURNING *`,
        [
          callId,
          requestKey,
          callerPhone || `unknown:${twilioCallSid}`,
          streamToken,
          twilioCallSid,
          JSON.stringify(payload),
          JSON.stringify({
            phone: callerPhone,
            phone_number: callerPhone,
            date_called: new Date().toISOString().slice(0, 10),
            caller_type: "Inbound Call",
            call_direction: "inbound",
            call_status: "answered",
            call_started_at: new Date().toISOString(),
            call_sid: twilioCallSid
          }),
          DEFAULT_TIMEZONE,
          DOUG_CONFIG.agentVersion,
          DOUG_CONFIG.promptVersion,
          DOUG_CONFIG.toolVersion,
          DOUG_CONFIG.knowledgeVersion,
          DOUG_CONFIG.routingVersion
        ]
      );
      call = result.rows[0];
      inboundLog("[TWILIO]", "inbound_call_started", {
        call_id: call.call_id,
        caller_phone: maskedPhoneLastFour(callerPhone),
        twilio_call_sid: twilioCallSid
      });
    }

    await ensureInboundMondayCaller(call);
    call = (await getCallById(call.call_id)) || call;
    inboundLog("[MONDAY_LINK]", "incoming_call_link_result", {
      call_id: call.call_id,
      normalized_caller_phone_last4: maskedPhoneLastFour(normalizePhone(call.phone)),
      monday_item_id: call.monday_item_id || null,
      linked_before_twiml: Boolean(call.monday_item_id)
    });

    const response = new twilio.twiml.VoiceResponse();
    const connect = response.connect();
    const websocketUrl = `${inboundWebsocketBaseUrl(req)}/api/v1/twilio/media`;
    const stream = connect.stream({
      url: websocketUrl
    });
    const parameters = {
      call_id: call.call_id,
      stream_token: call.stream_token,
      direction: "inbound",
      caller_phone: callerPhone || "",
      called_number: calledNumber || "",
      twilio_call_sid: twilioCallSid,
      lead_source: leadSource || "",
      campaign: campaign || ""
    };
    for (const [name, value] of Object.entries(parameters)) {
      stream.parameter({ name, value: String(value) });
    }
    inboundLog("[TWILIO_WS]", "twiml_generated", {
      call_id: call.call_id,
      twilio_call_sid: twilioCallSid,
      websocket_url: websocketUrl,
      parameter_names: Object.keys(parameters),
      has_stream_token: Boolean(call.stream_token)
    });
    res.type("text/xml").send(response.toString());
  } catch (error) {
    next(error);
  }
});

app.post("/api/v1/twilio/voice", async (req, res, next) => {
  try {
    const callId = cleanText(req.query.call_id, 100);
    const token = cleanText(req.query.token, 160);
    const call = await validateCallToken(callId, token);

    if (!call) throw new HttpError(401, "Invalid call token.");

    const response = new twilio.twiml.VoiceResponse();
    const connect = response.connect();
    const stream = connect.stream({
      url: `${websocketBaseUrl()}/api/v1/twilio/media`
    });

    stream.parameter({ name: "call_id", value: call.call_id });
    stream.parameter({ name: "stream_token", value: call.stream_token });

    res.type("text/xml").send(response.toString());
  } catch (error) {
    next(error);
  }
});

app.post("/api/v1/twilio/sms-status", async (req, res, next) => {
  try {
    const callId = cleanText(req.query.call_id, 100);
    const token = cleanText(req.query.token, 160);
    const call = await validateCallToken(callId, token);
    if (!call) throw new HttpError(401, "Invalid call token.");

    const messageSid = cleanText(req.body.MessageSid, 80);
    const status = cleanText(req.body.MessageStatus, 50) || "unknown";
    const errorCode = cleanText(req.body.ErrorCode, 50);
    const errorMessage = cleanText(req.body.ErrorMessage, 2000);
    if (!messageSid) throw new HttpError(422, "MessageSid is required.");

    const tracked = await pool.query(
      `
        UPDATE sms_deliveries
        SET status = $2, error_code = $3, error_message = $4,
            updated_at = NOW()
        WHERE message_sid = $1
        RETURNING message_type
      `,
      [messageSid, status, errorCode, errorMessage]
    );
    const messageType =
      tracked.rows[0]?.message_type ||
      (call.result?.application_sms_sid === messageSid
        ? "application"
        : "unknown");
    const failed = ["failed", "undelivered"].includes(status.toLowerCase());

    inboundLog("[TWILIO]", "sms_status", {
      call_id: call.call_id,
      message_type: messageType,
      message_sid: messageSid,
      message_status: status,
      error_code: errorCode,
      error_message: cleanText(errorMessage, 300)
    });

    await mergeCallResult(call.call_id, {
      [`${messageType}_sms_status`]: status,
      [`${messageType}_sms_error_code`]: errorCode,
      [`${messageType}_sms_error`]: errorMessage,
      ...(messageType === "application"
        ? { application_sms_status: status, application_sms_failed: failed }
        : {})
    });
    await appendAction(call.call_id, {
      action: "sms_delivery_status",
      success: !failed,
      message_type: messageType,
      message_sid: messageSid,
      status,
      error_code: errorCode,
      error: errorMessage
    });

    if (failed) {
      const humanReviewNote = messageType === "application"
        ? "Human must send the DPA application link manually"
        : `Human review required for failed ${messageType} text delivery`;
      await pool.query(
        `
          UPDATE ai_calls
          SET next_action = $2,
              last_error = $3,
              updated_at = NOW()
          WHERE call_id = $1
        `,
        [
          call.call_id,
          humanReviewNote,
          `${messageType} SMS ${status}${errorCode ? ` (${errorCode})` : ""}`
        ]
      );
      queueMondaySync(call.call_id, `${messageType}_sms_${status}`);
    }

    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.post("/api/v1/twilio/status", async (req, res, next) => {
  try {
    const callId = cleanText(req.query.call_id, 100);
    const token = cleanText(req.query.token, 160);
    const call = await validateCallToken(callId, token);

    if (!call) throw new HttpError(401, "Invalid call token.");

    const status = cleanText(req.body.CallStatus, 50) || "unknown";
    const twilioCallSid = cleanText(req.body.CallSid, 80);
    const durationSeconds = Number(req.body.CallDuration || 0);
    const answeredByRaw = cleanText(req.body.AnsweredBy, 50);
    const answeredBy = answeredByRaw
      ? answeredByRaw.toLowerCase().startsWith("human")
        ? "human"
        : answeredByRaw.toLowerCase().startsWith("machine")
          ? "voicemail"
          : "unknown"
      : null;

    await updateCallStatus(call.call_id, status, {
      twilio_call_sid: twilioCallSid
    });

    const refreshed = await getCallById(call.call_id);
    if (refreshed && refreshed.last_attempt_id && durationSeconds >= 0) {
      await pool.query(
        `
          UPDATE call_attempts
          SET
            duration_seconds = $2,
            answered_by = COALESCE($3::VARCHAR(30), answered_by),
            updated_at = NOW()
          WHERE attempt_id = $1
        `,
        [refreshed.last_attempt_id, durationSeconds, answeredBy]
      );
      queueMondaySync(call.call_id, `twilio_status_${status}_details`);
    }

    if (terminalCallStatus(status)) {
      await finalizeAfterTerminal(call.call_id, status);
      let finalCall = await getCallById(call.call_id);
      if (
        finalCall &&
        (finalCall.direction === "inbound" ||
          finalCall.payload?.direction === "inbound")
      ) {
        await persistFinalInboundSession({
          call_id: finalCall.call_id,
          intent: finalCall.intent || finalCall.result?.inbound_intent,
          outcome: finalCall.outcome || status,
          next_action: finalCall.next_action,
          call_status: status,
          inbound_status: "Closed",
          date_called: finalCall.started_at || finalCall.created_at,
          completion_status: status,
          disconnect_reason:
            status === "completed"
              ? finalCall.result?.disconnect_reason
              : status
        });
        finalCall = (await getCallById(call.call_id)) || finalCall;
      }
      queueMondaySync(call.call_id, `twilio_terminal_${status}`);
      void notifyHelux(finalCall);
    }

    res.status(204).end();
  } catch (error) {
    next(error);
  }
});
mediaServer.on("error", (error) => {
  console.error("[TWILIO_WS] WebSocket server error:", error);
});

mediaServer.on("connection", (twilioSocket, request) => {
  inboundLog("[TWILIO_WS]", "twilio_websocket_connected", {
    request_url: request?.url || null,
    remote_address: request?.socket?.remoteAddress || null
  });
  let openaiSocket = null;
  let call = null;
  let streamSid = null;
  let latestMediaTimestamp = 0;
  let responseStartTimestamp = null;
  let lastAssistantItemId = null;
  let markCounter = 0;
  let initialGreetingStarted = false;
  let pendingAudio = [];
  let closed = false;
  let assistantResponseActive = false;
  let responseCreatePending = false;
  let queuedResponseOptions = null;
  let activeResponseRequestOptions = null;
  let activeResponseIncludesAssistanceMaximum = false;
  let assistanceMaximumMentioned =
    call?.result?.assistance_maximum_mentioned === true;
  let pendingResponsePreservesQuestion = false;
  let activeResponsePreservesQuestion = false;
  let assistantResponseFinished = true;
  let normalCompletionRecorded = false;
  let normalCompletionPersistenceInProgress = false;
  let normalEndRequested = false;
  let sessionCallPhase = "";
  let finalClosingRequested = false;
  let finalClosingResponseId = "";
  let finalPlaybackMarkName = "";
  let finalHangupInProgress = false;
  let finalHangupCompleted = false;
  let finalHangupAttemptCount = 0;
  let finalHangupFallbackTimer = null;
  let finalAbsoluteHangupTimer = null;
  let activeTwilioCallSid = "";
  let activeTwilioStreamSid = "";
  let assistantAudioQueuedForResponse = false;
  let sustainedSpeechTimer = null;
  let speechCandidateStartedAt = 0;
  let speechCandidateConfirmed = false;
  let speechCandidateWhileAssistantSpeaking = false;
  let silenceReminderTimer = null;
  let customerTranscriptDebounceTimer = null;
  let pendingCustomerTranscripts = [];
  let customerSpeaking = false;
  let customerTurnBeganWhileAssistantSpeaking = false;
  let pendingTranscriptWasWhileAssistantSpeaking = false;
  let awaitingCustomerResponse = false;
  let pendingQuestionType = null;
  let pendingQuestionText = null;
  let questionAskedAt = null;
  let responseReminderCount = 0;
  let assistantTranscriptBuffer = "";
  let assistantTranscriptSaved = false;
  let questionCapturedForResponse = false;
  let pendingResponseWaitingPromptKind = null;
  let activeResponseWaitingPromptKind = null;
  let lastWaitingPromptKind = null;
  let suspendedQuestionState = null;
  let complianceRecoveryActive = false;
  let disconnectPersistencePromise = null;
  const handledToolCalls = new Set();
  const handledUserTurns = new Set();
  const pendingMarkNames = new Set();

  function briefListeningAcknowledgement(value) {
    return isListeningAcknowledgement(value);
  }

  function sendToOpenAI(event) {
    if (openaiSocket && openaiSocket.readyState === WebSocket.OPEN) {
      openaiSocket.send(JSON.stringify(event));
      return true;
    }
    return false;
  }

  function sendToTwilio(message) {
    if (twilioSocket.readyState === WebSocket.OPEN) {
      twilioSocket.send(JSON.stringify(message));
      return true;
    }
    return false;
  }

  function lockSessionCallPhase(loadedCall, attempt) {
    if (sessionCallPhase) return sessionCallPhase;
    sessionCallPhase = resolveSessionCallPhase(loadedCall, attempt);
    return sessionCallPhase;
  }

  function refreshActiveRealtimeInstructions() {
    if (!call || !sessionCallPhase) return false;
    return sendToOpenAI({
      type: "session.update",
      session: {
        instructions: buildDaisyInboundInstructions(call)
      }
    });
  }

  function refreshIntentInstructionsIfChanged(previousIntent) {
    const currentIntent = savedInboundIntent(call);
    if (!currentIntent || currentIntent === previousIntent) return false;
    return refreshActiveRealtimeInstructions();
  }

  function persistBeforeInboundMediaRelease(reason) {
    if (!call) return Promise.resolve(null);
    if (disconnectPersistencePromise) return disconnectPersistencePromise;
    disconnectPersistencePromise = (async () => {
      await mergeCallResult(call.call_id, {
        call_status: "disconnected",
        disconnect_reason: reason,
        disconnected_at: new Date().toISOString()
      });
      const finalCall = (await getCallById(call.call_id)) || call;
      const persisted = await persistFinalInboundSession({
        call_id: finalCall.call_id,
        intent: finalCall.intent || finalCall.result?.inbound_intent,
        outcome: finalCall.outcome,
        next_action: finalCall.next_action,
        call_status: "disconnected",
        date_called: finalCall.started_at || finalCall.created_at,
        completion_status: "disconnected"
      });
      call = (await getCallById(call.call_id)) || persisted || call;
      inboundLog("[INBOUND]", "media_session_release_ready", {
        call_id: call.call_id,
        reason
      });
      return call;
    })();
    return disconnectPersistencePromise;
  }

  function currentCallIsTerminal() {
    return (
      normalEndRequested ||
      normalCompletionRecorded ||
      finalHangupCompleted ||
      String(call?.status || "").toLowerCase() === "completed"
    );
  }

  function exactFinalClosingSpoken(value) {
    const expected = buildInboundFinalClosing(call);
    return normalizeCustomerUtterance(value) === normalizeCustomerUtterance(expected);
  }

  function stopCurrentCallAutomation() {
    cancelSilenceReminder();
    if (customerTranscriptDebounceTimer) {
      clearTimeout(customerTranscriptDebounceTimer);
      customerTranscriptDebounceTimer = null;
    }
    if (sustainedSpeechTimer) {
      clearTimeout(sustainedSpeechTimer);
      sustainedSpeechTimer = null;
    }
    speechCandidateStartedAt = 0;
    speechCandidateConfirmed = false;
    speechCandidateWhileAssistantSpeaking = false;
    customerSpeaking = false;
    pendingCustomerTranscripts = [];
    pendingTranscriptWasWhileAssistantSpeaking = false;
    queuedResponseOptions = null;
  }

  function beginNormalCallTermination(reason) {
    if (normalEndRequested) return false;
    normalEndRequested = true;
    finalClosingRequested = true;
    stopCurrentCallAutomation();
    finalAbsoluteHangupTimer = setTimeout(() => {
      void physicallyEndActiveTwilioCall("absolute_normal_end_timeout");
    }, 60000);
    console.log(JSON.stringify({
      event: "normal_call_termination_started",
      call_id: call?.call_id || null,
      reason,
      absolute_timeout_ms: 60000
    }));
    return true;
  }

  function sendMark() {
    if (!streamSid) return;
    markCounter += 1;
    const name = `openai-${markCounter}`;
    if (sendToTwilio({
      event: "mark",
      streamSid,
      mark: { name }
    })) pendingMarkNames.add(name);
  }

  function sendFinalHangupMark() {
    if (
      !normalEndRequested ||
      finalPlaybackMarkName ||
      finalHangupCompleted ||
      !activeTwilioStreamSid
    ) return false;

    finalPlaybackMarkName =
      "daisy_final_hangup_" +
      Date.now() +
      "_" +
      Math.random().toString(36).slice(2, 8);
    if (!sendToTwilio({
      event: "mark",
      streamSid: activeTwilioStreamSid,
      mark: { name: finalPlaybackMarkName }
    })) {
      finalPlaybackMarkName = "";
      return false;
    }
    pendingMarkNames.add(finalPlaybackMarkName);
    console.log(JSON.stringify({
      event: "final_hangup_mark_sent",
      call_id: call.call_id,
      mark_name: finalPlaybackMarkName,
      stream_sid: activeTwilioStreamSid
    }));
    finalHangupFallbackTimer = setTimeout(() => {
      void physicallyEndActiveTwilioCall("final_mark_timeout");
    }, 45000);
    return true;
  }

  async function handleInterruption() {
    if (
      !streamSid ||
      !lastAssistantItemId ||
      responseStartTimestamp === null
    ) {
      return;
    }

    const elapsed = Math.max(
      0,
      latestMediaTimestamp - responseStartTimestamp
    );

    sendToTwilio({ event: "clear", streamSid });
    pendingMarkNames.clear();
    sendToOpenAI({
      type: "conversation.item.truncate",
      item_id: lastAssistantItemId,
      content_index: 0,
      audio_end_ms: elapsed
    });

    responseStartTimestamp = null;
    lastAssistantItemId = null;
  }

  function requestAssistantResponse(options = {}) {
    if (
      currentCallIsTerminal() &&
      options.allowTerminalClosing !== true
    ) return false;
    if (awaitingCustomerResponse && options.allowWhileAwaiting !== true) {
      return false;
    }
    if (assistantResponseActive || responseCreatePending) {
      if (options.queueIfBusy === true) queuedResponseOptions = { ...options };
      return false;
    }
    responseCreatePending = true;
    assistantResponseFinished = false;
    activeResponseRequestOptions = {
      ...options,
      ...(options.response
        ? { response: { ...options.response } }
        : {})
    };
    pendingResponsePreservesQuestion = options.preservePendingQuestion === true;
    pendingResponseWaitingPromptKind = options.waitingPromptKind || null;
const event = { type: "response.create" };
if (options.response) event.response = options.response;

if (!sendToOpenAI(event)) {
  responseCreatePending = false;
  assistantResponseFinished = true;
  activeResponseRequestOptions = null;
  pendingResponsePreservesQuestion = false;
  pendingResponseWaitingPromptKind = null;
  return false;
}

return true;
}
  function currentQuestionState() {
    return {
      pending_question_type: pendingQuestionType,
      pending_question_text: pendingQuestionText,
      question_asked_at: questionAskedAt,
      response_reminder_count: responseReminderCount
    };
  }

  function cancelSilenceReminder() {
    if (silenceReminderTimer) clearTimeout(silenceReminderTimer);
    silenceReminderTimer = null;
  }

  function scheduleSilenceReminder() {
    cancelSilenceReminder();
    if (
      currentCallIsTerminal() ||
      !awaitingCustomerResponse ||
      customerSpeaking ||
      !assistantResponseFinished ||
      pendingMarkNames.size ||
      responseReminderCount >= 2 ||
      closed
    ) return;

    silenceReminderTimer = setTimeout(() => {
      silenceReminderTimer = null;
      void (async () => {
        if (!awaitingCustomerResponse || customerSpeaking || closed) return;
        const nextCount = responseReminderCount + 1;
        responseReminderCount = nextCount;
        await setResponseReminderCount(
          call.call_id,
          nextCount,
          currentQuestionState()
        );
        const instructions =
          nextCount === 1 || pendingQuestionType === "intent_discovery"
          ? 'Say exactly: "Are you still with me?" Say nothing else.'
          : `Repeat this pending question once, using the same meaning and no additional question: ${JSON.stringify(
              pendingQuestionText
            )}`;
        requestAssistantResponse({
          allowWhileAwaiting: true,
          preservePendingQuestion: true,
          waitingPromptKind:
            nextCount === 1 || pendingQuestionType === "intent_discovery"
              ? "presence_reminder"
              : "pending_repeat",
          response: { output_modalities: ["audio"], instructions }
        });
      })().catch((error) => {
        console.error("Daisy silence reminder failed:", error);
      });
    }, 8000);
  }

  async function captureAssistantQuestion(transcript) {
    if (activeResponsePreservesQuestion) return;
    const question = extractPrimaryQuestion(transcript);
    if (!question) return;
    const state = await setAwaitingCustomerResponse(call.call_id, question);
    awaitingCustomerResponse = true;
    pendingQuestionType = state.pending_question_type;
    pendingQuestionText = state.pending_question_text;
    questionAskedAt = state.question_asked_at;
    responseReminderCount = 0;
    queuedResponseOptions = null;
    scheduleSilenceReminder();
  }

  async function endLocalWaitingState(reason) {
    cancelSilenceReminder();
    await clearAwaitingCustomerResponse(call.call_id, reason);
    awaitingCustomerResponse = false;
    pendingQuestionType = null;
    pendingQuestionText = null;
    questionAskedAt = null;
    responseReminderCount = 0;
  }

  async function processCompletedCustomerTranscript(
    transcript,
    beganWhileAssistantSpeaking = false
  ) {
    if (currentCallIsTerminal()) return;
    logCustomerResponseState(call.call_id, {
      ...currentQuestionState(),
      awaiting_customer_response: awaitingCustomerResponse,
      completed_transcript_received: true
    });

    if (!isMeaningfulCustomerTranscript(transcript)) {
      console.log(
        JSON.stringify({
          event: "background_noise_ignored",
          call_id: call.call_id,
          transcript: cleanText(transcript, 200)
        })
      );
      scheduleSilenceReminder();
      return;
    }

    const overlappedAssistant =
      beganWhileAssistantSpeaking ||
      assistantResponseActive ||
      responseCreatePending;

    if (
      overlappedAssistant &&
      !customerExplicitlyInterrupted(transcript)
    ) {
      console.log(
        JSON.stringify({
          event: "non_explicit_overlap_ignored",
          call_id: call.call_id,
          transcript: cleanText(transcript, 200)
        })
      );

      return;
    }

    if (
      assistantResponseActive &&
      customerExplicitlyInterrupted(transcript)
    ) {
      await stopAssistantForCustomer();
    }

    const pendingQuestionAcceptsAffirmative =
      directYesNoQuestion(pendingQuestionText) ||
      [
        "identity_confirmation",
        "confirmation",
        "application_link_permission"
      ].includes(String(pendingQuestionType || ""));

    if (isInterestRateQuestion(transcript)) {
      const returnToQuestion = awaitingCustomerResponse && pendingQuestionText
        ? pendingQuestionType === "intent_discovery"
          ? " Then continue with the next applicable scripted step using the caller's already stated reason and saved context. Do not ask why they are calling again, and do not list or restate the routing choices."
          : ` Then ask this still-pending question once and stop: ${JSON.stringify(pendingQuestionText)}`
        : "";
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: awaitingCustomerResponse,
        response: {
          output_modalities: ["audio"],
          instructions: `Say exactly: ${JSON.stringify(
            sessionCallPhase === "INBOUND" ? SBA_RATE_RESPONSE : interestRateResponse()
          )}.${returnToQuestion}`
        }
      });
      return;
    }

    if (!awaitingCustomerResponse) {
      requestAssistantResponse({ queueIfBusy: true });
      return;
    }

    if (
      savedInboundIntent(call) &&
      callerExplicitlyRequestsIntentOptions(transcript)
    ) {
      console.log(JSON.stringify({
        event: "inbound_intent_options_explicitly_requested",
        call_id: call.call_id,
        saved_intent: savedInboundIntent(call)
      }));
    }

    if (pendingQuestionType === "caller_city_state") {
      const location = parseInboundPurchaseLocation(transcript);
      if (location?.city && location?.state) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          { city: location.city, state: location.state }
        );
        if (saved?.success !== true) {
          throw new Error("The caller city and state could not be persisted.");
        }
        call = (await getCallById(call.call_id)) || call;
        await endLocalWaitingState("caller_city_state_saved");
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: true,
        response: {
          output_modalities: ["audio"],
          instructions:
            'Say exactly: "What city and state are you calling from?" Say nothing else.'
        }
      });
      return;
    }

    if (pendingQuestionType === "caller_phone_confirmation") {
      const explicitAnswer = normalizeExplicitYesNo(transcript);
      if (explicitAnswer === true) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          {
            phone_number: normalizePhone(activeCall.phone),
            phone_verified: true
          }
        );
        if (saved?.success !== true) {
          throw new Error("The caller phone confirmation could not be persisted.");
        }
        call = (await getCallById(call.call_id)) || call;
        await endLocalWaitingState("caller_phone_confirmed");
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
      if (explicitAnswer === false) {
        await endLocalWaitingState("caller_phone_rejected");
        requestAssistantResponse({
          queueIfBusy: true,
          response: {
            output_modalities: ["audio"],
            instructions:
              'Say exactly: "What is the correct callback number?" Say nothing else.'
          }
        });
        return;
      }
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: true,
        response: {
          output_modalities: ["audio"],
          instructions: 'Say exactly: "Is that number correct?" Say nothing else.'
        }
      });
      return;
    }

    if (pendingQuestionType === "caller_phone_correction") {
      const correctedPhone = normalizeInboundSpokenPhone(transcript);
      if (correctedPhone) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          { phone_number: correctedPhone, phone_verified: true }
        );
        if (saved?.success !== true) {
          throw new Error("The corrected callback number could not be persisted.");
        }
        call = (await getCallById(call.call_id)) || call;
        await endLocalWaitingState("caller_phone_corrected");
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: true,
        response: {
          output_modalities: ["audio"],
          instructions:
            'Say exactly: "Please say the correct callback number, including the area code." Say nothing else.'
        }
      });
      return;
    }

    if (pendingQuestionType === "caller_name") {
      const capturedName = normalizeInboundFullName(transcript);
      if (capturedName.first_name && capturedName.last_name) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const previousIntent = savedInboundIntent(activeCall);
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          {
            full_name: capturedName.full_name,
            first_name: capturedName.first_name,
            last_name: capturedName.last_name
          }
        );
        if (saved?.success !== true) {
          throw new Error("The caller name could not be persisted.");
        }
        call = (await getCallById(call.call_id)) || call;
        refreshIntentInstructionsIfChanged(previousIntent);
        await endLocalWaitingState("caller_name_saved");
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
    }

    if (pendingQuestionType === "caller_email") {
      const capturedEmail = normalizeInboundEmail(transcript);
      if (capturedEmail) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const previousIntent = savedInboundIntent(activeCall);
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          { email: capturedEmail }
        );
        if (saved?.success !== true) {
          throw new Error("The caller email could not be persisted.");
        }
        call = (await getCallById(call.call_id)) || call;
        refreshIntentInstructionsIfChanged(previousIntent);
        await endLocalWaitingState("caller_email_saved");
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
    }

    if (
      [
        "credit_score",
        "gross_monthly_revenue",
        "business_entity_type",
        "time_in_business",
        "funding_amount",
        "funding_use",
        "annual_household_income",
        "estimated_home_price",
        "homebuying_timeline",
        "tax_return_status",
        "employment_history"
      ].includes(String(pendingQuestionType || ""))
    ) {
      const recognizedValue = pendingQuestionType === "credit_score"
        ? (sessionCallPhase === "INBOUND"
          ? normalizeSbaCreditRange(transcript)
          : normalizeInboundCreditScore(transcript))
        : pendingQuestionType === "gross_monthly_revenue"
          ? normalizeSbaRevenueRange(transcript)
          : pendingQuestionType === "business_entity_type"
            ? normalizeSbaEntityType(transcript)
            : pendingQuestionType === "funding_amount"
              ? normalizeInboundAnnualIncome(transcript)
              : ["time_in_business", "funding_use"].includes(pendingQuestionType)
                ? cleanInboundContactValue(transcript, 500)
        : pendingQuestionType === "annual_household_income"
          ? normalizeInboundAnnualIncome(transcript)
          : pendingQuestionType === "estimated_home_price"
            ? normalizeInboundEstimatedHomePrice(transcript)
            : pendingQuestionType === "homebuying_timeline"
              ? cleanInboundContactValue(transcript, 500)
              : pendingQuestionType === "tax_return_status"
                ? (sessionCallPhase === "INBOUND"
                  ? cleanInboundContactValue(transcript, 500)
                  : normalizeInboundTaxReturnStatus(transcript))
                : cleanInboundContactValue(transcript, 500);
      if (recognizedValue) {
        const activeCall = (await getCallById(call.call_id)) || call;
        const toolArgs = pendingQuestionType === "credit_score"
          ? { estimated_credit_score: recognizedValue }
          : pendingQuestionType === "gross_monthly_revenue"
            ? { gross_monthly_revenue: recognizedValue }
            : pendingQuestionType === "business_entity_type"
              ? { business_entity_type: recognizedValue }
              : pendingQuestionType === "time_in_business"
                ? { time_in_business: recognizedValue }
                : pendingQuestionType === "funding_amount"
                  ? { funding_amount: recognizedValue }
                  : pendingQuestionType === "funding_use"
                    ? { funding_use: recognizedValue }
          : pendingQuestionType === "annual_household_income"
            ? { annual_household_income: recognizedValue }
            : pendingQuestionType === "estimated_home_price"
              ? { estimated_home_price: recognizedValue }
              : pendingQuestionType === "homebuying_timeline"
                ? { homebuying_timeline: recognizedValue }
                  : pendingQuestionType === "tax_return_status"
                    ? (sessionCallPhase === "INBOUND"
                      ? { tax_returns_status: recognizedValue }
                      : { two_year_tax_filing_status: recognizedValue })
                  : { two_year_employment_history: recognizedValue };
        const saved = await executeInboundTool(
          activeCall,
          "save_inbound_caller_context",
          toolArgs
        );
        if (saved?.success !== true) {
          throw new Error(
            `The ${pendingQuestionType} answer could not be persisted.`
          );
        }
        call = (await getCallById(call.call_id)) || call;
        refreshActiveRealtimeInstructions();
        await endLocalWaitingState(`${pendingQuestionType}_saved`);
        requestAssistantResponse({ queueIfBusy: true });
        return;
      }
    }

    if (sessionCallPhase !== "INBOUND" && pendingQuestionType === "purchase_area") {
      const confirmedPurchaseArea = exactMeaningfulPurchaseArea(transcript);
      const confirmedPurchaseLocation = parseInboundPurchaseLocation(transcript);
      if (
        !confirmedPurchaseArea ||
        (sessionCallPhase === "INBOUND" && !confirmedPurchaseLocation)
      ) {
        requestAssistantResponse({
          queueIfBusy: true,
          allowWhileAwaiting: true,
          preservePendingQuestion: true,
          response: {
            output_modalities: ["audio"],
            instructions:
              sessionCallPhase === "INBOUND"
                ? 'Say exactly: "What city and state would you like to purchase a home in?" Say nothing else.'
                : 'Say exactly: "What city or area would you like to purchase in?" Say nothing else.'
          }
        });
        return;
      }
      await mergeCallResult(call.call_id, {
        purchase_area: confirmedPurchaseArea,
        ...(confirmedPurchaseLocation
          ? {
              purchase_city: confirmedPurchaseLocation.city,
              purchase_state: confirmedPurchaseLocation.state
            }
          : {})
      });
      console.log(JSON.stringify({
        event: "purchase_area_confirmed",
        call_id: call.call_id,
        purchase_area: confirmedPurchaseArea,
        purchase_city: confirmedPurchaseLocation?.city || null,
        purchase_state: confirmedPurchaseLocation?.state || null
      }));
      call = (await getCallById(call.call_id)) || call;
      if (sessionCallPhase === "INBOUND" && INBOUND_MONDAY_CONNECTED) {
        await persistCallSessionToMonday(call.call_id, {
          forceFullState: false
        });
        call = (await getCallById(call.call_id)) || call;
      }

      refreshActiveRealtimeInstructions();
      await endLocalWaitingState("purchase_area_confirmed");
      if (sessionCallPhase === "INBOUND") {
        requestAssistantResponse({
          queueIfBusy: true,
          response: {
            output_modalities: ["audio"],
            instructions:
              'Say exactly: "Excellent. To make sure we head in the right direction, tell me which best describes why you\'re calling today: you\'ve already started the Readiness Assessment, you\'d like to know how to get started, you\'d like to know if you qualify or how much assistance may be available, or something else?" Say nothing else.'
          }
        });
        return;
      }
      requestAssistantResponse({
  queueIfBusy: true,
  response: {
    output_modalities: ["audio"],
    instructions:
      `Say exactly: "Well, that's everything for this call, and now you're one step closer to becoming a homeowner in ${confirmedPurchaseArea}." Then say exactly: "Your next step is to start the application so I can follow up with you about its status, explore potential program options, and review your preliminary debt-to-income ratio. How does that sound?" Say nothing else.`
  }
});
      return;
    }

    const professionalAnswerKey = ["has_realtor", "applied_with_lender"].includes(
      String(pendingQuestionType || "")
    )
      ? pendingQuestionType
      : null;
    if (sessionCallPhase !== "INBOUND" && professionalAnswerKey) {
      const explicitAnswer = normalizeExplicitYesNo(transcript);
      if (explicitAnswer !== null) {
        const storedAnswer = explicitAnswer ? "Yes" : "No";
        await mergeCallResult(call.call_id, {
          [professionalAnswerKey]: storedAnswer,
          ...(professionalAnswerKey === "applied_with_lender"
            ? { has_lender: storedAnswer }
            : {})
        });
        call = (await getCallById(call.call_id)) || call;
        await endLocalWaitingState("explicit_professional_yes_no_answer");
        requestAssistantResponse({
  queueIfBusy: true,
  response: {
    output_modalities: ["audio"],
    instructions:
      'Continue immediately with the next exact scripted question. Do not acknowledge, thank, summarize, comment on, or elaborate on the customer’s answer. Do not say "got it," "thanks," "thank you for that," "understood," "okay," "perfect," "excellent," "let me think," or any similar filler.'
  }
});
        return;
      }
    }

    if (customerRequestedMoreTime(transcript)) {
      await endLocalWaitingState("customer_requested_more_time");
      requestAssistantResponse({
        queueIfBusy: true,
        response: {
          output_modalities: ["audio"],
          instructions: 'Say exactly: "Of course—take your time." Say nothing else.'
        }
      });
      return;
    }


    if (
      lastWaitingPromptKind === "presence_reminder" &&
      presenceOnlyResponse(transcript)
    ) {
      const nextCount = 2;
      responseReminderCount = nextCount;
      await setResponseReminderCount(
        call.call_id,
        nextCount,
        currentQuestionState()
      );
      const intentDiscoveryPending =
        pendingQuestionType === "intent_discovery";
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: true,
        waitingPromptKind: "pending_repeat",
        response: {
          output_modalities: ["audio"],
          instructions: intentDiscoveryPending
            ? "Continue with the next applicable scripted step using the caller's already stated reason and saved context. Do not ask why they are calling again, and do not list or restate the routing choices."
            : `Repeat this pending question once, using the same meaning and no additional question: ${JSON.stringify(
                pendingQuestionText
              )}`
        }
      });
      return;
    }

    if (customerAskedSeparateQuestion(transcript)) {
      const returnToPendingQuestion =
        pendingQuestionType === "intent_discovery" &&
        !callerExplicitlyRequestsIntentOptions(transcript)
          ? "Then continue with the next applicable scripted step using the caller's already stated reason and saved context. Do not ask why they are calling again, and do not list or restate the routing choices."
          : `Then return naturally to this still-pending question, ask it once, and stop: ${JSON.stringify(
              pendingQuestionText
            )}`;
      requestAssistantResponse({
        queueIfBusy: true,
        allowWhileAwaiting: true,
        preservePendingQuestion: true,
        response: {
          output_modalities: ["audio"],
          instructions:
            `Answer the customer's separate question briefly and accurately. ${returnToPendingQuestion}`
        }
      });
      return;
    }

    if (
      pendingQuestionAcceptsAffirmative &&
      affirmativeCustomerResponse(transcript)
    ) {
      await endLocalWaitingState("affirmative_customer_answer");
      requestAssistantResponse({ queueIfBusy: true });
      return;
    }

    if (briefListeningAcknowledgement(transcript)) {
      if (directYesNoQuestion(pendingQuestionText)) {
        requestAssistantResponse({
          queueIfBusy: true,
          allowWhileAwaiting: true,
          preservePendingQuestion: true,
          response: {
            output_modalities: ["audio"],
            instructions: 'Say exactly: "Was that a yes?" Say nothing else.'
          }
        });
      } else {
        scheduleSilenceReminder();
      }
      return;
    }

    await endLocalWaitingState("meaningful_completed_customer_answer");
    if (suspendedQuestionState) {
      const suspended = suspendedQuestionState;
      suspendedQuestionState = null;
      const resumeInstruction =
        suspended.pending_question_type === "intent_discovery"
          ? "Then continue with the next applicable scripted step using the caller's already stated reason and saved context. Do not ask why they are calling again, and do not list or restate the routing choices."
          : `Then return to this previously pending question, ask it once, and stop: ${JSON.stringify(
              suspended.pending_question_text
            )}`;
      requestAssistantResponse({
        queueIfBusy: true,
        response: {
          output_modalities: ["audio"],
          instructions:
            `Continue the identity-confirmed introduction briefly without asking another question. ${resumeInstruction}`
        }
      });
    } else {
      requestAssistantResponse({ queueIfBusy: true });
    }
  }

  async function stopAssistantForCustomer() {
    if (!assistantResponseActive) return;
    sendToOpenAI({ type: "response.cancel" });
    await handleInterruption();
  }

  async function physicallyEndActiveTwilioCall(reason) {
    if (
      !normalEndRequested ||
      finalHangupInProgress ||
      finalHangupCompleted ||
      finalHangupAttemptCount >= 3
    ) {
      return false;
    }

    finalHangupInProgress = true;

    try {
      const refreshedCall =
        (await getCallById(call.call_id)) || call;
      const twilioCallSid =
        activeTwilioCallSid ||
        String(
          refreshedCall?.twilio_call_sid ||
          call?.twilio_call_sid ||
          ""
        ).trim();

      if (!twilioCallSid) {
        throw new Error(
          "No live Twilio Call SID is available."
        );
      }

      let updatedCall = null;
      let lastUpdateError = null;
      while (finalHangupAttemptCount < 3) {
  finalHangupAttemptCount += 1;

  try {
    updatedCall = await twilioClient
      .calls(twilioCallSid)
      .update({
        status: "completed"
      });

    lastUpdateError = null;
    break;
  } catch (error) {
    lastUpdateError = error;

    if (finalHangupAttemptCount < 3) {
      await sleep(finalHangupAttemptCount === 1 ? 500 : 1000);
    }
  }
}
      if (lastUpdateError) throw lastUpdateError;

      finalHangupCompleted = true;
      if (finalHangupFallbackTimer) {
        clearTimeout(finalHangupFallbackTimer);
        finalHangupFallbackTimer = null;
      }
      if (finalAbsoluteHangupTimer) {
        clearTimeout(finalAbsoluteHangupTimer);
        finalAbsoluteHangupTimer = null;
      }
      await appendAction(call.call_id, {
        action: "twilio_physical_hangup",
        success: true,
        reason,
        twilio_call_sid: twilioCallSid,
        twilio_status: updatedCall?.status || "completed"
      });

      console.log(JSON.stringify({
        event: "twilio_physical_hangup",
        call_id: call.call_id,
        twilio_call_sid: twilioCallSid,
        twilio_status: updatedCall?.status || null,
        reason,
        success: true
      }));

      return true;
    } catch (error) {
      const safeError =
        cleanText(error.message, 1000) ||
        "Twilio physical hangup failed.";

      await appendAction(call.call_id, {
        action: "twilio_physical_hangup",
        success: false,
        reason,
        error: safeError
      });

      console.error(JSON.stringify({
        event: "twilio_physical_hangup",
        call_id: call.call_id,
        reason,
        success: false,
        error: safeError
      }));

      return false;
    } finally {
      finalHangupInProgress = false;
    }
  }

  async function handleToolCall(name, toolCallId, argumentText) {
    if (!call || !toolCallId || handledToolCalls.has(toolCallId)) return;
    if (currentCallIsTerminal()) return;
    handledToolCalls.add(toolCallId);

    let args = {};
    try {
      args = argumentText ? JSON.parse(argumentText) : {};
    } catch {
      args = {};
    }

    let output;
    let previousInboundIntent = null;
    try {
      const refreshed = await getCallById(call.call_id);
      const activeCall = refreshed || call;
      previousInboundIntent = savedInboundIntent(activeCall);
      output = INBOUND_TOOL_NAMES.has(name)
        ? await executeInboundTool(activeCall, name, args)
        : await routeIntent({
            toolName: name,
            args,
            call: activeCall,
            execute: (routedCall, toolName, toolArgs) =>
              executeDougTool(
                routedCall,
                toolName,
                toolArgs,
                sessionCallPhase
              )
          });
    } catch (error) {
      console.error(`Daisy tool ${name} failed for ${call.call_id}:`, error);
      output = {
        success: false,
        intent: "UNKNOWN_INTENT",
        customer_safe_message: null,
        data: {},
        error: { code: "ACTION_FAILED", retryable: true }
      };
    }

    if (name === "save_inbound_caller_context" && output?.success === true) {
      call = (await getCallById(call.call_id)) || call;
      refreshIntentInstructionsIfChanged(previousInboundIntent);
    }

    const completeCallSucceeded =
      name === "complete_call" &&
      output?.success === true &&
      output?.intent === "COMPLETE_CALL" &&
      output?.error === null;
    const terminalActionSucceeded = completeCallSucceeded;

    if (terminalActionSucceeded) {
      normalCompletionPersistenceInProgress = true;
      try {
        await pool.query(
        `
          UPDATE ai_calls
          SET current_state = CASE
              WHEN current_state IN ('reconnect_pending', 'reconnect_in_progress')
                THEN 'completed'
              ELSE current_state
            END,
            last_error = NULL,
            result = result
              - 'unexpected_disconnect_at'
              - 'unexpected_disconnect_reconnect_attempted'
              - 'unexpected_disconnect_reconnect_completed',
            updated_at = NOW()
          WHERE call_id = $1
        `,
        [call.call_id]
      );
      await pool.query(
        `
          UPDATE call_attempts
          SET technical_status = 'canceled', completed_at = NOW(),
              cancellation_reason = 'normal_call_completion', updated_at = NOW()
          WHERE call_id = $1
            AND attempt_id IS DISTINCT FROM $2
            AND attempt_type = 'disconnect_reconnect'
            AND completed_at IS NULL
            AND technical_status IN ('pending', 'created')
        `,
        [call.call_id, call.last_attempt_id]
      );
      await mergeCallResult(call.call_id, {
        normal_completion_recorded: true,
        final_hangup_requested: true,
        completion_reason: "normal_completion",
        terminal_action: name,
        normal_completion_recorded_at: new Date().toISOString(),
        call_started_at: call.started_at || call.answered_at || call.created_at,
        call_ended_at: new Date().toISOString(),
        transcript_url: `${PUBLIC_BASE_URL}/api/v1/calls/${encodeURIComponent(call.call_id)}`,
        recording_url: cleanText(
          call.result?.recording_url || call.payload?.recording_url,
          1000
        )
      });
      call = (await getCallById(call.call_id)) || call;
      const followUpRecord = call.result?.inbound_follow_up || null;
      const followUpDeclined = followUpRecord?.follow_up_declined === true;
      const followUpScheduled = Boolean(
        followUpRecord?.follow_up_at && !followUpDeclined
      );
      const transferredToOutbound = call.result?.transferred_to_outbound === true;
      const inboundIntent = call.intent || call.result?.inbound_intent;
      const finalInboundOutcome = call.result?.call_outcome || call.outcome;
      try {
        await persistFinalInboundSession({
          call_id: call.call_id,
          intent: inboundIntent,
          outcome: finalInboundOutcome,
          summary: call.summary,
          next_action: call.next_action,
          inbound_status: transferredToOutbound
            ? "Transferred to Outbound"
            : followUpScheduled
              ? "Follow-Up Scheduled"
              : followUpDeclined
                ? "Follow-Up Declined"
                : "Closed",
          follow_up_needed: followUpScheduled || transferredToOutbound ? "Yes" : "No",
          call_status: finalInboundOutcome,
          date_called: call.started_at || call.created_at,
          group_id: transferredToOutbound
            ? INBOUND_MONDAY.groups.transferredToOutbound
            : followUpDeclined
              ? INBOUND_MONDAY.groups.closed
              : followUpScheduled && inboundIntent === "EXISTING_APPLICATION_FOLLOWUP"
                ? INBOUND_MONDAY.groups.existingApplicantFollowUp
                : followUpScheduled
                  ? null
                  : INBOUND_MONDAY.groups.closed
        });
      } catch (error) {
        inboundLog("[INBOUND]", "completion_summary_save_failed", {
          call_id: call.call_id,
          error: cleanText(error.message, 300)
        });
      }
        beginNormalCallTermination(name);
        normalCompletionRecorded = true;
      } finally {
        normalCompletionPersistenceInProgress = false;
      }
    }

    if (
      name === "save_call_progress" &&
      output?.success === true &&
      exactMeaningfulPurchaseArea(args?.answers?.purchase_area)
    ) {
      call = (await getCallById(call.call_id)) || call;
      refreshActiveRealtimeInstructions();
    }

    sendToOpenAI({
      type: "conversation.item.create",
      item: {
        type: "function_call_output",
        call_id: toolCallId,
        output: JSON.stringify(output)
      }
    });
    if (terminalActionSucceeded) {
      const finalClosing = buildInboundFinalClosing(call);
      requestAssistantResponse({
        queueIfBusy: true,
        allowTerminalClosing: true,
        response: {
          output_modalities: ["audio"],
          instructions: `Say exactly: "${finalClosing}" Then stop speaking. Do not ask a question. Do not wait for another response. Do not call another tool. Do not add any other sentence.`
        }
      });
    } else {
      requestAssistantResponse({ queueIfBusy: true });
    }
  }

  function connectToOpenAI() {
    const url = `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(
      OPENAI_REALTIME_MODEL
    )}`;

    inboundLog("[OPENAI_WS]", "realtime_connection_attempted", {
      call_id: call?.call_id || null,
      model: OPENAI_REALTIME_MODEL,
      url
    });

    try {
      openaiSocket = new WebSocket(url, {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "OpenAI-Safety-Identifier": safetyIdentifier(call)
        }
      });
    } catch (error) {
      console.error("[OPENAI_WS] Realtime initialization failed:", error);
      if (twilioSocket.readyState === WebSocket.OPEN) {
        twilioSocket.close(1011, "OpenAI initialization failed");
      }
      return;
    }

    openaiSocket.on("open", () => {
      inboundLog("[OPENAI_WS]", "realtime_connected", {
        call_id: call?.call_id || null,
        model: OPENAI_REALTIME_MODEL
      });
      const realtimeSession = buildRealtimeSession({
        model: OPENAI_REALTIME_MODEL,
        voice: OPENAI_VOICE,
        transcriptionModel: OPENAI_TRANSCRIPTION_MODEL,
        instructions: buildDaisyInboundInstructions(call),
        tools: REALTIME_TOOLS
      });

      realtimeSession.audio.input.turn_detection = {
        ...(realtimeSession.audio.input.turn_detection || {}),
        type: "server_vad",
        threshold: 0.75,
        prefix_padding_ms: 300,
        silence_duration_ms: 700,
        create_response: false,
        interrupt_response: false
      };

      sendToOpenAI({
        type: "session.update",
        session: realtimeSession
      });

      for (const audio of pendingAudio) {
        sendToOpenAI({ type: "input_audio_buffer.append", audio });
      }
      pendingAudio = [];
    });

    openaiSocket.on("message", async (rawMessage) => {
      try {
        let event;
        try {
          event = JSON.parse(rawMessage.toString());
        } catch {
          return;
        }

        if (!initialGreetingStarted) {
          initialGreetingStarted = true;
          requestAssistantResponse({
            response: {
              output_modalities: ["audio"],
              instructions:
                `Say exactly: ${JSON.stringify(SBA_OPENING)} Say nothing else until the caller answers.`
            }
          });
        }

        if (event.type === "response.created") {
          responseCreatePending = false;
          assistantResponseActive = true;
          assistantResponseFinished = false;
          if (
            normalEndRequested &&
            finalClosingRequested &&
            !finalClosingResponseId
          ) {
            finalClosingResponseId = cleanText(
              event.response?.id || event.response_id,
              160
            ) || "";
          }
          activeResponsePreservesQuestion = pendingResponsePreservesQuestion;
          pendingResponsePreservesQuestion = false;
          activeResponseIncludesAssistanceMaximum = false;
          activeResponseWaitingPromptKind = pendingResponseWaitingPromptKind;
          pendingResponseWaitingPromptKind = null;
          assistantTranscriptBuffer = "";
          assistantTranscriptSaved = false;
          questionCapturedForResponse = false;
          assistantAudioQueuedForResponse = false;
          return;
        }

        if (event.type === "response.output_audio_transcript.delta") {
          assistantTranscriptBuffer += event.delta || "";
          const includesAssistanceMaximum =
            sessionCallPhase !== "INBOUND" &&
            /\bup to\s+(?:\$\s*)?(?:100(?:,\s*|\s*)000|one hundred thousand)(?:\s+dollars?)?\b/i.test(
              assistantTranscriptBuffer
            );
          const duplicateAssistanceMaximum =
            includesAssistanceMaximum &&
            assistanceMaximumMentioned &&
            !activeResponseIncludesAssistanceMaximum;
          if (
            includesAssistanceMaximum &&
            !assistanceMaximumMentioned &&
            !activeResponseIncludesAssistanceMaximum
          ) {
            activeResponseIncludesAssistanceMaximum = true;
            assistanceMaximumMentioned = true;
            await mergeCallResult(call.call_id, {
              assistance_maximum_mentioned: true
            });
            call = {
              ...call,
              result: {
                ...(call.result || {}),
                assistance_maximum_mentioned: true
              }
            };
          }
          const duplicatePurchaseLocationQuestion =
            sessionCallPhase !== "INBOUND" &&
            Boolean(inboundPurchaseLocationLabel(call)) &&
            /\bwhat city and state\b.{0,100}\bpurchase\b/i.test(
              assistantTranscriptBuffer
            );
          const compliance = duplicatePurchaseLocationQuestion
            ? {
                allowed: false,
                code: "DUPLICATE_PURCHASE_LOCATION_QUESTION",
                replacement: null
              }
            : duplicateAssistanceMaximum
              ? {
                  allowed: false,
                  code: "DUPLICATE_ASSISTANCE_MAXIMUM",
                  replacement: null
                }
            : guardAssistantOutput(assistantTranscriptBuffer, sessionCallPhase);
          if (!compliance.allowed && !complianceRecoveryActive) {
            complianceRecoveryActive = true;
            sendToOpenAI({ type: "response.cancel" });
            if (streamSid) sendToTwilio({ event: "clear", streamSid });
            pendingMarkNames.clear();
            queuedResponseOptions = null;
            if (
              compliance.code === "UNSCRIPTED_FILLER" ||
              compliance.code === "DUPLICATE_PURCHASE_LOCATION_QUESTION" ||
              compliance.code === "DUPLICATE_ASSISTANCE_MAXIMUM"
            ) {
              const originalOptions = activeResponseRequestOptions || {};
              const originalResponse = originalOptions.response || {};
              const duplicateLocationBlocked =
                compliance.code === "DUPLICATE_PURCHASE_LOCATION_QUESTION";
              const duplicateAssistanceMaximumBlocked =
                compliance.code === "DUPLICATE_ASSISTANCE_MAXIMUM";
              requestAssistantResponse({
                ...originalOptions,
                queueIfBusy: true,
                response: {
                  ...originalResponse,
                  output_modalities:
                    originalResponse.output_modalities || ["audio"],
                  instructions: duplicateLocationBlocked
                    ? "The caller's purchase city and state are already saved. Do not ask for either value again. Continue from the current conversation stage with only the next exact approved scripted line."
                    : duplicateAssistanceMaximumBlocked
                      ? "The approved assistance maximum has already been stated during this call. Do not repeat the amount. Continue from the current conversation stage with only the next exact approved scripted line."
                      : [
                        originalResponse.instructions,
                        "Retry the same response using the existing script exactly. Start directly with the required scripted words. Do not add any preface, transition, internal thought, planning narration, or new offer. Do not change, expand, summarize, or reinterpret the approved talk track."
                        ].filter(Boolean).join("\n")
                }
              });
            } else {
              requestAssistantResponse({
                queueIfBusy: true,
                response: {
                  output_modalities: ["audio"],
                  instructions:
                    `Say exactly: ${JSON.stringify(compliance.replacement)} Say nothing else.`
                }
              });
            }
            await appendAction(call.call_id, {
              action: "compliance_output_intercepted",
              success: true,
              policy_code: compliance.code
            });
            return;
          }
          return;
        }

        if (
          event.type === "response.output_item.added" ||
          event.type === "response.output_item.created"
        ) {
          if (event.item && event.item.id) lastAssistantItemId = event.item.id;
          return;
        }

        if (
          event.type === "response.output_audio.delta" ||
          event.type === "response.audio.delta"
        ) {
          if (!event.delta || !streamSid) return;
          if (responseStartTimestamp === null) {
            responseStartTimestamp = latestMediaTimestamp;
          }
          assistantAudioQueuedForResponse = true;
          sendToTwilio({
            event: "media",
            streamSid,
            media: { payload: event.delta }
          });
          sendMark();
          return;
        }

        if (event.type === "input_audio_buffer.speech_started") {
          const assistantWasSpeaking =
            assistantResponseActive || responseCreatePending;

          speechCandidateStartedAt = Date.now();
          speechCandidateConfirmed = false;
          speechCandidateWhileAssistantSpeaking =
            assistantWasSpeaking;

          if (sustainedSpeechTimer) {
            clearTimeout(sustainedSpeechTimer);
          }

          sustainedSpeechTimer = setTimeout(() => {
            sustainedSpeechTimer = null;

            if (
              closed ||
              !speechCandidateStartedAt ||
              speechCandidateConfirmed
            ) {
              return;
            }

            speechCandidateConfirmed = true;
            customerSpeaking = true;
            customerTurnBeganWhileAssistantSpeaking =
              customerTurnBeganWhileAssistantSpeaking ||
              speechCandidateWhileAssistantSpeaking;

            cancelSilenceReminder();

            if (customerTranscriptDebounceTimer) {
              clearTimeout(customerTranscriptDebounceTimer);
              customerTranscriptDebounceTimer = null;
            }

            logCustomerResponseState(call.call_id, {
              ...currentQuestionState(),
              awaiting_customer_response:
                awaitingCustomerResponse,
              customer_speech_detected: true
            });

          }, Math.max(
            DAISY_SPEECH_CONFIRM_MS,
            Number(
              REALTIME_DEFAULTS.meaningfulInterruptionMs || 0
            )
          ));

          return;
        }

        if (event.type === "input_audio_buffer.speech_stopped") {
          const candidateDurationMs = speechCandidateStartedAt
            ? Date.now() - speechCandidateStartedAt
            : 0;

          const wasConfirmedSpeech = speechCandidateConfirmed;

          if (sustainedSpeechTimer) {
            clearTimeout(sustainedSpeechTimer);
          }

          sustainedSpeechTimer = null;
          speechCandidateStartedAt = 0;
          speechCandidateConfirmed = false;
          speechCandidateWhileAssistantSpeaking = false;
          customerSpeaking = false;

          if (!wasConfirmedSpeech) {
            console.log(
              JSON.stringify({
                event: "short_vad_noise_ignored",
                call_id: call.call_id,
                duration_ms: candidateDurationMs
              })
            );

            if (!pendingMarkNames.size) {
              scheduleSilenceReminder();
            }
          }

          return;
        }

        if (event.type === "response.output_audio_transcript.done") {
          if (!assistantTranscriptBuffer && event.transcript) {
            assistantTranscriptBuffer = event.transcript;
          }
          if (!assistantTranscriptSaved) {
            await appendTranscript(call.call_id, "assistant", event.transcript);
            assistantTranscriptSaved = true;
          }
          if (!questionCapturedForResponse) {
            await captureAssistantQuestion(event.transcript);
            questionCapturedForResponse = Boolean(
              extractPrimaryQuestion(event.transcript)
            );
          }
          return;
        }

        if (
          event.type ===
          "conversation.item.input_audio_transcription.completed"
        ) {
          await appendTranscript(call.call_id, "lead", event.transcript);
          const turnKey = String(
            event.item_id ||
              stableHash(`${event.transcript}:${event.audio_end_ms || ""}`)
          );
          if (handledUserTurns.has(turnKey)) return;
          handledUserTurns.add(turnKey);
          pendingCustomerTranscripts.push(cleanText(event.transcript, 8000));
          pendingTranscriptWasWhileAssistantSpeaking =
            pendingTranscriptWasWhileAssistantSpeaking ||
            customerTurnBeganWhileAssistantSpeaking;
          customerTurnBeganWhileAssistantSpeaking = false;
          if (customerTranscriptDebounceTimer) {
            clearTimeout(customerTranscriptDebounceTimer);
          }
          customerTranscriptDebounceTimer = setTimeout(() => {
            customerTranscriptDebounceTimer = null;
            const completedTranscript = pendingCustomerTranscripts
              .filter(Boolean)
              .join(" ");
            pendingCustomerTranscripts = [];
            const beganWhileAssistantSpeaking =
              pendingTranscriptWasWhileAssistantSpeaking;
            pendingTranscriptWasWhileAssistantSpeaking = false;
            void processCompletedCustomerTranscript(
              completedTranscript,
              beganWhileAssistantSpeaking
            ).catch(
              (error) => {
                console.error("Daisy completed transcript handling failed:", error);
              }
            );
          }, Math.max(
            DAISY_MIN_TRANSCRIPT_SETTLE_MS,
            semanticTurnDelay(event.transcript)
          ));
          return;
        }

        if (event.type === "response.function_call_arguments.done") {
          await handleToolCall(
            event.name,
            event.call_id,
            event.arguments || "{}"
          );
          return;
        }

        if (
          event.type === "response.output_item.done" &&
          event.item &&
          event.item.type === "function_call"
        ) {
          await handleToolCall(
            event.item.name,
            event.item.call_id,
            event.item.arguments || "{}"
          );
          return;
        }

        if (event.type === "response.done") {
          const completedResponseId = cleanText(
            event.response?.id || event.response_id,
            160
          ) || "";
          assistantResponseActive = false;
          responseCreatePending = false;
          assistantResponseFinished = true;
          if (!assistantTranscriptSaved && assistantTranscriptBuffer) {
            await appendTranscript(
              call.call_id,
              "assistant",
              assistantTranscriptBuffer
            );
            assistantTranscriptSaved = true;
          }
          if (!questionCapturedForResponse && assistantTranscriptBuffer) {
            await captureAssistantQuestion(assistantTranscriptBuffer);
            questionCapturedForResponse = Boolean(
              extractPrimaryQuestion(assistantTranscriptBuffer)
            );
          }
          for (const item of event.response?.output || []) {
            if (item && item.type === "function_call") {
              await handleToolCall(
                item.name,
                item.call_id,
                item.arguments || "{}"
              );
            }
          }
          if (
            normalEndRequested &&
            completedResponseId &&
            completedResponseId === finalClosingResponseId &&
            assistantAudioQueuedForResponse &&
            !finalPlaybackMarkName &&
            !finalHangupCompleted
          ) {
            sendFinalHangupMark();
          }
          activeResponsePreservesQuestion = false;
          lastWaitingPromptKind = activeResponseWaitingPromptKind;
          activeResponseWaitingPromptKind = null;
          if (queuedResponseOptions) {
            const options = queuedResponseOptions;
            queuedResponseOptions = null;
            requestAssistantResponse(options);
          }
          complianceRecoveryActive = false;
          if (!normalEndRequested) scheduleSilenceReminder();
          return;
        }

        if (event.type === "error") {
          const message =
            event.error?.message || event.message || "OpenAI Realtime error";
          console.error(
            `OpenAI Realtime error for ${call.call_id}:`,
            message
          );
          await updateCallStatus(call.call_id, "in-progress", {
            last_error: message
          });
        }
      } catch (error) {
        console.error(
          `OpenAI event handler failed for ${call ? call.call_id : "unknown"}:`,
          error
        );
      }
    });

    openaiSocket.on("error", async (error) => {
      try {
        console.error("[OPENAI_WS] Realtime socket error:", {
          call_id: call?.call_id || null,
          message: error.message,
          stack: error.stack
        });
        await updateCallStatus(call.call_id, "in-progress", {
          last_error: error.message
        });
      } catch (updateError) {
        console.error("Failed to save OpenAI socket error:", updateError);
      }
    });

    openaiSocket.on("close", (code, reason) => {
      inboundLog("[OPENAI_WS]", "realtime_socket_closed", {
        call_id: call?.call_id || null,
        code,
        reason: String(reason || "")
      });
      if (!closed && twilioSocket.readyState === WebSocket.OPEN) {
        twilioSocket.close();
      }
    });
  }

  twilioSocket.on("message", async (rawMessage) => {
    try {
      let message;
      try {
        message = JSON.parse(rawMessage.toString());
      } catch {
        return;
      }

      if (message.event === "start") {
        const parameters = message.start?.customParameters || {};
        const callId = cleanText(parameters.call_id, 100);
        const token = cleanText(parameters.stream_token, 160);
        activeTwilioCallSid = String(
          message?.start?.callSid || parameters.twilio_call_sid || ""
        ).trim();
        activeTwilioStreamSid = String(
          message?.start?.streamSid || message?.streamSid || ""
        ).trim();
        streamSid = activeTwilioStreamSid;

        inboundLog("[TWILIO_WS]", "start_event_received", {
          call_id: callId || null,
          custom_parameter_names: Object.keys(parameters),
          has_stream_token: Boolean(token)
        });
        inboundLog("[TWILIO_WS]", "stream_sid_received", {
          call_id: callId || null,
          twilio_stream_sid: activeTwilioStreamSid || null
        });
        inboundLog("[TWILIO_WS]", "call_sid_received", {
          call_id: callId || null,
          twilio_call_sid: activeTwilioCallSid || null
        });

        if (!callId || !token) {
          inboundLog("[TWILIO_WS]", "required_parameters_missing", {
            has_call_id: Boolean(callId),
            has_stream_token: Boolean(token)
          });
        }

        call = await validateCallToken(callId, token);
        if (!call) {
          inboundLog("[TWILIO_WS]", "call_session_rejected", {
            call_id: callId || null,
            reason: "ai_calls record or stream token not found"
          });
          twilioSocket.close(1008, "Invalid stream token");
          return;
        }

        inboundLog("[TWILIO_WS]", "call_session_validated", {
          call_id: call.call_id
        });

        const activeAttempt = call.last_attempt_id
          ? await getAttemptById(call.last_attempt_id)
          : null;
        lockSessionCallPhase(call, activeAttempt);
        console.log(JSON.stringify({
          event: "media_session_phase_locked",
          call_id: call.call_id,
          session_call_phase: sessionCallPhase
        }));

        console.log(JSON.stringify({
          event: "twilio_live_call_captured",
          call_id: call?.call_id || null,
          twilio_call_sid: activeTwilioCallSid || null,
          twilio_stream_sid: activeTwilioStreamSid || null
        }));
        await updateCallStatus(
          call.call_id,
          "in-progress",
          activeTwilioCallSid
            ? { twilio_call_sid: activeTwilioCallSid }
            : {}
        );
        call = await getCallById(call.call_id);
        awaitingCustomerResponse = call.awaiting_customer_response === true;
        pendingQuestionType = call.pending_question_type || null;
        pendingQuestionText = call.pending_question_text || null;
        questionAskedAt = call.question_asked_at
          ? new Date(call.question_asked_at).toISOString()
          : null;
        responseReminderCount = Number(call.response_reminder_count || 0);
        if (awaitingCustomerResponse && pendingQuestionText) {
          suspendedQuestionState = currentQuestionState();
          awaitingCustomerResponse = false;
          pendingQuestionType = null;
          pendingQuestionText = null;
          questionAskedAt = null;
          responseReminderCount = 0;
        }
        connectToOpenAI();
        return;
      }

      if (message.event === "mark") {
        const returnedMarkName =
          String(message?.mark?.name || "").trim();
        if (returnedMarkName) pendingMarkNames.delete(returnedMarkName);
        if (
          returnedMarkName &&
          returnedMarkName === finalPlaybackMarkName
        ) {
          if (finalHangupFallbackTimer) {
            clearTimeout(finalHangupFallbackTimer);
            finalHangupFallbackTimer = null;
          }
          console.log(JSON.stringify({
            event: "final_hangup_mark_received",
            call_id: call.call_id,
            mark_name: returnedMarkName
          }));
          setTimeout(() => {
            void physicallyEndActiveTwilioCall(
              "final_audio_playback_complete"
            );
          }, 1200);
        }
        if (!pendingMarkNames.size) {
          scheduleSilenceReminder();
        }
        return;
      }

      if (message.event === "media") {
        const payload = message.media?.payload;
        latestMediaTimestamp = Number(message.media?.timestamp || 0);
        if (!payload || currentCallIsTerminal()) return;

        if (
          !sendToOpenAI({
            type: "input_audio_buffer.append",
            audio: payload
          })
        ) {
          if (pendingAudio.length < 200) pendingAudio.push(payload);
        }
        return;
      }

      if (message.event === "stop") {
        closed = true;
        cancelSilenceReminder();
        if (customerTranscriptDebounceTimer) {
          clearTimeout(customerTranscriptDebounceTimer);
        }
        if (
          call &&
          !normalEndRequested &&
          !finalClosingRequested &&
          !finalPlaybackMarkName &&
          !finalHangupInProgress &&
          !normalCompletionPersistenceInProgress &&
          !normalCompletionRecorded &&
          !finalHangupCompleted
        ) {
          try {
            await persistBeforeInboundMediaRelease("twilio_media_stop");
          } catch (error) {
            inboundLog("[INBOUND]", "media_disconnect_persistence_failed", {
              call_id: call.call_id,
              reason: "twilio_media_stop",
              error: cleanText(error.message, 300)
            });
          }
          void reconnectAfterUnexpectedDisconnect(call.call_id).catch((error) => {
            console.error("Failed to reconnect after disconnect:", error);
          });
        } else if (call) {
          console.log(JSON.stringify({
            event: "unexpected_reconnect_skipped",
            call_id: call.call_id,
            reason: "normal_terminal_call"
          }));
        }
        if (openaiSocket && openaiSocket.readyState === WebSocket.OPEN) {
          openaiSocket.close();
        }
      }
    } catch (error) {
      console.error("[TWILIO_WS] Media message handler failed:", error);
      if (call) {
        try {
          await updateCallStatus(call.call_id, "in-progress", {
            last_error: error.message
          });
        } catch (updateError) {
          console.error("Failed to save Twilio handler error:", updateError);
        }
      }
    }
  });

  twilioSocket.on("close", async (code, reason) => {
    inboundLog("[TWILIO_WS]", "twilio_socket_closed", {
      call_id: call?.call_id || null,
      twilio_call_sid: activeTwilioCallSid || null,
      twilio_stream_sid: activeTwilioStreamSid || null,
      code,
      reason: String(reason || "")
    });
    closed = true;
    cancelSilenceReminder();
    if (customerTranscriptDebounceTimer) {
      clearTimeout(customerTranscriptDebounceTimer);
    }
    if (sustainedSpeechTimer) clearTimeout(sustainedSpeechTimer);
    if (
      call &&
      !normalEndRequested &&
      !finalClosingRequested &&
      !finalPlaybackMarkName &&
      !finalHangupInProgress &&
      !normalCompletionPersistenceInProgress &&
      !normalCompletionRecorded &&
      !finalHangupCompleted
    ) {
      try {
        await persistBeforeInboundMediaRelease("twilio_socket_close");
      } catch (error) {
        inboundLog("[INBOUND]", "media_disconnect_persistence_failed", {
          call_id: call.call_id,
          reason: "twilio_socket_close",
          error: cleanText(error.message, 300)
        });
      }
      void reconnectAfterUnexpectedDisconnect(call.call_id).catch((error) => {
        console.error("Failed to reconnect after disconnect:", error);
      });
    } else if (call) {
      console.log(JSON.stringify({
        event: "unexpected_reconnect_skipped",
        call_id: call.call_id,
        reason: "normal_terminal_call"
      }));
    }
    if (openaiSocket && openaiSocket.readyState === WebSocket.OPEN) {
      openaiSocket.close();
    }
  });

  twilioSocket.on("error", (error) => {
    console.error("[TWILIO_WS] Twilio media socket error:", {
      call_id: call?.call_id || null,
      twilio_call_sid: activeTwilioCallSid || null,
      twilio_stream_sid: activeTwilioStreamSid || null,
      message: error.message,
      stack: error.stack
    });
  });
});

server.on("upgrade", (request, socket, head) => {
  inboundLog("[TWILIO_WS]", "websocket_upgrade_received", {
    request_url: request.url || null,
    host: request.headers.host || null,
    upgrade: request.headers.upgrade || null
  });
  socket.on("error", (error) => {
    console.error("[TWILIO_WS] Upgrade socket error:", {
      request_url: request.url || null,
      message: error.message,
      stack: error.stack
    });
  });
  try {
    const requestUrl = new URL(
      request.url,
      `http://${request.headers.host || "localhost"}`
    );
    inboundLog("[TWILIO_WS]", "websocket_upgrade_pathname", {
      pathname: requestUrl.pathname
    });

    if (requestUrl.pathname !== "/api/v1/twilio/media") {
      inboundLog("[TWILIO_WS]", "websocket_upgrade_rejected", {
        pathname: requestUrl.pathname,
        expected_pathname: "/api/v1/twilio/media"
      });
      socket.destroy();
      return;
    }

    inboundLog("[TWILIO_WS]", "websocket_upgrade_accepted", {
      pathname: requestUrl.pathname
    });
    mediaServer.handleUpgrade(request, socket, head, (websocket) => {
      mediaServer.emit("connection", websocket, request);
    });
  } catch (error) {
    console.error("[TWILIO_WS] WebSocket upgrade failed:", {
      request_url: request.url || null,
      message: error.message,
      stack: error.stack
    });
    socket.destroy();
  }
});

app.use((error, req, res, next) => {
  const statusCode =
    error instanceof HttpError || error instanceof SchedulingError
      ? error.statusCode
      : 500;

  if (statusCode >= 500) {
    console.error("[INBOUND] Request failed:", error);
  }

  if (res.headersSent) {
    return next(error);
  }

  res.status(statusCode).json({
    success: false,
    error: statusCode >= 500 ? "Internal server error." : error.message,
    ...(error instanceof SchedulingError ? { code: error.code } : {})
  });
});

async function start() {
  try {
    await initializeDatabase();

    server.listen(PORT, "0.0.0.0", () => {
      inboundLog("[INBOUND]", "service_started", {
        port: PORT,
        service: "helux-ai-inbound-calls",
        mode: "inbound-demo",
        agent_version: DOUG_CONFIG.agentVersion,
        realtime_model: OPENAI_REALTIME_MODEL,
        voice: OPENAI_VOICE,
        outbound_calls_enabled: false,
        monday_configured: INBOUND_MONDAY_CONNECTED
      });
    });

    if (INBOUND_MONDAY_CONNECTED) {
      void loadInboundMondayMetadata(true)
        .then((metadata) => {
          inboundLog("[MONDAY]", "connected", {
            board_id: metadata.id,
            board_name: metadata.name
          });
        })
        .catch((error) => {
          inboundLog("[MONDAY]", "metadata_warmup_failed", {
            error: cleanText(error.message, 300)
          });
        });
    } else {
      inboundLog("[MONDAY]", "not_configured", {
        caller_remains_available: true
      });
    }
  } catch (error) {
    console.error("[INBOUND] Service failed to start:", error);
    process.exit(1);
  }
}

async function shutdown() {
  console.log("[INBOUND] Service shutting down.");

  for (const timer of mondaySyncTimers.values()) clearTimeout(timer);
  mondaySyncTimers.clear();

  server.close(async () => {
    await Promise.allSettled([
      ...mondaySyncChains.values(),
      ...inboundMondayCallerPromises.values(),
      ...inboundFinalPersistenceChains.values(),
      ...inboundSessionPersistenceQueue.values()
    ]);
    await pool.end();
    process.exit(0);
  });
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("unhandledRejection", (error) => {
  console.error("Unhandled promise rejection:", error);
});
process.on("uncaughtException", (error) => {
  console.error("Uncaught exception:", error);
});

if (require.main === module) {
  start();
}


