"use strict";

const WIX_SBA_INTAKE_PATH = "/api/intake/sba";

const WIX_SBA_ALLOWED_ORIGINS = Object.freeze([
  "https://www.sbahelpcenter.com",
  "https://sbahelpcenter.com"
]);

const WIX_SBA_ACCEPTED_FIELDS = Object.freeze([
  "first_name",
  "last_name",
  "full_name",
  "email",
  "phone",
  "funding_goal",
  "business_entity_type",
  "estimated_credit_score",
  "gross_monthly_revenue",
  "sample_cash_flow",
  "estimated_funding",
  "funding_readiness_score",
  "lead_source",
  "page_url",
  "submitted_at",
  "trigger_outbound"
]);

const WIX_SBA_REQUIRED_FIELDS = Object.freeze([
  "first_name",
  "last_name",
  "email",
  "phone",
  "funding_goal",
  "business_entity_type",
  "estimated_credit_score",
  "gross_monthly_revenue"
]);

function nonblank(value) {
  return value !== undefined && value !== null && String(value).trim() !== "";
}

function semanticFields(body = {}) {
  return WIX_SBA_ACCEPTED_FIELDS.filter((field) => nonblank(body[field]));
}

function applyWixSbaCors(req, res) {
  const origin = String(req.get?.("origin") || req.headers?.origin || "").trim();
  const allowed = !origin || WIX_SBA_ALLOWED_ORIGINS.includes(origin);
  if (origin && allowed) {
    res.set("Access-Control-Allow-Origin", origin);
    res.vary("Origin");
  }
  res.set("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");
  return allowed;
}

function createWixSbaIntakeHandlers({
  createItem,
  log,
  boardId,
  cleanText,
  normalizeEmail,
  normalizePhone,
  validPhone,
  normalizeEntityType,
  normalizeCreditScore,
  normalizeRevenueRange
}) {
  if (typeof createItem !== "function") {
    throw new TypeError("createItem is required.");
  }

  function options(req, res) {
    if (!applyWixSbaCors(req, res)) {
      return res.status(403).json({ success: false, error: "Origin is not allowed." });
    }
    return res.status(204).end();
  }

  async function post(req, res, next) {
    try {
      if (!applyWixSbaCors(req, res)) {
        return res.status(403).json({ success: false, error: "Origin is not allowed." });
      }

      const body = req.body && typeof req.body === "object" ? req.body : {};
      const receivedFields = semanticFields(body);
      log("[WIX_SBA_INTAKE]", "received", {
        semantic_fields: receivedFields
      });

      const missingFields = WIX_SBA_REQUIRED_FIELDS.filter(
        (field) => !nonblank(body[field])
      );
      if (missingFields.length) {
        return res.status(400).json({
          success: false,
          error: `Missing required fields: ${missingFields.join(", ")}.`
        });
      }

      const email = normalizeEmail(body.email);
      if (!email) {
        return res.status(400).json({ success: false, error: "A valid email is required." });
      }
      const phone = normalizePhone(body.phone);
      if (!validPhone(phone)) {
        return res.status(400).json({ success: false, error: "A valid phone number is required." });
      }

      const firstName = cleanText(body.first_name, 100);
      const lastName = cleanText(body.last_name, 100);
      const suppliedFullName = cleanText(body.full_name, 200);
      const submittedAt = new Date(body.submitted_at || Date.now());
      const item = await createItem({
        first_name: firstName,
        last_name: lastName,
        full_name: suppliedFullName || `${firstName} ${lastName}`,
        email,
        phone,
        funding_goal: cleanText(body.funding_goal, 120),
        business_entity_type:
          normalizeEntityType(body.business_entity_type) ||
          cleanText(body.business_entity_type, 120),
        estimated_credit_score:
          normalizeCreditScore(body.estimated_credit_score) ||
          cleanText(body.estimated_credit_score, 120),
        gross_monthly_revenue:
          normalizeRevenueRange(body.gross_monthly_revenue) ||
          cleanText(body.gross_monthly_revenue, 120),
        sample_cash_flow: cleanText(body.sample_cash_flow, 120),
        estimated_funding: cleanText(body.estimated_funding, 120),
        funding_readiness_score: cleanText(body.funding_readiness_score, 120),
        lead_source: cleanText(body.lead_source, 160) || "Wix SBA intake",
        source: "Inbound - Website",
        page_url: cleanText(body.page_url, 1000),
        submitted_at: Number.isNaN(submittedAt.getTime())
          ? null
          : submittedAt.toISOString(),
        updated_date: Number.isNaN(submittedAt.getTime())
          ? new Date().toISOString().slice(0, 10)
          : submittedAt.toISOString().slice(0, 10)
      });

      if (!item?.id) {
        throw new Error("monday.com did not return an SBA intake item ID.");
      }

      log("[WIX_SBA_INTAKE]", "monday_write_success", {
        board_id: String(boardId),
        monday_item_id: String(item.id),
        semantic_fields: receivedFields
      });

      // This inbound-only service intentionally does not initiate outbound calls.
      const outboundTriggered = false;
      log("[WIX_SBA_INTAKE]", "outbound_triggered", {
        requested: body.trigger_outbound === true,
        outbound_triggered: outboundTriggered
      });

      return res.status(200).json({
        success: true,
        monday_item_id: String(item.id),
        outbound_triggered: outboundTriggered
      });
    } catch (error) {
      return next(error);
    }
  }

  return { options, post };
}

module.exports = {
  WIX_SBA_ACCEPTED_FIELDS,
  WIX_SBA_ALLOWED_ORIGINS,
  WIX_SBA_INTAKE_PATH,
  WIX_SBA_REQUIRED_FIELDS,
  applyWixSbaCors,
  createWixSbaIntakeHandlers,
  semanticFields
};
