"use strict";

const { applyWixSbaCors } = require("./wix-sba-intake");

const WIX_SBA_ANALYZER_PATH = "/api/intake/sba/update";
const WIX_SBA_ANALYZER_FIELDS = Object.freeze([
  "monthly_business_expenses",
  "years_in_business",
  "tax_filing_status",
  "average_ending_bank_balance",
  "estimated_credit_score",
  "gross_monthly_revenue"
]);

const TAX_FILING_STATUSES = Object.freeze([
  "I need to file",
  "I have - 1yr tax return",
  "I have - 2yrs tax returns",
  "I have - 3yr tax returns"
]);

function nonblank(value) {
  return value !== undefined && value !== null &&
    (typeof value !== "string" || value.trim() !== "");
}

function analyzerKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[–—]/g, "-")
    .replace(/[^a-z0-9]+/g, "");
}

function normalizeYearsInBusiness(value) {
  if (!nonblank(value)) return null;
  const key = analyzerKey(value);
  const labels = new Map([
    ["lessthan1year", 0],
    ["under1year", 0],
    ["01year", 0],
    ["12years", 1],
    ["1to2years", 1],
    ["25years", 2],
    ["2to5years", 2],
    ["5years", 5],
    ["5plusyears", 5],
    ["morethan5years", 5]
  ]);
  if (labels.has(key)) return labels.get(key);
  if (/^(?:0|1|2|5)$/.test(String(value).trim())) return Number(value);
  throw new Error(
    "years_in_business must be Less than 1 year, 1 - 2 years, 2 - 5 years, or 5+ years; scoring points are not accepted."
  );
}

function normalizeTaxFilingStatus(value) {
  if (!nonblank(value)) return null;
  const desired = analyzerKey(value);
  const status = TAX_FILING_STATUSES.find(
    (candidate) => analyzerKey(candidate) === desired
  );
  if (!status) {
    throw new Error(`Unsupported tax_filing_status: ${String(value).trim()}.`);
  }
  return status;
}

function createWixSbaAnalyzerHandlers({
  findItem,
  updateItem,
  verifyItem,
  log,
  boardId,
  cleanText,
  sanitizeLogData = (value) => value,
  normalizeCreditScore,
  normalizeRevenueRange
}) {
  if (
    typeof findItem !== "function" ||
    typeof updateItem !== "function" ||
    typeof verifyItem !== "function"
  ) {
    throw new TypeError("findItem, updateItem, and verifyItem are required.");
  }

  function failed(res, statusCode, mondayItemId, error) {
    log("[SBA_ANALYZER_UPDATE]", "monday_update_failed", {
      monday_item_id: mondayItemId || null,
      error
    });
    log("[WIX_SBA_ANALYZER]", "monday_update_failed", {
      monday_item_id: mondayItemId || null,
      error
    });
    return res.status(statusCode).json({ success: false, error });
  }

  function options(req, res) {
    if (!applyWixSbaCors(req, res)) {
      return res.status(403).json({ success: false, error: "Origin is not allowed." });
    }
    return res.status(204).end();
  }

  async function post(req, res) {
    if (!applyWixSbaCors(req, res)) {
      return res.status(403).json({ success: false, error: "Origin is not allowed." });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const mondayItemId = cleanText(body.monday_item_id, 100);
    const updateData = body.updateData && typeof body.updateData === "object"
      ? body.updateData
      : body.update_data && typeof body.update_data === "object"
        ? body.update_data
        : {};
    const receivedFields = WIX_SBA_ANALYZER_FIELDS.filter((field) =>
      nonblank(updateData[field])
    );
    log("[SBA_ANALYZER_UPDATE]", "received", {
      logical_fields: receivedFields
    });
    log("[SBA_ANALYZER_UPDATE]", "monday_item_id", {
      monday_item_id: mondayItemId || null
    });
    log("[SBA_ANALYZER_UPDATE]", "item_id", {
      item_id: mondayItemId || null
    });
    log("[SBA_ANALYZER_UPDATE]", "incoming_fields", {
      incoming_fields: Object.keys(updateData)
    });
    log("[SBA_ANALYZER_UPDATE]", "incoming_updateData", {
      monday_item_id: mondayItemId || null,
      incoming_field_names: Object.keys(updateData),
      incoming_updateData: sanitizeLogData(updateData)
    });
    log("[WIX_SBA_ANALYZER]", "received", {
      monday_item_id: mondayItemId || null,
      logical_fields: receivedFields
    });

    if (!mondayItemId || !/^\d+$/.test(mondayItemId)) {
      return failed(res, 400, mondayItemId, "A valid monday_item_id is required.");
    }
    if (receivedFields.length === 0) {
      return failed(res, 400, mondayItemId, "No supported analyzer fields were supplied.");
    }

    let patch;
    try {
      patch = {};
      if (nonblank(updateData.monthly_business_expenses)) {
        patch.monthly_business_expenses = cleanText(
          updateData.monthly_business_expenses,
          120
        );
      }
      if (nonblank(updateData.years_in_business)) {
        patch.years_in_business = normalizeYearsInBusiness(
          updateData.years_in_business
        );
      }
      if (nonblank(updateData.tax_filing_status)) {
        patch.tax_filing_status = normalizeTaxFilingStatus(
          updateData.tax_filing_status
        );
      }
      if (nonblank(updateData.average_ending_bank_balance)) {
        patch.average_ending_bank_balance = cleanText(
          updateData.average_ending_bank_balance,
          120
        );
      }
      if (nonblank(updateData.estimated_credit_score)) {
        patch.estimated_credit_score = normalizeCreditScore(
          updateData.estimated_credit_score
        );
        if (!patch.estimated_credit_score) {
          throw new Error("Unsupported estimated_credit_score value.");
        }
      }
      if (nonblank(updateData.gross_monthly_revenue)) {
        patch.gross_monthly_revenue = normalizeRevenueRange(
          updateData.gross_monthly_revenue
        );
        if (!patch.gross_monthly_revenue) {
          throw new Error("Unsupported gross_monthly_revenue value.");
        }
      }
    } catch (error) {
      return failed(res, 400, mondayItemId, error.message);
    }
    log("[SBA_ANALYZER_UPDATE]", "normalized_values", {
      monday_item_id: mondayItemId,
      normalized_values: sanitizeLogData(patch)
    });

    try {
      const item = await findItem(mondayItemId);
      if (!item?.id) {
        return failed(
          res,
          404,
          mondayItemId,
          `Monday item ${mondayItemId} was not found.`
        );
      }
      if (String(item.board?.id || "") !== String(boardId)) {
        return failed(
          res,
          409,
          mondayItemId,
          `Monday item ${mondayItemId} is not on SBA board ${boardId}.`
        );
      }

      const updated = await updateItem(mondayItemId, patch);
      if (!updated?.id || String(updated.id) !== mondayItemId) {
        throw new Error("Monday did not confirm the requested item update.");
      }
      const writtenFields = Array.isArray(updated.sba_written_logical_fields)
        ? updated.sba_written_logical_fields
        : [];
      const missingFields = Object.keys(patch).filter(
        (field) => !writtenFields.includes(field)
      );
      if (missingFields.length) {
        throw new Error(
          `Monday did not confirm fields: ${missingFields.join(", ")}.`
        );
      }

      const readback = await verifyItem(mondayItemId, patch);
      const verifiedFields = Array.isArray(readback?.verified_fields)
        ? readback.verified_fields
        : [];
      const unverifiedFields = Object.keys(patch).filter(
        (field) => !verifiedFields.includes(field)
      );
      if (unverifiedFields.length) {
        throw new Error(
          `Monday read-back did not verify fields: ${unverifiedFields.join(", ")}.`
        );
      }
      log("[SBA_ANALYZER_UPDATE]", "readback_verified", {
        item_id: mondayItemId,
        verified_fields: verifiedFields
      });

      log("[WIX_SBA_ANALYZER]", "monday_update_success", {
        monday_item_id: mondayItemId,
        updated_fields: writtenFields
      });
      log("[SBA_ANALYZER_UPDATE]", "monday_update_success", {
        monday_item_id: mondayItemId,
        updated_fields: writtenFields
      });
      return res.status(200).json({
        success: true,
        monday_item_id: mondayItemId,
        updated_fields: writtenFields
      });
    } catch (error) {
      return failed(res, 502, mondayItemId, error.message);
    }
  }

  return { options, post };
}

module.exports = {
  TAX_FILING_STATUSES,
  WIX_SBA_ANALYZER_FIELDS,
  WIX_SBA_ANALYZER_PATH,
  createWixSbaAnalyzerHandlers,
  normalizeTaxFilingStatus,
  normalizeYearsInBusiness
};
