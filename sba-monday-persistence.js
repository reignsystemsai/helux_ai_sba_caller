"use strict";

const { SBA_BOARD } = require("./sba-inbound");

const SBA_QUALIFICATION_COLUMN_IDS = Object.freeze({
  full_name: SBA_BOARD.columns.name,
  first_name: SBA_BOARD.columns.firstName,
  last_name: SBA_BOARD.columns.lastName,
  email: SBA_BOARD.columns.email,
  business_entity_type: SBA_BOARD.columns.businessEntityType,
  entity_status: SBA_BOARD.columns.entityStatus,
  phone: SBA_BOARD.columns.phoneNumber,
  phone_number: SBA_BOARD.columns.phoneNumber,
  estimated_credit_score: SBA_BOARD.columns.estimatedCreditScore,
  credit_status: SBA_BOARD.columns.creditStatus,
  gross_monthly_revenue: SBA_BOARD.columns.grossMonthlyRevenue,
  income_status: SBA_BOARD.columns.incomeStatus,
  city: SBA_BOARD.columns.city,
  updated_date: SBA_BOARD.columns.updatedDate,
  lead_id: SBA_BOARD.columns.leadId
});

const SBA_QUALIFICATION_SESSION_FIELDS = Object.freeze([
  "first_name",
  "last_name",
  "full_name",
  "email",
  "phone",
  "phone_number",
  "business_entity_type",
  "entity_status",
  "time_in_business",
  "estimated_credit_score",
  "credit_status",
  "gross_monthly_revenue",
  "income_status",
  "city",
  "state",
  "lead_id",
  "taxes",
  "tax_status",
  "zip"
]);

function meaningful(value) {
  return value !== undefined && value !== null &&
    (typeof value !== "string" || value.trim() !== "");
}

function clean(value, maximum = 500) {
  if (!meaningful(value)) return "";
  return String(value).trim().slice(0, maximum);
}

function key(value) {
  return clean(value, 200).toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function mondayItemColumnText(item, columnId) {
  return clean(
    item?.column_values?.find((column) => column.id === columnId)?.text,
    500
  );
}

function mondayItemProfileScore(item) {
  const weightedColumns = [
    [SBA_BOARD.columns.firstName, 4],
    [SBA_BOARD.columns.lastName, 4],
    [SBA_BOARD.columns.email, 6],
    [SBA_BOARD.columns.city, 2],
    [SBA_BOARD.columns.zip, 2],
    [SBA_BOARD.columns.businessEntityType, 3],
    [SBA_BOARD.columns.taxes, 2],
    [SBA_BOARD.columns.estimatedCreditScore, 3],
    [SBA_BOARD.columns.grossMonthlyRevenue, 3],
    [SBA_BOARD.columns.leadId, 3]
  ];
  return weightedColumns.reduce(
    (score, [columnId, weight]) =>
      score + (mondayItemColumnText(item, columnId) ? weight : 0),
    0
  );
}

function mondayItemTimestamp(value) {
  const timestamp = Date.parse(clean(value, 100));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function compareMondayItemIdsDescending(left, right) {
  const leftId = clean(left?.id, 100);
  const rightId = clean(right?.id, 100);
  if (/^\d+$/.test(leftId) && /^\d+$/.test(rightId)) {
    const leftNumeric = BigInt(leftId);
    const rightNumeric = BigInt(rightId);
    return leftNumeric === rightNumeric ? 0 : leftNumeric > rightNumeric ? -1 : 1;
  }
  return rightId.localeCompare(leftId, "en", { numeric: true });
}

function selectBestSbaMondayMatch(matches = []) {
  const candidates = matches.filter((item) => item?.id);
  if (candidates.length === 0) return null;
  if (candidates.length === 1) {
    return {
      item: candidates[0],
      selection_reason: "only_phone_match",
      profile_score: mondayItemProfileScore(candidates[0])
    };
  }

  const ranked = candidates
    .map((item) => ({
      item,
      profile_score: mondayItemProfileScore(item),
      updated_at: mondayItemTimestamp(item.updated_at),
      created_at: mondayItemTimestamp(item.created_at)
    }))
    .sort((left, right) =>
      right.profile_score - left.profile_score ||
      right.updated_at - left.updated_at ||
      right.created_at - left.created_at ||
      compareMondayItemIdsDescending(left.item, right.item)
    );

  const selected = ranked[0];
  const runnerUp = ranked[1];
  const selectionReason = selected.profile_score !== runnerUp.profile_score
    ? "strongest_profile_data"
    : selected.updated_at !== runnerUp.updated_at
      ? "most_recently_updated"
      : selected.created_at !== runnerUp.created_at
        ? "most_recently_created"
        : "highest_item_id";
  return {
    item: selected.item,
    selection_reason: selectionReason,
    profile_score: selected.profile_score
  };
}

function buildSbaQualificationSessionPatch(data = {}) {
  return Object.fromEntries(
    SBA_QUALIFICATION_SESSION_FIELDS
      .filter((field) => meaningful(data[field]))
      .map((field) => [
        field,
        typeof data[field] === "string" ? data[field].trim() : data[field]
      ])
  );
}

function columnByIdOrTitle(metadata, columnId, title) {
  return metadata?.columns?.find((column) => column.id === columnId) ||
    metadata?.columns?.find((column) => key(column.title) === key(title)) ||
    null;
}

function columnLabel(column, desired) {
  const labels = column?.settings?.labels;
  const entries = Array.isArray(labels) ? labels : Object.values(labels || {});
  return entries
    .map((entry) => typeof entry === "string" ? entry : entry?.name || entry?.label)
    .find((label) => key(label) === key(desired)) || null;
}

function buildSbaMondayUpdateValues({ data = {}, metadata, onSkippedColumn }) {
  const values = {};
  const skip = typeof onSkippedColumn === "function"
    ? onSkippedColumn
    : () => undefined;

  for (const [field, columnId] of [
    ["first_name", SBA_BOARD.columns.firstName],
    ["last_name", SBA_BOARD.columns.lastName],
    ["taxes", SBA_BOARD.columns.taxes],
    ["city", SBA_BOARD.columns.city],
    ["lead_id", SBA_BOARD.columns.leadId]
  ]) {
    const value = clean(data[field]);
    if (value) values[columnId] = value;
  }

  const email = clean(data.email, 320).toLowerCase();
  if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    values[SBA_BOARD.columns.email] = { email, text: email };
  }

  const zip = clean(data.zip, 20).replace(/[^0-9]/g, "");
  if (zip) values[SBA_BOARD.columns.zip] = zip;

  for (const [field, columnId, title] of [
    ["business_entity_type", SBA_BOARD.columns.businessEntityType, "Business Entity Type"],
    ["estimated_credit_score", SBA_BOARD.columns.estimatedCreditScore, "Estimated Credit Score"],
    ["gross_monthly_revenue", SBA_BOARD.columns.grossMonthlyRevenue, "Gross Monthly Revenue"]
  ]) {
    const desired = clean(data[field], 120);
    if (!desired) continue;
    const column = columnByIdOrTitle(metadata, columnId, title);
    const label = columnLabel(column, desired);
    if (column?.id && label) values[column.id] = { labels: [label] };
    else skip({ field, columnId, desiredValue: desired, reason: "dropdown_label_not_found" });
  }

  for (const [field, columnId, title] of [
    ["entity_status", SBA_BOARD.columns.entityStatus, "Entity_Status"],
    ["tax_status", SBA_BOARD.columns.taxStatus, "Tax_Status"],
    ["credit_status", SBA_BOARD.columns.creditStatus, "Credit_Status"],
    ["income_status", SBA_BOARD.columns.incomeStatus, "Income_Status"]
  ]) {
    const desired = clean(data[field], 100);
    if (!desired) continue;
    const column = columnByIdOrTitle(metadata, columnId, title);
    const label = columnLabel(column, desired);
    if (column?.id && label) values[column.id] = { label };
    else skip({ field, columnId, desiredValue: desired, reason: "status_label_not_found" });
  }

  const phone = clean(data.phone || data.phone_number, 100);
  if (phone) {
    values[SBA_BOARD.columns.phoneNumber] = {
      phone,
      countryShortName: "US"
    };
  }

  const updatedAt = new Date(data.updated_date || data.date_called || "");
  if (!Number.isNaN(updatedAt.getTime())) {
    values[SBA_BOARD.columns.updatedDate] = {
      date: updatedAt.toISOString().slice(0, 10)
    };
  }

  return values;
}

module.exports = {
  SBA_QUALIFICATION_COLUMN_IDS,
  buildSbaMondayUpdateValues,
  buildSbaQualificationSessionPatch,
  selectBestSbaMondayMatch
};
