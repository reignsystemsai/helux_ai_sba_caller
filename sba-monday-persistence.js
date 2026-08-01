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
  buildSbaQualificationSessionPatch
};
