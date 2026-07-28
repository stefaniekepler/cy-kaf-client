"use strict";

const fs = require("node:fs");

const expectedQuotaKey = "producer_byte_rate";

function absentOrEmpty(row, key) {
  return !Object.prototype.hasOwnProperty.call(row, key) || row[key] === "";
}

function hasExactQuota(rows) {
  return rows.some((row) => {
    if (
      row === null ||
      typeof row !== "object" ||
      row.user !== "e2e" ||
      !absentOrEmpty(row, "clientId") ||
      !absentOrEmpty(row, "ip") ||
      row.quotas === null ||
      typeof row.quotas !== "object" ||
      Array.isArray(row.quotas)
    ) {
      return false;
    }

    const keys = Object.keys(row.quotas);
    const value = row.quotas[expectedQuotaKey];
    return keys.length === 1 && keys[0] === expectedQuotaKey && typeof value === "number" && Number.isFinite(value) && value === 1024;
  });
}

function main(body) {
  let rows;
  try {
    rows = JSON.parse(body);
  } catch (error) {
    return {
      status: 2,
      diagnostic: `listQuotas returned invalid JSON (${error.message}); body=${body}`,
    };
  }
  if (!Array.isArray(rows)) {
    return { status: 2, diagnostic: `listQuotas returned non-array JSON; body=${body}` };
  }

  return { status: hasExactQuota(rows) ? 0 : 1, diagnostic: "" };
}

if (require.main === module) {
  const result = main(fs.readFileSync(0, "utf8"));
  if (result.diagnostic) {
    console.error(result.diagnostic);
  }
  process.exitCode = result.status;
}

module.exports = { absentOrEmpty, expectedQuotaKey, hasExactQuota, main };
