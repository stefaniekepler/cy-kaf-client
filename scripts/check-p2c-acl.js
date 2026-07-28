"use strict";

const fs = require("node:fs");

const expected = Object.freeze({
  resourceType: "TOPIC",
  resourceName: "e2e-acl",
  namePatternType: "LITERAL",
  principal: "User:e2e",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
});

function hasExactAcl(rows) {
  return rows.some(
    (row) =>
      row !== null &&
      typeof row === "object" &&
      Object.entries(expected).every(([field, value]) => row[field] === value),
  );
}

function main(mode, body) {
  if (mode !== "present" && mode !== "absent") {
    return { status: 2, diagnostic: `expected mode present|absent, got ${mode}` };
  }

  let rows;
  try {
    rows = JSON.parse(body);
  } catch (error) {
    return {
      status: 2,
      diagnostic: `listAcls returned invalid JSON (${error.message}); body=${body}`,
    };
  }
  if (!Array.isArray(rows)) {
    return { status: 2, diagnostic: `listAcls returned non-array JSON; body=${body}` };
  }

  const found = hasExactAcl(rows);
  const matched = mode === "present" ? found : !found;
  return { status: matched ? 0 : 1, diagnostic: "" };
}

if (require.main === module) {
  const result = main(process.argv[2], fs.readFileSync(0, "utf8"));
  if (result.diagnostic) {
    console.error(result.diagnostic);
  }
  process.exitCode = result.status;
}

module.exports = { expected, hasExactAcl, main };
