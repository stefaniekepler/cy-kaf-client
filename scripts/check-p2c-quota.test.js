"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const path = require("node:path");
const test = require("node:test");

const script = path.join(__dirname, "check-p2c-quota.js");

function run(rows) {
  const input = typeof rows === "string" ? rows : JSON.stringify(rows);
  return spawnSync(process.execPath, [script], { input, encoding: "utf8" });
}

test("accepts only the exact e2e quota entity and quota map", () => {
  assert.equal(run([{ user: "e2e", quotas: { producer_byte_rate: 1024 } }]).status, 0);
  assert.equal(run([{ user: "e2e", quotas: { producer_byte_rate: 1024, consumer_byte_rate: 1 } }]).status, 1);
  assert.equal(run([{ user: "e2e", quotas: { producer_byte_rate: 1025 } }]).status, 1);
});

test("allows absent-or-empty optional identity components only", () => {
  assert.equal(run([{ user: "e2e", clientId: "", ip: "", quotas: { producer_byte_rate: 1024 } }]).status, 0);
  assert.equal(run([{ user: "e2e", clientId: "client", quotas: { producer_byte_rate: 1024 } }]).status, 1);
});

test("invalid JSON fails immediately with the body in diagnostics", () => {
  const result = run("{not-json");
  assert.equal(result.status, 2);
  assert.match(result.stderr, /invalid JSON/);
  assert.match(result.stderr, /body=\{not-json/);
});
