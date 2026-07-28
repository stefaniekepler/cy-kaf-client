"use strict";

const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const path = require("node:path");
const test = require("node:test");

const script = path.join(__dirname, "check-p2c-acl.js");
const exact = {
  resourceType: "TOPIC",
  resourceName: "e2e-acl",
  namePatternType: "LITERAL",
  principal: "User:e2e",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
};

function run(mode, rows) {
  const input = typeof rows === "string" ? rows : JSON.stringify(rows);
  return spawnSync(process.execPath, [script, mode], { input, encoding: "utf8" });
}

test("present requires all seven fields on the same object", () => {
  assert.equal(run("present", [exact]).status, 0);
  assert.equal(run("present", [{ principal: "User:e2e" }]).status, 1);
  assert.equal(run("present", [{ ...exact, host: "192.0.2.1" }]).status, 1);
});

test("absent is the exact negation and cannot pass while the ACL remains", () => {
  assert.equal(run("absent", [exact]).status, 1);
  assert.equal(run("absent", [{ ...exact, operation: "WRITE" }]).status, 0);
  assert.equal(run("absent", []).status, 0);
});

test("invalid JSON fails immediately with the body in diagnostics", () => {
  const result = run("present", "{not-json");
  assert.equal(result.status, 2);
  assert.match(result.stderr, /invalid JSON/);
  assert.match(result.stderr, /body=\{not-json/);
});
