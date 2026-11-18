"""The audit chain's vocabulary is one list (root ADR-0569 §2, §3).

`config/audit-events.contract.json` names who may write each audit source and the words a row may
use; the migrations spell the same lists as `audit.event` CHECKs and `audit.source` rows, which are
what the database enforces. Two spellings drift: a source added to the CHECK but not to
`audit.source` is a source nobody may write, and one added to `audit.source` for the wrong role lets
that role write another's events (the forgery ADR-0569 §3 closes). These tests refuse any
difference, and an operator ledger action that the audit trigger would emit without the contract
listing it.
"""

import json
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
CONTRACT = PLATFORM / "config/audit-events.contract.json"
MIGRATIONS = sorted((PLATFORM / "migrations").glob("*.sql"))


def contract():
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def audit_migrations():
    return [path.read_text(encoding="utf-8") for path in MIGRATIONS if "audit.event" in path.read_text(encoding="utf-8")]


def last_in_list(texts, column):
    """The values of the last `CHECK (<column> IN ('a', 'b', ...)` the migrations spell, in order."""
    found = None
    for text in texts:
        for match in re.finditer(rf"CHECK\s*\(\s*{column}\s+IN\s+\(([^)]*)\)", text):
            found = re.findall(r"'([^']*)'", match.group(1))
    return found


def source_rows(texts):
    rows = set()
    for text in texts:
        for block in re.finditer(r"INSERT INTO audit\.source \(source, writer_role, verifies_actor\) VALUES(.*?);", text, re.S):
            for source, role, verifies in re.findall(r"\('([a-z_]+)',\s*'([a-z_]+)',\s*(true|false)\)", block.group(1)):
                rows.add((source, role, verifies == "true"))
    return rows


class AuditVocabularyIsOneList(unittest.TestCase):
    def setUp(self):
        self.contract = contract()
        self.texts = audit_migrations()
        self.assertTrue(self.texts, "no migration creates audit.event")

    def test_the_writers_are_the_audit_source_rows(self):
        listed = {(w["source"], w["writer_role"], w["verifies_actor"]) for w in self.contract["writers"]}
        self.assertEqual(listed, source_rows(self.texts))

    def test_every_listed_source_is_one_the_event_check_accepts_and_back(self):
        sources = {w["source"] for w in self.contract["writers"]}
        self.assertEqual(sources, set(last_in_list(self.texts, "source")))

    def test_the_closed_words_are_the_checks(self):
        for key, column in [
            ("operations", "operation"),
            ("actor_kinds", "actor_kind"),
            ("results", "result"),
            ("applied_via_kinds", "applied_via_kind"),
            ("cursor_states", "state"),
        ]:
            with self.subTest(column=column):
                self.assertEqual(set(self.contract[key]), set(last_in_list(self.texts, column)))

    def test_every_action_names_a_listed_source_and_the_audit_action_shape(self):
        sources = {w["source"] for w in self.contract["writers"]}
        actions = [a["action"] for a in self.contract["actions"]]
        self.assertEqual(len(actions), len(set(actions)), "an action is listed twice")
        for entry in self.contract["actions"]:
            with self.subTest(action=entry["action"]):
                self.assertRegex(entry["action"], r"^[a-z_]+\.[a-z_]+$")
                self.assertIn(entry["source"], sources)

    def test_every_operator_ledger_action_the_trigger_emits_is_listed(self):
        ledger = [path.read_text(encoding="utf-8") for path in MIGRATIONS
                  if "operator_action_action_check" in path.read_text(encoding="utf-8")]
        ledger_actions = last_in_list(ledger, "action")
        self.assertTrue(ledger_actions)
        listed = {a["action"] for a in self.contract["actions"] if a["source"] == "dawneer"}
        emitted = {f"operator_action.{action}" for action in ledger_actions}
        self.assertEqual(emitted, {a for a in listed if a.startswith("operator_action.")})


if __name__ == "__main__":
    unittest.main()
