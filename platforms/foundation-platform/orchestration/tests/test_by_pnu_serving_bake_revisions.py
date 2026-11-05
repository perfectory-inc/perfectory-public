"""Another registry revision of the same document schema name explains a change set (root ADR-0336,
ADR-0346).

2026-10-29..11-04 the 공매 lane's registry revision 2 -> 3 (ADR-0340) re-rendered every document
under the one name `.v2`: the bake saw the same schema, stated no revision, and refused the change
set (4,104 changed, 0 same) as unexplained for six days, while users saw `photos: []`.
Its own file beside test_by_pnu_serving_bake.py, which holds the fake publisher and Spark it runs.
"""

import json
import unittest

import test_by_pnu_serving_bake as base


class RegistryRevisionBake(unittest.TestCase):
    setUp = base.SectionPackBake.setUp
    bake = base.SectionPackBake.bake
    exports = base.SectionPackBake.exports
    published = base.SectionPackBake.published
    PREFIX = base.SectionPackBake.PREFIX

    def test_another_registry_revision_of_the_same_name_explains_a_change_set_over_the_bound(self):
        # Root ADR-0336: 2026-10-29..11-04 the 공매 lane's registry revision 2 -> 3 (ADR-0340)
        # re-rendered every document under the one name `.v2`; `schemas` said same, no revision was
        # stated, and the change set was refused as unexplained for six days. The served packs now
        # record their revision, and a move to the export's is the record that explains it.
        why = {"document_schema": "the documents move doc.v2 revision 2->3 (root ADR-0320)"}
        result, calls = self.bake(FAKE_PACKS=base.packs_state(served_document_schema_versions=["doc.v2"],
                                                         served_document_schema_revisions=[2, 2]),
                                  FAKE_REVISION="3", FAKE_DELTA_EXIT="6", FAKE_DELTA_EXPLAINED=json.dumps(why))
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual(delta["options"]["--document-schema-revision"], "doc.v2 revision 2->3")
        self.assertTrue(self.exports(calls))
        self.assertTrue(self.published(calls))

    def assert_no_revision_stated(self, served):
        _result, calls = self.bake(FAKE_PACKS=base.packs_state(served_document_schema_versions=["doc.v2"],
                                                          served_document_schema_revisions=served),
                                   FAKE_REVISION="3", FAKE_DELTA_EXIT="4")
        [delta] = [call for call in calls if call["command"] == "delta"]
        self.assertEqual(delta["options"]["--document-schema-revision"], "")

    def test_a_revision_the_packs_do_not_record_explains_nothing(self):
        # A base published before the record says nothing: unknown is never "moved".
        self.assert_no_revision_stated([None, 2])

    def test_the_revision_the_packs_already_serve_explains_nothing(self):
        self.assert_no_revision_stated([3])


if __name__ == "__main__":
    unittest.main()
