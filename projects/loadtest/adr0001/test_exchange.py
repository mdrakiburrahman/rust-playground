import copy
import unittest
from jsonschema import ValidationError

from exchange import CONTRACT, load, validate


class ExchangeTests(unittest.TestCase):
    def setUp(self):
        self.value = load(CONTRACT / "benchmark-v1.fixture.json")

    def rejects(self, mutate):
        value = copy.deepcopy(self.value)
        mutate(value)
        with self.assertRaises((ValueError, ValidationError)):
            validate(value)

    def test_fixture(self):
        validate(self.value)

    def test_wrong_trace_kind(self):
        self.rejects(lambda v: v["records"][2].update(trace_id="batch-1"))

    def test_cross_run_trace(self):
        self.value["runs"].append({**self.value["runs"][0], "id": "run-2"})
        self.rejects(lambda v: v["records"][1].update(run_id="run-2"))

    def test_missing_table(self):
        self.rejects(lambda v: v["records"][0].update(table_id="missing"))

    def test_unavailable_value(self):
        self.rejects(lambda v: v["records"][0].update(evidence="unavailable", method="not-observed", value=10))

    def test_negative_metric(self):
        self.rejects(lambda v: v["records"][0].update(value=-1))

    def test_unlinked_derived(self):
        self.rejects(lambda v: v["records"][0].update(evidence="derived"))

    def test_false_complete(self):
        self.rejects(lambda v: v["coverage"].update(dropped_events=1))

    def test_unknown_field(self):
        self.rejects(lambda v: v.update(unrecognized_field="synthetic-placeholder"))


if __name__ == "__main__":
    unittest.main()
