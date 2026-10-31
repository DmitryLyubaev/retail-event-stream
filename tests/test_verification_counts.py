import pathlib
import tempfile
import unittest
from scripts.run_checks import junit_counts

class VerificationCountsTest(unittest.TestCase):
    def test_requires_actual_reports(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError):junit_counts(pathlib.Path(folder))

    def test_any_failure_error_or_skip_refuses_acceptance(self):
        with tempfile.TemporaryDirectory() as folder:
            report=pathlib.Path(folder)/"TEST-sample.xml"
            for field in ["failures","errors","skipped"]:
                with self.subTest(field=field):
                    values={"tests":1,"failures":0,"errors":0,"skipped":0};values[field]=1
                    report.write_text('<testsuite '+ ' '.join(k+'="'+str(v)+'"' for k,v in values.items())+'/>')
                    with self.assertRaises(ValueError):junit_counts(pathlib.Path(folder))

    def test_records_observed_totals(self):
        with tempfile.TemporaryDirectory() as folder:
            for name,count in [("one",3),("two",2)]:
                (pathlib.Path(folder)/("TEST-"+name+".xml")).write_text('<testsuite tests="'+str(count)+'" failures="0" errors="0" skipped="0"/>')
            self.assertEqual({"tests":5,"failures":0,"errors":0,"skipped":0},junit_counts(pathlib.Path(folder)))
