import os
from pathlib import Path
import unittest

from mpx4.vectorcheck import verify_all


class SpecificationVectorTests(unittest.TestCase):
    def test_specification_vectors(self):
        value = os.environ.get("MPX4_SPEC_DIR")
        if not value:
            self.skipTest("MPX4_SPEC_DIR is not set")
        spec = Path(value)
        self.assertTrue((spec / "SPECIFICATION.md").exists())
        passed = verify_all(spec)
        self.assertEqual(
            passed,
            ["varint", "frame-encoding", "key-schedule", "secure-record", "tcp-binding"],
        )


if __name__ == "__main__":
    unittest.main()
