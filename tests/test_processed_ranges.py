import unittest

from common.dedup import ProcessedRanges


class ProcessedRangesTest(unittest.TestCase):
    def test_adds_and_compacts_adjacent_ranges(self):
        ranges = ProcessedRanges()

        ranges.add(20).add(21).add(22).add(23).add(26)

        self.assertEqual(ranges.to_string(), "20-23;26")
        self.assertTrue(ranges.contains(22))
        self.assertFalse(ranges.contains(24))

        ranges.add(24)
        self.assertEqual(ranges.to_string(), "20-24;26")

        ranges.add(25)
        self.assertEqual(ranges.to_string(), "20-26")

    def test_parses_serialized_ranges(self):
        ranges = ProcessedRanges.from_string("20-23;26;30-35")

        self.assertEqual(ranges.as_tuples(), ((20, 23), (26, 26), (30, 35)))
        self.assertTrue(ranges.contains(31))
        self.assertFalse(ranges.contains(29))

    def test_rejects_invalid_ranges(self):
        with self.assertRaises(ValueError):
            ProcessedRanges().add_range(10, 9)


if __name__ == "__main__":
    unittest.main()
