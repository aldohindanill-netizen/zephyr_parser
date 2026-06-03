from __future__ import annotations

import unittest

from scripts.calibrate_bug_duplicates import _metrics, _pair_key


class CalibrateBugDuplicatesTests(unittest.TestCase):
    """Класс «CalibrateBugDuplicatesTests»."""
    def test_pair_key_is_order_independent(self) -> None:
        """Вспомогательная функция: test pair key is order independent."""
        self.assertEqual(_pair_key("CSD-2", "CSD-1"), ("CSD-1", "CSD-2"))
        self.assertEqual(_pair_key("csd-1", "CSD-2"), ("CSD-1", "CSD-2"))

    def test_metrics_computation(self) -> None:
        """Вспомогательная функция: test metrics computation."""
        predicted = {("CSD-1", "CSD-2"), ("CSD-3", "CSD-4")}
        positive = {("CSD-1", "CSD-2"), ("CSD-5", "CSD-6")}
        m = _metrics(predicted, positive)
        self.assertEqual((m.tp, m.fp, m.fn), (1, 1, 1))
        self.assertAlmostEqual(m.precision, 0.5)
        self.assertAlmostEqual(m.recall, 0.5)


if __name__ == "__main__":
    unittest.main()
