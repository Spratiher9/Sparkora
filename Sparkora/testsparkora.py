import unittest
from main import Sparkora
from pyspark.shell import spark
import pandas as pd

class TestSparkora(unittest.TestCase):
  def setUp(self):
    self.sparkora = Sparkora()
    self.sparkora.configure(output = 'A', data = './test_data.csv')

  def test_configure(self):
    data = spark.read.csv('./test_data.csv',header=True)
    self.assertEqual(self.sparkora.output, 'A')
    self.assertEqual(self.sparkora.data,data)

  def test_remove_feature(self):
    self.sparkora.remove_feature('useless_feature')
    self.assertFalse('useless_feature' in self.sparkora.data.columns)

  def test_extract_feature(self):
    self.sparkora.extract_feature(
      'useless_feature',
      'another_useless_feature',
      lambda x: x * 2
    )
    actual_column = self.sparkora.data.select("another_useless_feature").rdd.flatMap(lambda x: x).collect()
    expected_column = [2, 2, 2]
    self.assertEqual(actual_column, expected_column)

  def test_impute_missing_values(self):
    self.sparkora.impute_missing_values()
    actual_column = self.sparkora.data.select("B").rdd.flatMap(lambda x: x).collect()
    expected_column = [2.0, 5.0, 8.0]
    self.assertEqual(actual_column, expected_column)

  def test_scale_input_values(self):
    self.sparkora.scale_input_values()
    actual_column = self.sparkora.data.select("C").rdd.flatMap(lambda x: x).collect()
    expected_column = [-1.224745, 0.0, 1.224745]
    pairwise_diffs = map(
      lambda actual, expected: abs(actual - expected),
      actual_column,
      expected_column
    )
    total_diff = sum(pairwise_diffs)
    self.assertAlmostEqual(total_diff, 0, places = 6)

  def test_extract_ordinal_feature(self):
    self.sparkora.extract_ordinal_feature('D')
    features = self.sparkora.data.columns
    self.assertTrue('D=left' in features and 'D=right' in features)

  def test_input_columns(self):
    actual_input_columns = self.sparkora.input_columns()
    expected_input_columns = self.sparkora.data.columns
    expected_input_columns.remove(self.sparkora.output)
    self.assertEqual(actual_input_columns, expected_input_columns)

  def test_logs(self):
    self.sparkora.extract_ordinal_feature('D')
    self.sparkora.impute_missing_values()
    self.sparkora.scale_input_values()

    actual_logs = self.sparkora.logs
    expected_logs = [
      "self.extract_ordinal_feature('D')",
       'self.impute_missing_values()',
       'self.scale_input_values()'
    ]
    self.assertEqual(actual_logs, expected_logs)

  def test_snapshots(self):
    self.sparkora.snapshot('start')
    self.sparkora.extract_ordinal_feature('D')
    self.sparkora.use_snapshot('start')

    self.assertEqual(self.sparkora.logs, [])
    self.assertTrue(self.sparkora.data.equals(self.sparkora.initial_data))

if __name__ == '__main__':
    unittest.main()