import datetime
import unittest
import sys
sys.path.append('/app/src')
from modeling.feature.feature_generator import FeatureGenerator

class FeatureGeneratorUnderTest(unittest.TestCase):
  def test_newly_created_returns_feature_with_the_id(self):
    generator = FeatureGenerator(1, datetime.date.min)
    feature = generator.getFeature()
    self.assertEqual(feature['uid'], 1)

  def test_newly_created_returns_feature_with_predTime_in_iso_format(self):
    today = datetime.date.today()
    generator = FeatureGenerator(0, today)
    feature = generator.getFeature()
    self.assertEqual(feature['predTime'], today.isoformat())

