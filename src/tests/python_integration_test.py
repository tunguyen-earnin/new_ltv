import datetime
import unittest
import sys
import csv
sys.path.append('/app/src')

class FullUnitTest(unittest.TestCase):
  def test_getreasoncategory(self):
    filename = '/app/src/modeling/model/max_adjustment/var2reasonCategory.csv'
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)