import os.path
import sys
TEST_ROOT = os.path.dirname(__file__)
sys.path.insert(1, os.path.join(TEST_ROOT, "tests"))
import glob
import unittest


test_file_strings = glob.glob('tests/test_*.py')
module_strings = [str[len('tests/'):len(str) - len('.py')]
                  for str in test_file_strings]
suites = [unittest.defaultTestLoader.loadTestsFromName(str) for str
          in module_strings]
testSuite = unittest.TestSuite(suites)
text_runner = unittest.TextTestRunner().run(testSuite)
