#!/usr/bin/env python

import unittest
from libfactory import info


class TestStatusInfo(unittest.TestCase):

    def setUp(self):
        self.info = info.StatusInfo()

    def test_groupby(self):
        pass



if __name__ == '__main__':
    unittest.main()

