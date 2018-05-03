#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

"""
Some example of Analyzers
"""

class GroupByKey(AnalyzerGroup):

    def __init__(self, key):
        self.key = key

    def group(self, job):
        try:
            return job[self.key]
        except Exception, ex:
            return None


class GroupByKeyRemap(AnalyzerGroup):

    def __init__(self, key, mapping_d):
        self.key = key
        self.mapping_d = mapping_d

    def group(self, job):
        try:
            value = job[self.key]
        except Exception, ex:
            return None

        if value in self.mapping_d.keys():
            return self.mapping_d[value]
        else:
            return None


class Length(AnalyzerReduce):

    def __init__(self):
        pass

    def reduce(self, data):
        return len(data)
