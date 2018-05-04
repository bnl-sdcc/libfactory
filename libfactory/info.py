#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

"""
Generic code to store and manipulate data
"""

import datetime
import inspect
import logging
import logging.handlers
import threading
import time
import traceback
import os
import pwd
import sys


# =============================================================================
# Info class
# =============================================================================

class StatusInfo(object):
    """
    """

    def __init__(self, data, is_raw=True, is_mutable=True, timestamp=None):
        """ 
        :param data: the data to be recorded
        :param is_raw boolean: indicates if the object is primary or it is composed by other StatusInfo objects
        :param is_mutable boolean: indicates if the data can still be processed or not
        :param timestamp: the time when this object was created
        """ 
        self.log = logging.getLogger('autopyfactory')
        self.is_raw = is_raw
        self.is_mutable = is_mutable
        self.data = data 
        if not timestamp:
            self.timestamp = int(time.time())
        else:
            self.timestamp = timestamp

    # -------------------------------------------------------------------------
    # methods to manipulate the data
    # -------------------------------------------------------------------------

    def analyze(self, analyzer):
        """
        generic method that picks the right one 
        based on the type of analyzer
        :param analyzer: an Analyzer object 
        :rtype StatusInfo:
        """
        if analyzer.analyzertype == 'group':
            return self.group(analyzer)
        if analyzer.analyzertype == 'filter':
            return self.filter(analyzer)
        if analyzer.analyzertype == 'map':
            return self.map(analyzer)
        if analyzer.analyzertype == 'reduce':
            return self.reduce(analyzer)


    def group(self, analyzer):
        """
        groups the items recorded in self.data into a dictionary
        and creates a new StatusInfo object with it. 
           1. make a dictinary grouping items according to rules in analyzer
           2. convert that dictionary into a dictionary of StatusInfo objects
           3. make a new StatusInfo with that dictionary
        :param analyzer: an object implementing method group()
        :rtype StatusInfo:
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('group')

        if not analyzer.analyzertype == 'group':
            raise IncorrectAnalyzer(analyzer, 'group')

        if self.is_raw:
            # 1
            tmp_new_data = {} 
            for item in self.data:
                key = analyzer.group(item)
                if key:
                    if key not in tmp_new_data.keys():
                        tmp_new_data[key] = []
                    tmp_new_data[key].append(item) 
            # 2
            new_data = {}
            for k, v in tmp_new_data.items():
                new_data[k] = StatusInfo(v, timestamp=self.timestamp)
            # 3
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items():
                new_data[key] = statusinfo.group(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def map(self, analyzer):
        """
        modifies each item in self.data according to rules
        in analyzer
        :param analyzer: an object implementing method map()
        :rtype StatusInfo:
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('map')

        if not analyzer.analyzertype == 'map':
            raise IncorrectAnalyzer(analyzer, 'map')

        if self.is_raw:
            new_data = []
            for item in self.data:
                new_item = analyzer.map(item)
                new_data.append(new_item)
            new_info = StatusInfo(new_data, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items():
                new_data[key] = statusinfo.map(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def filter(self, analyzer):
        """
        eliminates the items in self.data that do not pass
        the filter implemented in analyzer
        :param analyzer: an object implementing method filter()
        :rtype StatusInfo:
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('filter')

        if not analyzer.analyzertype == 'filter':
            raise IncorrectAnalyzer(analyzer, 'filter')

        if self.is_raw:
            new_data = []
            for item in self.data:
                if analyzer.filter(item):
                    new_data.append(item)
            new_info = StatusInfo(new_data, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items(): 
                new_data[key] = statusinfo.filter(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, timestamp=self.timestamp)
            return new_info


    def reduce(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an object implementing method reduce()
        :rtype : value output of reduce()
        """
        if not self.is_mutable:
            raise ObjectIsNotMutable('reduce')

        if not analyzer.analyzertype == 'reduce':
            raise IncorrectAnalyzer(analyzer, 'reduce')

        if self.is_raw:
            new_data = None
            new_data = analyzer.reduce(self.data)
            new_info = StatusInfo(new_data, is_mutable=False, timestamp=self.timestamp)
            return new_info
        else:
            new_data = {}
            for key, statusinfo in self.data.items(): 
                new_data[key] = statusinfo.reduce(analyzer)
            new_info = StatusInfo(new_data, is_raw=False, is_mutable=False, timestamp=self.timestamp)
            return new_info

    # -------------------------------------------------------------------------
    # method to get the data
    # -------------------------------------------------------------------------

    def getraw(self):
        """
        returns the structure of all raw data components
        :rtype composed data:
        """
        if self.is_raw:
            return self.data
        else:
            out = {}
            for key, value in self.data.items():
                out[key] = value.getraw()
            return out


    def get(self, *key_l):
        """
        returns the data hosted by the Info object in the tree structure pointed 
        by all keys
        The output is the data, either a dictionary or the original raw list 
        :param key_l list: list of keys for each nested dictionary
        :rtype data:
        """
        if len(key_l) == 0:
            return self.data
        else:
            key = key_l[0]
            if key not in self.data.keys():
                raise MissingKey(key)
            data = self.data[key]
            return data.get(*key_l[1:])
            

    def __getitem__(self, key):
        """
        returns the Info object pointed by the key
        :param key: the key in the higher level dictionary
        :rtype StatusInfo: 
        """
        if self.is_raw:
            raise IsRawData(key)
        if key not in self.data.keys():
            raise MissingKey(key)
        return self.data[key]


# =============================================================================
# Analyzers
# =============================================================================

class Analyzer(object):
    pass

class AnalyzerGroup(Analyzer):
    analyzertype = "group"
    def group(self):
        raise NotImplementedError

class AnalyzerFilter(Analyzer):
    analyzertype = "filter"
    def filter(self):
        raise NotImplementedError

class AnalyzerMap(Analyzer):
    analyzertype = "map"
    def map(self):
        raise NotImplementedError

class AnalyzerReduce(Analyzer):
    analyzertype = "reduce"
    def reduce(self):
        raise NotImplementedError


class Algorithm(object):
    """
    container for multiple Analyzer objects
    """
    def __init__(self):
        self.analyzer_l= []

    def add(self, analyzer):
        self.analyzer_l.append(analyzer)

    def analyze(self, input_data):
        tmp_out = input_data
        for analyzer in self.analyzer_l:
            tmp_out = tmp_out.analyze(analyzer)
        return tmp_out



# =============================================================================
#  Some basic Analyzers
# =============================================================================

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



# =============================================================================
# Exceptions
# =============================================================================

class IncorrectAnalyzer(Exception):
    def __init__(self, analyzer, methodname):
        self.value = "object %s does not have a method % methodname" %(analzyer, methodname)
    def __str__(self):
        return repr(self.value)


class MissingKey(Exception):
    def __init__(self, key):
        self.value = "Key %s is not in the data dictionary" %key
    def __str__(self):
        return repr(self.value)


class IsRawData(Exception):
    def __init__(self, key):
        self.value = "Info object is raw. It does not have key %s" %key
    def __str__(self):
        return repr(self.value)


class ObjectIsNotMutable(Exception):
    def __init__(self, method):
        self.value = "object is not mutable, method %s can not be invoked anymore" %method
    def __str__(self):
        return repr(self.value)

