#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

"""
Code to store and manipulate data.

-------------------------------------------------------------------------------
                            class StatusInfo
-------------------------------------------------------------------------------

This is the only class implemented that is meant to be public.

The data stored by instances of class StatusInfo must be a list of items. 
These items can be anything, including objects. 
A typical example is data is a list of HTCondor ClassAds, where each 
item in the data list represents an HTCondor job.

Class StatusInfo has several methods to manipulate the data, 
but in all cases the output of the method is a new instance of one of the 
classes implemented: StatusInfo, _DictStatusInfo, etc.
Methods never modify the current instance data.
This allows to perform different manipulations from the same source object.

There are two types of methods in class StatusInfo:

    - methods whose object output accepts further processing.
      Examples are methods indexby(), filter(), and map().

    - methods whose object output can not be processed anymore.
      An attempt to call any method on these instances
      will raise an Exception.
      Examples are methods reduce(), and process().

The method indexby() is somehow special. 
It is being used to split the stored data into a dictionary, 
according to whatever rule is provided. 
The values of this dictionary are themselves new StatusInfo instances. 
Therefore, the output of calling indexby() once is an _DictStatusInfo object 
with data:
        
    self.data = {
                 key1: <StatusInfo>,
                 key2: <StatusInfo>,
                 ...
                 keyN: <StatusInfo>
                }

-------------------------------------------------------------------------------

The UML source for the classes is as follows:

        @startuml
        
        object <|-- _Base
        
        _Base <|-- _BaseDict 
        _Base <|-- StatusInfo 
        _Base <|-- _NonMutableStatusInfo 

        _AnalysisInterface <|-- StatusInfo  
        _AnalysisInterface <|-- _DictStatusInfo 

        _BaseDict <|-- _DictStatusInfo 
        _BaseDict <|-- _NonMutableDictStatusInfo 

        _GetRawBase <|-- StatusInfo 
        _GetRawBase <|-- _NonMutableStatusInfo 
        
        @enduml


                                                            +--------+      
                                                            | object |
                                                            +--------+
                                                               ^
                                                               |
 +--------------------+                                     +-------+
 | _AnalysisInterface |    +------------------------------->| _Base |<-----------------+                  
 +--------------------+    |                                +-------+                  |
   ^                ^      |        +-------------+              ^              +-----------+              
   |                |      |        | _GetRawBase |              |              | _BaseDict |       
   |                |      |        +-------------+              |              +-----------+      
   |                |      |          ^        ^                 |                ^      ^     
   |                |      |          |        |                 |                |      |   
   |                |      |          |        |                 |                |      |   
   |                |      |          |        |                 |                |      |   
   |                |      |          |        |                 |                |      |   
   |             +------------+       |        |   +-----------------------+      |      |
   |             | StatusInfo |-------+        +---| _NonMutableStatusInfo |      |      |
   |             +------------+                    +-----------------------+      |      |
   |                                    +-----------------+                       |  +---------------------------+
   +------------------------------------| _DictStatusInfo |-----------------------+  | _NonMutableDictStatusInfo |
                                        +-----------------+                          +---------------------------+




-------------------------------------------------------------------------------
                            Analyzers 
-------------------------------------------------------------------------------


The input to all methods is an object of type Analyzer. 
Analyzers are classes that implement the rules or policies to be used 
for each method call.  
For example: 
    - a call to method indexby() expects an object of type AnalyzerIndexBy
    - a call to method map() expects an object of type AnalyzerMap
    - a call to method reduce() expects an object of type AnalyzerReduce
    - etc.

Each Analyzer object must have implemented a method 
with the same name that the StatusInfo's method it is intended for. 
For exmple:

    - classes AnalyzerIndexBy must implement method indexby()
    - classes AnalyzerMap must implement method map()
    - classes AnalyzerReduce must implement method reduce()
    - ...

Passing an analyzer object that does not implement the right method will 
raise an IncorrectAnalyzer Exception.

A few basic pre-made Analyzers have been implemented, ready to use. 
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
#  Decorators 
#
#   Note:
#   the decorator must be implemented before the classes using it 
#   otherwise, they do not find it
# =============================================================================

def validate_call(method):
    """
    validates calls to the processing methods.
    Checks: 
        * if the StatusInfo object is mutable or not, 
        * if a method is being called with the right type of Analyzer
    Exceptions are raised with some criteria is not met.
    """
    def wrapper(self, analyzer, *k, **kw):
        method_name = method.__name__
        analyzertype = analyzer.analyzertype
        if not analyzertype == method_name:
            msg = 'Analyzer object {obj} is not type {name}. Raising exception.'
            msg = msg.format(obj = analyzer,
                             name = method_name)
            self.log.error(msg)
            raise IncorrectAnalyzer(analyzer, analyzertype, method_name)
        out = method(self, analyzer, *k, **kw)
        return out
    return wrapper


def catch_exception(method):
    """
    catches any exception during data processing
    and raises an AnalyzerFailure exception
    """
    def wrapper(self, analyzer):
        try:
            out = method(self, analyzer)
        except Exception as ex:
            msg = 'Exception of type "%s" ' %ex.__class__.__name__
            msg += 'with content "%s" ' %ex
            msg += 'while calling "%s" ' %method.__name__
            msg += 'with analyzer "%s"' %analyzer
            raise AnalyzerFailure(msg)
        else:
            return out
    return wrapper



# =============================================================================
# Base classes and interfaces
# =============================================================================

class _Base(object):

    def __init__(self, data, timestamp=None):
        """ 
        :param data: the data to be recorded
        :param timestamp: the time when this object was created
        """ 
        self.log = logging.getLogger('info')
        self.log.addHandler(logging.NullHandler())

        msg ='Initializing object with input options: \
data={data}, timestamp={timestamp}'
        msg = msg.format(data=data,
                         timestamp=timestamp)
        self.log.debug(msg)

        self.data = data 

        if not timestamp:
            timestamp = int(time.time())
            msg = 'Setting timestamp to %s' %timestamp
            self.log.debug(msg)
        self.timestamp = timestamp

        self.log.debug('Object initialized')


    def get(self, *key_l):
        """
        returns the data hosted by the Info object in the 
        tree structure pointed by all keys
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


class _BaseDict(_Base):
    """
    adds an extra check for the input data
    """
    def __init__(self, data, timestamp=None):
        super(_BaseDict, self).__init__(data, timestamp)
        if type(self.data) is not dict:
            raise IncorrectInputDataType(dict)

    def getraw(self):
        out = {}
        for key, value in self.data.items():
            out[key] = value.getraw()
        return out

    def __getitem__(self, key):
        """
        returns the Info object pointed by the key
        :param key: the key in the higher level dictionary
        :rtype StatusInfo: 
        """
        if key not in self.data.keys():
            raise MissingKey(key)
        return self.data[key]

# extra get methods

class _GetRawBase:

    def getraw(self):
        return self.data


# interfaces 

class _AnalysisInterface:

    def indexyby(self, analyzer):
        raise NotImplementedError

    def map(self, analyzer):
        raise NotImplementedError

    def filter(self, analyzer):
        raise NotImplementedError

    def reduce(self, analyzer):
        raise NotImplementedError

    def transform(self, analyzer):
        raise NotImplementedError

    def process(self, analyzer):
        raise NotImplementedError


# =============================================================================
# Info class
# =============================================================================

class StatusInfo(_Base, _AnalysisInterface, _GetRawBase):

    def __init__(self, data, timestamp=None):
        super(StatusInfo, self).__init__(data, timestamp)
        if type(self.data) is not list:
            msg = 'Input data %s is not a dict. Raising exception' %data
            self.log.error(msg)
            raise IncorrectInputDataType(list)


    def analyze(self, analyzer):
        """
        generic method that picks the right one 
        based on the type of analyzer
        :param analyzer: an Analyzer object 
        :rtype StatusInfo:
        """
        self.log.debug('Starting')
        if analyzer.analyzertype == 'indexby':
            return self.indexby(analyzer)
        elif analyzer.analyzertype == 'filter':
            return self.filter(analyzer)
        elif analyzer.analyzertype == 'map':
            return self.map(analyzer)
        elif analyzer.analyzertype == 'reduce':
            return self.reduce(analyzer)
        elif analyzer.analyzertype == 'transform':
            return self.transform(analyzer)
        elif analyzer.analyzertype == 'process':
            return self.process(analyzer)
        else:
            msg = 'Input object %s is not a valid analyzer. Raising exception.'
            self.log.error(msg)
            raise NotAnAnalyzer()


    def apply_algorithm(self, algorithm):
        """
        invoke all steps in an Algorithm object
        and returns the final output
        :param Algorithm algorithm: 
        :rtype StatusInfo:
        """
        return algorithm.analyze(self)

    # -------------------------------------------------------------------------
    # methods to manipulate the data
    # -------------------------------------------------------------------------

    @validate_call
    def indexby(self, analyzer):
        """
        groups the items recorded in self.data into a dictionary
        and creates a new StatusInfo object with it. 
           1. make a dictinary grouping items according to rules in analyzer
           2. convert that dictionary into a dictionary of StatusInfo objects
           3. make a new StatusInfo with that dictionary
        :param analyzer: an instance of AnalyzerIndexBy-type class 
                         implementing method indexby()
        :rtype StatusInfo:
        """
        self.log.debug('Starting with analyzer %s' %analyzer)

        new_data = self.__indexby(analyzer)
        new_info = _DictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __indexby(self, analyzer):
        # 1
        tmp_new_data = {} 
        for item in self.data:
            key = analyzer.indexby(item)
            if key is not None:
                if key not in tmp_new_data.keys():
                    tmp_new_data[key] = []
                tmp_new_data[key].append(item) 
        # 2
        new_data = {}
        for k, v in tmp_new_data.items():
            new_data[k] = StatusInfo(v, timestamp=self.timestamp)

        return new_data


    # -------------------------------------------------------------------------

    @validate_call
    def map(self, analyzer):
        """
        modifies each item in self.data according to rules
        in analyzer
        :param analyzer: an instance of AnalyzerMap-type class 
                         implementing method map()
        :rtype StatusInfo:
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__map(analyzer)
        new_info = StatusInfo(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __map(self, analyzer):
        new_data = []
        for item in self.data:
            new_item = analyzer.map(item)
            new_data.append(new_item)
        return new_data
    
    # -------------------------------------------------------------------------

    @validate_call
    def filter(self, analyzer):
        """
        eliminates the items in self.data that do not pass
        the filter implemented in analyzer
        :param analyzer: an instance of AnalyzerFilter-type class 
                         implementing method filter()
        :rtype StatusInfo:
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__filter(analyzer)
        new_info = StatusInfo(new_data, timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __filter(self, analyzer):
        new_data = []
        for item in self.data:
            if analyzer.filter(item):
                new_data.append(item)
        return new_data

    # -------------------------------------------------------------------------

    @validate_call
    def reduce(self, analyzer):
        """
        process the entire self.data at the raw level and accumulate values
        :param analyzer: an instance of AnalyzerReduce-type class 
                         implementing method reduce()
        :rtype StatusInfo: 
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__reduce(analyzer)
        new_info = _NonMutableStatusInfo(new_data, 
                              timestamp=self.timestamp)
        return new_info

    @catch_exception
    def __reduce(self, analyzer):
        value = analyzer.init_value
        for item in self.data:
            value = analyzer.reduce(value, item) 
        return value

    # -------------------------------------------------------------------------

    @validate_call
    def transform(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an instance of AnalyzerTransform-type class 
                         implementing method transform()
        :rtype StatusInfo: 
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__transform(analyzer)
        new_info = StatusInfo(new_data, timestamp=self.timestamp)
        return new_info
        
    @catch_exception
    def __transform(self, analyzer):
        new_data = analyzer.transform(self.data)
        return new_data

    # -------------------------------------------------------------------------

    @validate_call
    def process(self, analyzer):
        """
        process the entire self.data at the raw level
        :param analyzer: an instance of AnalyzerProcess-type class 
                         implementing method process()
        :rtype StatusInfo: 
        """
        self.log.debug('Starting with analyzer %s' %analyzer)
        new_data = self.__process(analyzer)
        new_info = _NonMutableStatusInfo(new_data, timestamp=self.timestamp)
        return new_info
        
    @catch_exception
    def __process(self, analyzer):
        new_data = analyzer.process(self.data)
        return new_data

# =============================================================================

class _DictStatusInfo(_BaseDict, _AnalysisInterface):

    # -------------------------------------------------------------------------
    # methods to manipulate the data
    # -------------------------------------------------------------------------

    @validate_call
    def indexby(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items():
            self.log.debug('calling indexby() for content in key %s'%key)
            new_data[key] = statusinfo.indexby(analyzer)
        new_info = _DictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info
    

    @validate_call
    def map(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items():
            self.log.debug('calling map() for content in key %s'%key)
            new_data[key] = statusinfo.map(analyzer)
        new_info = _DictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def filter(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items(): 
            self.log.debug('calling filter() for content in key %s'%key)
            new_data[key] = statusinfo.filter(analyzer)
        new_info = _DictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def reduce(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items(): 
            self.log.debug('calling reduce() for content in key %s'%key)
            new_data[key] = statusinfo.reduce(analyzer)
        new_info = _NonMutableDictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def transform(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items(): 
            self.log.debug('calling transform() for content in key %s'%key)
            new_data[key] = statusinfo.transform(analyzer)
        new_info = _DictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info


    @validate_call
    def process(self, analyzer):
        new_data = {}
        for key, statusinfo in self.data.items(): 
            self.log.debug('calling process() for content in key %s'%key)
            new_data[key] = statusinfo.process(analyzer)
        new_info = _NonMutableDictStatusInfo(new_data, timestamp=self.timestamp)
        return new_info


class _NonMutableStatusInfo(_Base, _GetRawBase):
    pass

class _NonMutableDictStatusInfo(_BaseDict):
    pass


# =============================================================================
# Analyzers
# =============================================================================

class Analyzer(object):
    pass


class AnalyzerIndexBy(Analyzer):
    analyzertype = "indexby"
    def indexby(self):
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
    def __init__(self, init_value=None):
        self.init_value = init_value
    def reduce(self):
        raise NotImplementedError


class AnalyzerProcess(Analyzer):
    analyzertype = "process"
    def process(self):
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
#  Some basic pre-made Analyzers
# =============================================================================

class IndexByKey(AnalyzerIndexBy):

    def __init__(self, key):
        self.key = key

    def indexby(self, job):
        try:
            return job[self.key]
        except Exception:
            return None


class IndexByKeyRemap(AnalyzerIndexBy):

    def __init__(self, key, mapping_d):
        self.key = key
        self.mapping_d = mapping_d

    def indexby(self, job):
        try:
            value = str(job[self.key])
        except Exception:
            return None

        if value in self.mapping_d.keys():
            return self.mapping_d[value]
        else:
            return None


class AttributeValue(AnalyzerFilter):

    def __init__(self, attribute, value):
        self.attribute = attribute 
        self.value = value

    def filter(self, job):
        if self.attribute not in job.keys():
            msg = 'job {job} does not have key {key}.'
            msg = msg.format(job=job, 
                             key=self.attribute)
            logmsg = msg + ' Raising Exception.'
            self.log.error(logmsg)
            raise AnalyzerFailure(msg)
        return job[self.attribute] == self.value


class Count(AnalyzerProcess):

    def process(self, data):
        return len(data)


class TotalRunningTimeFromRunningJobs(AnalyzerReduce):

    def __init__(self):
        self.now = int(time.time())
        super(TotalRunningTimeFromRunningJobs, self).__init__(0)

    def reduce(self, value, job):
        running = self.now - int(job['enteredcurrentstatus'])
        if value:
            running += value
        return running


class TotalRunningTimeFromRunningAndFinishedJobs(AnalyzerReduce):

    def __init__(self):
        self.now = int(time.time())
        super(TotalRunningTimeFromRunningAndFinishedJobs, self).__init__(0)

    def reduce(self, value, job):
        if job['jobstatus'] == 2:
            running = self.now - int(job['enteredcurrentstatus'])
        elif job['jobstatus'] == 3 or \
             job['jobstatus'] == 4:
            running = int(job['remotewallclocktime'])
        else:
            running = 0

        if value:
            running += value
        return running


class IdleTime(AnalyzerMap):

    def __init__(self):
        self.now = int(time.time())

    def map(self, job):
        return self.now - int(job['enteredcurrentstatus'])


class ApplyFunction(AnalyzerProcess):

    def __init__(self, func):
        self.func = func

    def process(self, data):
        if data:
            return self.func(data)
        else:
            return None


# =============================================================================
# Exceptions
# =============================================================================

class IncorrectInputDataType(Exception):
    def __init__(self, type):
        self.value = 'Type of input data is not %s' %type
    def __str__(self):
        return repr(self.value)


class NotAnAnalyzer(Exception):
    def __init__(self):
        self.value = 'object does not have a valid analyzertype value'
    def __str__(self):
        return repr(self.value)


class IncorrectAnalyzer(Exception):
    def __init__(self, analyzer, analyzertype, methodname):
        value = "Analyzer object {ana} is of type '{atype}' but used for '{call}()'" 
        self.value = value.format(ana=analyzer, 
                                  atype=analyzertype, 
                                  call=methodname)
    def __str__(self):
        return repr(self.value)


class MissingKey(Exception):
    def __init__(self, key):
        self.value = "Key %s is not in the data dictionary" %key
    def __str__(self):
        return repr(self.value)


class AnalyzerFailure(Exception):
    """
    generic Exception for any unclassified failure
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


# =============================================================================
#   class DataItem 
# =============================================================================

class DataItem(object):
    """
    class to store an arbitrary dictionary, 
    and read them as they were attributes
    """

    def __init__(self, data_d={}, default=0, timestamp=None):
        """
        :param dict data_d: input data
        :param default: default value to return when the attribute 
                        is being tried to read
                        is not a key in the dictionary
        """
        self.log = logging.getLogger('info')
        self.log.addHandler(logging.NullHandler())

        msg ='Initializing object with input options: \
data_d={data_d}, default={default}, timestamp={timestamp}'
        msg = msg.format(data_d=data_d,
                         default=default,
                         timestamp=timestamp)
        self.log.debug(msg)

        self._data_d = data_d
        self._default = default
        if not timestamp:
            timestamp = int(time.time())
            msg = 'Setting timestamp to %s' %timestamp
            self.log.debug(msg)
        self.timestamp = timestamp
            

    def __getattr__(self, attr):
        """
        read the values in the dictionary
        as the keys of the dictionary were
        attributes of the class.
        For example, self.foo allows to read 
        the content of self.data_d['foo'] 
        """
        return self._data_d.get(attr, self._default)


    def __setitem__(self, attr, value):
        """
        to allow using [] as if this class were actually a dict.
        :param attr: the key 
        :param value: the value 
        """
        self._data_d[attr] = value


    def __getitem__(self, attr):
        """
        to allow using [] as if this class were actually a dict.
        :param attr: the key 
        """
        return self.__getattr__(attr)


    def __str__(self):
        str_l = []
        for pair in self._data_d.items():
            s = '%s: %s' %pair
            str_l.append(s)
        return ', '.join(str_l)


    def __repr__(self):
        s = str(self)
        return s


