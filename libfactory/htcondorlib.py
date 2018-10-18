#!/usr/bin/env python

"""
Classes and tools to facilitate the usage of the HTCondor python bindings
to interact with a collector and/or a schedd.

What these classes provide:
    * A more intuitive way to get an object representing a collector
      or a schedd.
    * Exceptions for different wrong behaviors.
    * A more intuitive way to perform queries:
        * condor_q
        * condor_history
        * condor_status
    * Logging. NullHandler is used in case there is no need for logging,
      but otherwise, if a root logger is used, its setup will be inherited.

The output of the query methods -condor_q(), condor_history() and 
condor_status()- are list of htcondor.ClassAds objects. 
They look like this example, from condor_status:
    [
        [ Name = "slot1@mysite.net"; 
          Activity = "Idle"; 
          MyType = "Machine"; 
          TargetType = "Job"; 
          State = "Unclaimed"; 
          CurrentTime = time() 
        ], 
        [ Name = "slot2@mysite.net"; 
          Activity = "Idle"; 
          MyType = "Machine"; 
          TargetType = "Job"; 
          State = "Unclaimed"; 
          CurrentTime = time() 
        ]
    ]
but they can actually be treated as dictionaries.

This module does not provide for extra functionalities to parse or analyze
the outputs of the query methods. Developers are expected to have a separate
tool, or set of tools, for that.
"""

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers
import os
import socket
import subprocess
import time
import threading

import classad
import htcondor


# =============================================================================
#               A N C I L L A R I E S 
# =============================================================================

def condor_version():
    """
    output is like
        '$CondorVersion: 8.6.12 Jul 31 2018 BuildID: 446077 $'
    """
    return htcondor.version()


def condor_config_files():
    CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
    if CONDOR_CONFIG:
        return CONDOR_CONFIG
    else:
        cmd = 'condor_config_val -config'
        subproc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = subproc.communicate()
        st = subproc.returncode
        return out


def condor_config():
    cmd = 'condor_config_val -dump'
    subproc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = subproc.communicate()
    st = subproc.returncode
    return out

# =============================================================================

def _build_constraint_str(constraint_l=None):
    """
    builds the contraint string expression for different
    queries. 
    Default is string 'true'.
    :param list constraint_l: list of constraints to be combined
    """
    if constraint_l:
        constraint_str = " && ".join(constraint_l)
    else:
        constraint_str = "true"
    return constraint_str


def _address(hostname, port=None):
    """
    builds the final string to contact a remote collector or
    remote Schedd, as '<hostname>' or '<hostname>:<port>'
    :param string hostname: the hostname or IP address of the remote service
    :param int port: [optional] port for the remote service 
    """
    hostname = socket.gethostbyaddr(hostname)[0]
    if port:
        address = '%s:%s' %(hostname, port)
    else:
        address = hostname
    return address


def _clean_cache(cache_d, now, cachingtime):
    """
    remove too old items from a caching dictionary
    :param dict cache_d: the dictionary
    :param int now: now expressed in seconds since epoch
    :param int cachingtime: number of seconds for how long to keep an item
                            in the cache
    """
    for k,v in cache_d.items():
        if now > v[0] + cachingtime:
            cache_d.pop(k)
    return cache_d


# =============================================================================
#              C O L L E C T O R   &   S C H E D D 
# =============================================================================


class HTCondorCollectorImpl(object):

    def __init__(self, hostname=None, port=None):
        """
        :param string hostname: hostname or IP of the remote collector
        :param int port: [optional] port to contact the remote collector
        """
        self.log = logging.getLogger('htcondorcollector')
        self.log.addHandler(logging.NullHandler())
        self.hostname = hostname
        self.port = port  
        self.collector = self.__getcollector()
        # Lock object to serialize the submission and query calls
        self.lock = threading.Lock() 
        self.log.debug('HTCondorCollector object initialized')


    def __getcollector(self):
        self.log.debug('starting')
        if self.hostname:
            address = _address(self.hostname, self.port)
            collector = htcondor.Collector(address)
            self.log.debug('got remote collector')
        else:
            collector = htcondor.Collector()
            self.log.debug('got local collector')
        self.__validate_collector(collector)
        return collector


    def __validate_collector(self, collector):
        """
        checks if the collector is reachable
        """
        try:
            # should return an empty list if Collector exists
            collector.query(constraint="False") 
        except Exception: 
            raise CollectorNotReachable()


    def getSchedd(self, hostname, port=None):
        """
        returns a schedd known by this collector
        :param string hostname: the hostname or IP of the remote schedd
        :param int port: [optional] port to contact the remote schedd
        :return HTCondorSchedd: 
        """
        address = _address(hostname, port)
        scheddAd = self.collector.locate(htcondor.DaemonTypes.Schedd, address) 
        try:
            schedd = htcondor.Schedd(scheddAd)
        except Exception as ex:
            self.log.critical('Unable to instantiate an Schedd object')
            raise ScheddNotReachable()
        return HTCondorSchedd(schedd, address)

    # --------------------------------------------------------------------------

    def condor_status(self, attribute_l, constraint_l=None):
        """ 
        Returns a list of ClassAd objects, output of a condor_status query. 
        :param list attribute_l: list of classads strings to be included 
            in the query 
        :param list constraint_l: [optional] list of constraints strings 
            for the query
        """
        self.log.debug('starting')
        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)
        self.log.debug('list of attributes in the query = %s' %attribute_l)
        self.log.debug('list of constraints in the query = %s' %constraint_l)
        constraint_str = _build_constraint_str(constraint_l)
        self.lock.acquire()
        out = self.collector.query(htcondor.AdTypes.Startd, constraint_str, attribute_l)
        self.lock.release()
        self.log.debug('out = %s' %out)
        return out



class HTCondorCollector(object):
    """
    overriding __new__() to make HTCondorCollector a Singleton
    """
    instances = {}

    def __new__(cls, hostname=None, port=None):

        if not hostname:
            address = 'localhost'
            if not address in HTCondorCollector.instances.keys():
                HTCondorCollector.instances[address] = HTCondorCollectorImpl()
        else:
            address = _address(hostname, port)
            if not address in HTCondorCollector.instances.keys():
                HTCondorCollector.instances[address] = HTCondorCollectorImpl(hostname, port)
 
        return HTCondorCollector.instances[address]


# =============================================================================

    
class HTCondorScheddImpl(object):

    def __init__(self, schedd=None, address=None, cachingtime=60):
        """
        :param htcondor.schedd schedd: [optional] the schedd
        :param str address: [optional] when provided, the address of the schedd
        :param int cachingtime: number of seconds to cache previous outputs. 
                                If a new query is issued before that time, 
                                the cached value is returned. 
                                Otherwise, a new one is calculated.
                                Default is 60 seconds.
        """
        self.log = logging.getLogger('htcondorschedd')
        self.log.addHandler(logging.NullHandler())
        if schedd:
            self.schedd = schedd
        else:
            try:
                self.schedd = htcondor.Schedd()  
            except Exception as ex:
                self.log.critical('Unable to instantiate an Schedd object')
                raise ScheddNotReachable()
        if address:
            self.address = address
        else:
            self.address = None
        # Lock object to serialize the submission and query calls
        self.lock = threading.Lock() 

        # cached outputs
        # the keys for these caching dictionaries is a 2-items tuple:
        #   - a hash of the list of attributes included in the query
        #   - a hash of the list of contraints included in the query
        # the values are a 2-items tuple
        #   - time stamp (in seconds since epoch) of the query
        #   - the output of the query
        self.cachingtime = cachingtime
        self.condor_q_cached_d = {}
        self.condor_history_cached_d = {}

        self.log.debug('HTCondorSchedd object initialized')


#    def __validate_schedd(self, schedd):
#        """
#        checks if the schedd is reachable
#        """
#        try:
#            # should return an "empty" iterator if Schedd exists
#            schedd.xquery(limit = 0)
#        except Exception:
#            raise ScheddNotReachable()


    # --------------------------------------------------------------------------

    def condor_q(self, attribute_l, constraint_l=None, globalquery=False):
        '''
        Returns a list of ClassAd objects, output of a condor_q query. 
        :param list attribute_l: list of classads strings to be included 
            in the query 
        :param list constraint_l: [optional] list of constraints strings 
            for the query
        :param bool globalquery: when True, query all schedds in the pool    
        :return list: list of ClassAd objects
        '''
        self.log.debug('starting')

        if globalquery:
            out = []
            schedd_ad_l = htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd)
            for schedd_ad in schedd_ad_l:
                address = schedd_ad['MyAddress']
                self.log.debug('Quering Schedd with address %s' %address)
                schedd = HTCondorSchedd(htcondor.Schedd(schedd_ad), address) 
                out += schedd.condor_q(attribute_l, constraint_l)
            return out
        # if not globalquery...

        now = int(time.time())
        self.condor_q_cached_d = _clean_cache(self.condor_q_cached_d, now, self.cachingtime)

        # sorting input lists, needed for them to become keys in the caching
        attribute_l.sort()
        if constraint_l:
            constraint_l.sort()

        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)
    
        self.log.debug('list of attributes in the query = %s' %attribute_l)
        self.log.debug('list of constraints in the query = %s' %constraint_l)
    
        constraint_str = _build_constraint_str(constraint_l)

        try:
            # check if there is a cached value
            key = (hash(str(attribute_l)), hash(constraint_str))
            cached_output = self.condor_q_cached_d[key]
            self.log.debug('Found previous output for this query in the cached.')
            out = cached_output[1]
        except:
            self.log.debug('There is no cached value for this query. Getting new one.')
            out = self.__condor_q(constraint_str, attribute_l)
            self.condor_q_cached_d[key] = (now, out)

        self.log.debug('out = %s' %out)
        return out


    def __condor_q(self, constraint_str, attribute_l):
        """
        the actual query
        """
        self.lock.acquire() 
        out = self.schedd.query(constraint_str, attribute_l)
        self.lock.release() 
        return out

    # --------------------------------------------------------------------------
    
    def condor_history(self, attribute_l, constraint_l=None, globalquery=False):
        """
        Returns a list of ClassAd objects, output of a condor_history query. 
        :param list attribute_l: list of classads strings to be included 
            in the query 
        :param list constraint_l: [optional] list of constraints strings 
            for the query
        :param bool globalquery: when True, query all schedds in the pool
        :return list: list of ClassAd objects
        """
        self.log.debug('starting')

        if globalquery:
            out = []
            schedd_ad_l = htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd)
            for schedd_ad in schedd_ad_l:
                address = schedd_ad['MyAddress']
                self.log.debug('Quering Schedd with address %s' %address)
                schedd = HTCondorSchedd(htcondor.Schedd(schedd_ad), address)
                out += schedd.condor_history(attribute_l, constraint_l)
            return out
        # if not globalquery...


        now = int(time.time())
        self.condor_history_cached_d = _clean_cache(self.condor_history_cached_d, now, self.cachingtime)

        # sorting input lists, needed for them to become keys in the caching
        attribute_l.sort()
        if constraint_l:
            constraint_l.sort()

        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)

        self.log.debug('list of attributes in the query = %s' %attribute_l)
        self.log.debug('list of constraints in the query = %s' %constraint_l)

        constraint_str = _build_constraint_str(constraint_l)

        try:
            # check if there is a cached value
            key = (hash(str(attribute_l)), hash(constraint_str))
            cached_output = self.condor_history_cached_d[key]
            self.log.debug('Found previous output for this query in the cached.')
            out = cached_output[1]
        except:
            self.log.debug('There is no cached value for this query. Getting new one.')
            out = self.__condor_history(constraint_str, attribute_l)
            self.condor_history_cached_d[key] = (now, out)

        self.log.debug('out = %s' %out)
        return out


    def __condor_history(self, constraint_str, attribute_l):
        """
        the actual query 
        """
        self.lock.acquire() 
        out = self.schedd.history(constraint_str, attribute_l, 0)
        self.lock.release() 
        out = list(out)
        return out

    # --------------------------------------------------------------------------

    def condor_rm(self, jobid_l):
        """
        remove a list of jobs from the queue in this schedd.
        :param list jobid_l: list of strings "ClusterId.ProcId"
        """
        self.log.debug('starting')
        self.log.debug('list of jobs to kill = %s' %jobid_l)
        self.lock.acquire()
        self.schedd.act(htcondor.JobAction.Remove, jobid_l)
        self.lock.release()
        self.log.debug('finished')

    # --------------------------------------------------------------------------

    def condor_submit(self, jsd, n):
        """
        performs job submission from a string representation 
        of the submit file. The string containing the submit file should not
        contain the "queue" statement, as the number of jobs is being passed
        as a separate argument.
        :param JobSubmissionDescription jsd: instance of JobSubmissionDescription 
        :param int n: number of jobs to submit
        :return int: the clusterid of jobs submitted
        """
        self.log.debug('starting')

        submit_d = jsd.items()
        self.log.debug('dictionary for submission = %s' %submit_d)
        if not bool(submit_d):
            raise EmptySubmitFile()

        self.lock.acquire() 
        clusterid = self.__submit(jsd, n)
        self.lock.release() 

        self.log.debug('finished submission for clusterid %s' %clusterid)
        return clusterid


    def __submit(self, jsd, n):
        """
        :param JobSubmissionDescription jsd: instance of JobSubmissionDescription 
        :param int n: number of jobs to submit
        :return int: the clusterid of jobs submitted
        """
        version = condor_version()
        version_num = version.split()[1]
        if version_num >= '8.7': 
            clusterid = self.__submit_python(jsd, n)
        else:
            clusterid = self.__submit_from_file(jsd)
        return clusterid


    def __submit_python(self, jsd, n):
        """
        submit using the python bindings
        :param JobSubmissionDescription jsd: instance of JobSubmissionDescription 
        :param int n: number of jobs to submit
        :return int: the clusterid of jobs submitted
        """
        submit_d = jsd.items()
        submit = htcondor.Submit(submit_d)
        with self.schedd.transaction() as txn:
            clusterid = submit.queue(txn, n)
        return clusterid


    def __submit_from_file(self, jsd):
        """
        temporary solution to submit in a subshell from submit file
        :param JobSubmissionDescription jsd: instance of JobSubmissionDescription 
        :return int: the clusterid of jobs submitted
        """
        cmd = 'condor_submit -verbose %s' %jsd.path
        subproc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = subproc.communicate()
        st = subproc.returncode

        for line in out.split('\n'):
            if line.strip().startswith('**'):
                procid = line.split()[-1].split('.')[0]
        return int(procid) 


class HTCondorSchedd(object):
    """
    overriding __new__() to make HTCondorSchedd a Singleton
    """
    instances = {}

    def __new__(cls, schedd=None, address=None, cachingtime=60):

        if not address:
            address = 'localhost'
            if not address in HTCondorSchedd.instances.keys():
                HTCondorSchedd.instances[address] = HTCondorScheddImpl(schedd, cachingtime=cachingtime)
        else:
            address = address
            if not address in HTCondorSchedd.instances.keys():
                HTCondorSchedd.instances[address] = HTCondorScheddImpl(schedd, address, cachingtime=cachingtime)

        return HTCondorSchedd.instances[address]


# =============================================================================

class JobSubmissionDescription(object):
    """
    class to manage the content of the submission file,
    or its equivalent in several formats
    """

    def __init__(self):
        """
        _jsd_d is a dictionary of submission file expressions
        _n is the number of jobs to submit
        """
        self.log = logging.getLogger('jobsubmissiondescription')
        self.log.addHandler(logging.NullHandler())
        self._jsd_d = {}
        self._n = 0
        self.path = None


    def loadf(self, path):
        """
        gets the submission content from a file
        :param str path: path to the submission file
        """
        try:
            with open(path) as f:
                self.loads(f.read())
            self.path = path
        except MalformedSubmitFile as ex:
            raise ex 
        except EmptySubmitFile as ex:
            raise ex 
        except Exception as ex:
            self.log.error('file %s cannot be read' %path)
            raise ErrorReadingSubmitFile(path)


    def loads(self, jsd_str):
        """
        gets the submission content from a string
        :param str jsd_str: single string with the submission content
        """
        self.log.debug('starting')
        for line in jsd_str.split('\n'):
            if line.startswith('queue '):
                # the "queue" statement should not be part of the
                # submit file string, but it is harmless
                continue
            if line.strip() == '':
                continue
            try:
                fields = line.split('=')
                key = fields[0].strip()
                value = '='.join(fields[1:]).strip()
                self.add(key, value)
            except Exception:
                raise MalformedSubmitFile(line)
        self.log.debug('dictionary for submission = %s' %self._jsd_d)
        if not bool(self._jsd_d):
            raise EmptySubmitFile()


    def dumpf(self, path):
        """
        write the submission content into a file
        :param str path: path to the file
        """
        str = self.dumps()
        try:
            with open(path, "w") as f:
                f.write(str)
            self.path = path
        except Exception as ex:
            self.log.error('file %s cannot be written' %path)
            raise ErrorWritingSubmitFile(path)


    def dumps(self):
        """
        returns the submission content as a single string
        :rtype str:
        """
        str = ""
        for pair in self._jsd_d.items():
            str += '%s = %s\n' %pair
        str += 'queue %s\n' %self._n
        return str


    def add(self, key, value):
        """
        adds a new key,value pair submission expression
        to the dictionary
        :param str key: the submission expression key
        :param str value: the submission expression value
        """
        self._jsd_d[key] = value


    def setnjobs(self, n):
        """
        sets the number of jobs to submit
        :param int n: the number of jobs to submit
        """
        if n<0:
            raise NegativeSubmissionNumber(n)
        self._n = n


    def items(self):
        """
        returns the content as a dict of submission items
        :rtype dict:
        """
        return self._jsd_d


    def getnjobs(self):
        """
        returns the number of jobs to submit
        :rtype int:
        """
        return self._n


# =============================================================================
#   Exceptions
# =============================================================================

class CollectorNotReachable(Exception):
    def __init__(self):
        self.value = "Collector not reachable"
    def __str__(self):
        return repr(self.value)

class ScheddNotReachable(Exception):
    def __init__(self):
        self.value = "Schedd not reachable"
    def __str__(self):
        return repr(self.value)

class EmptySubmitFile(Exception):
    def __init__(self):
        self.value = "submit file is emtpy"
    def __str__(self):
        return repr(self.value)

class MalformedSubmitFile(Exception):
    def __init__(self, line):
        self.value = 'line %s in submit file does not have the right format'
    def __str__(self):
        return repr(self.value)

class IncorrectInputType(Exception):
    def __init__(self, name, type):
        self.value = 'Input option %s is not type %s' %(name, type)
    def __str__(self):
        return repr(self.value)

class NegativeSubmissionNumber(Exception):
    def __init__(self, value):
        self.value = "Negative number of jobs to submit: %s" %value
    def __str__(self):
        return repr(self.value)

class ErrorReadingSubmitFile(Exception):
    def __init__(self, value):
        self.value = "Unable to read the submit file %s" %value
    def __str__(self):
        return repr(self.value)

class ErrorWritingSubmitFile(Exception):
    def __init__(self, value):
        self.value = "Unable to write the submit file %s" %value
    def __str__(self):
        return repr(self.value)
