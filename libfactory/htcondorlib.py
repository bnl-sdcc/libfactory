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
import threading

import classad
import htcondor


# =============================================================================
#               A N C I L L A R I E S 
# =============================================================================

def condor_version():
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


# =============================================================================
#              C O L L E C T O R   &   S C H E D D 
# =============================================================================

class HTCondorRemoteScheddWrapper(object):
    """
    trivial class just to host in a single object 
      * an htcondor.Schedd() object 
      * its address 
    """
    def __init__(self, schedd, address):
        self.schedd = schedd
        self.address = address


class _HTCondorCollector(object):

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
        return collector


    # FIXME: right now this is orphan code
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
        schedd = htcondor.Schedd(scheddAd)
        #return HTCondorSchedd(schedd)
        scheddwrap = HTCondorRemoteScheddWrapper(schedd, address)
        return HTCondorSchedd(scheddwrap)

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
        out = self.collector.query(htcondor.AdTypes.Startd, constraint_str, attribute_l)
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
                HTCondorCollector.instances[address] = _HTCondorCollector()
        else:
            address = _address(hostname, port)
            if not address in HTCondorCollector.instances.keys():
                HTCondorCollector.instances[address] = _HTCondorCollector(hostname, port)
 
        return HTCondorCollector.instances[address]


# =============================================================================

    
class _HTCondorSchedd(object):

    def __init__(self, scheddwrap=None):
        """
        :param HTCondorRemoteScheddWrapper scheddwrap: [optional] when provided, 
            the current object is built on it to contact a remote schedd.
            Otherwise, a local schedd is assumed.
        """
        self.log = logging.getLogger('htcondorschedd')
        self.log.addHandler(logging.NullHandler())
        if scheddwrap:
            self.schedd = scheddwrap.schedd
            self.address = scheddwrap.address
        else:
            self.schedd = htcondor.Schedd()  
            self.address = None
	    # Lock object to serialize the submission and query calls
	    self.lock = threading.Lock() 

        self.log.debug('HTCondorSchedd object initialized')


    # FIXME: right now this is orphan code
    def __validate_schedd(self, schedd):
        """
        checks if the schedd is reachable
        """
        try:
            # should return an "empty" iterator if Schedd exists
            schedd.xquery(limit = 0)
        except Exception:
            raise ScheddNotReachable()

    # --------------------------------------------------------------------------

    def condor_q(self, attribute_l, constraint_l=None):
        '''
        Returns a list of ClassAd objects, output of a condor_q query. 
        :param list attribute_l: list of classads strings to be included 
            in the query 
        :param list constraint_l: [optional] list of constraints strings 
            for the query
        :return list: list of ClassAd objects
        '''
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
        out = self.schedd.query(constraint_str, attribute_l)
        self.lock.release() 
        self.log.debug('out = %s' %out)
        return out

    
    def condor_history(self, attribute_l, constraint_l=None):
        """
        Returns a list of ClassAd objects, output of a condor_history query. 
        :param list attribute_l: list of classads strings to be included 
            in the query 
        :param list constraint_l: [optional] list of constraints strings 
            for the query
        :return list: list of ClassAd objects
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
        out = self.schedd.history(constraint_str, attribute_l, 0)
        self.lock.release() 
        out = list(out)
        self.log.debug('out = %s' %out)
        return out


    def condor_rm(self, jobid_l):
        """
        remove a list of jobs from the queue in this schedd.
        :param list jobid_l: list of strings "ClusterId.ProcId"
        """
        self.log.debug('starting')
        self.log.debug('list of jobs to kill = %s' %jobid_l)
        self.schedd.act(htcondor.JobAction.Remove, jobid_l)
        self.log.debug('finished')
    

    def condor_submit(self, jdl_str, n):
        """
        performs job submission from a string representation 
        of the submit file. The string containing the submit file should not
        contain the "queue" statement, as the number of jobs is being passed
        as a separate argument.
        :param str jdl_str: single string with the content of the submit file
        :param int n: number of jobs to submit
        """
        self.log.debug('starting')
        submit_d = {}
        for line in jdl_str.split('\n'):
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
                submit_d[key] = value
            except Exception:
                raise MalformedSubmitFile(line)
        self.log.debug('dictionary for submission = %s' %submit_d)
        if not bool(submit_d):
            raise EmptySubmitFile()
        
        self.lock.acquire() 
        submit = htcondor.Submit(submit_d)
        with self.schedd.transaction() as txn:
            clusterid = submit.queue(txn, n)
        self.lock.release() 
        self.log.debug('finished submission for clusterid %s' %clusterid)
        return clusterid


class HTCondorSchedd(object):
    """
    overriding __new__() to make HTCondorSchedd a Singleton
    """
    instances = {}

    def __new__(cls, scheddwrap=None):

        if not scheddwrap:
            address = 'localhost'
            if not address in HTCondorSchedd.instances.keys():
                HTCondorSchedd.instances[address] = _HTCondorSchedd()
        else:
            address = scheddwrap.address
            if not address in HTCondorSchedd.instances.keys():
                HTCondorSchedd.instances[address] = _HTCondorSchedd(scheddwrap)

        return HTCondorSchedd.instances[address]



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


