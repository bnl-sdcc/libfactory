#!/usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers
import socket

import classad
import htcondor


# =============================================================================
#               A N C I L L A R I E S 
# =============================================================================

def _build_constraint_str(constraint_l=None):
    """
    """
    if constraint_l:
        constraint_str = " && ".join(constraint_l)
    else:
        constraint_str = "true"
    return constraint_str


def _address(hostname, port=None):
    """
    """
    hostname = socket.gethostbyaddr(hostname)[0]
    if port:
        address = '%s:%s' %(hostname, port)
    else:
        address = hostname
    return address

# =============================================================================


class HTCondorPool(object):

    def __init__(self, hostname=None, port=None):
        """
        :param string remotecollector: hostname of the collector
        """
        self.log = logging.getLogger('htcondorpool')
        self.log.addHandler(logging.NullHandler())
        self.hostname = hostname
        self.port = port  
        self.collector = self.__getcollector()
        self.log.debug('HTCondorPool object initialized')


    def __getcollector(self):
        """
        """
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
        except Exception, ex: 
            raise CollectorNotReachable()


    def getSchedd(self, hostname, port=None):
        """
        """
        address = _address(hostname, port)
        scheddAd = self.collector.locate(htcondor.DaemonTypes.Schedd, address) 
        schedd = htcondor.Schedd(scheddAd)
        return HTCondorSchedd(schedd)

    # --------------------------------------------------------------------------

    def condor_status(self, attribute_l, constraint_l=None):
        """ 
        Equivalent to condor_status
        We query for a few specific ClassAd attributes 
        (faster than getting everything)
        Output of collector.query(htcondor.AdTypes.Startd) looks like
         [
          [ Name = "slot1@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ], 
          [ Name = "slot2@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ]
         ]
        :param list attribute_l: list of classads strings to include in the query 
        :param list constraint_l: list of constraints strings in the status query
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



    
class HTCondorSchedd(object):

    def __init__(self, schedd=None):
        """
        """
        self.log = logging.getLogger('htcondorschedd')
        self.log.addHandler(logging.NullHandler())
        if schedd:
            self.schedd = schedd 
        else:
            self.schedd = htcondor.Schedd()  
        self.log.debug('HTCondorSchedd object initialized')


    def __validate_schedd(self, schedd):
        """
        checks if the schedd is reachable
        """
        try:
            # should return an "empty" iterator if Schedd exists
            schedd.xquery(limit = 0)
        except Exception, ex:
            raise ScheddNotReachable()

    # --------------------------------------------------------------------------

    def condor_q(self, attribute_l, constraint_l=None):
        '''
        Returns a list of ClassAd objects. 
        :param list attribute_l: list of classads strings to include in the query 
        :param list constraint_l: list of constraints strings in the query
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
        out = self.schedd.query(constraint_str, attribute_l)
        self.log.debug('out = %s' %out)
        return out

    
    def condor_history(self, attribute_l, constraint_l=None):
        """
        :param list attribute_l: list of classads strings to include in the query 
        :param list constraint_l: list of constraints strings in the history query
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
        out = self.schedd.history(constraint_str, attribute_l, 0)
        out = list(out)
        self.log.debug('out = %s' %out)
        return out


    def condor_rm(self, jobid_l):
        """
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
            if line.strip() == '':
                continue
            try:
                fields = line.split('=')
                key = fields[0].strip()
                value = '='.join(fields[1:]).strip()
                submit_d[key] = value
            except Exception, ex:
                if line.startswith('queue '):
                    # the "queue" statement should not be part of the 
                    # submit file string, but it is harmless 
                    pass
                else:
                    raise MalformedSubmitFile(line)
        self.log.debug('dictionary for submission = %s' %submit_d)
        if not bool(submit_d):
            raise EmptySubmitFile()
        
        submit = htcondor.Submit(submit_d)
        with self.schedd.transaction() as txn:
            submit.queue(txn, n)
        self.log.debug('finished')


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


