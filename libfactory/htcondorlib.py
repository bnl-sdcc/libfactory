#!/usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import htcondor
import classad


class HTCondorPool(object):

    def __init__(self, remotecollector=None, remoteschedd=None):
        """
        :param string remotecollector: hostname of the collector
        :param string remoteschedd: hostname of the schedd
        """
        self.remotecollector = remotecollector
        self.remoteschedd = remoteschedd
        self.collector = self.getcollector()
        self.schedd = self.getschedd()


    def getcollector(self):
        if self.remotecollector:
            collector = htcondor.Collector(self.remotecollector)
        else:
            collector = htcondor.Collector()
        return collector


    def getschedd(self):
        if self.remotecollector:
            scheddAd = self.collector.locate(htcondor.DaemonTypes.Schedd, self.remoteschedd)
            schedd = htcondor.Schedd(scheddAd) 
        else:
            schedd = htcondor.Schedd() # Defaults to the local schedd.
        return schedd

    # -------------------------------------------------------------------------

    def condor_q(self, attribute_l, constraint_l=None):
        '''
        Returns a list of ClassAd objects. 
        :param list attribute_l: list of classads strings to include in the query 
        :param list constraint_l: list of constraints strings in the query
        '''
        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)

        constraint_str = self._build_constraint_str(constraint_l)
        out = self.schedd.query(constraint_str, attribute_l)
        out = list(out)
        return out

    
    def condor_history(self, attribute_l, constraint_l=None):
        """
        :param list attribute_l: list of classads strings to include in the query 
        :param list constraint_l: list of constraints strings in the history query
        """
        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)

        constraint_str = self._build_constraint_str(constraint_l)
        out = self.schedd.history(constraint_str, attribute_l, 0)
        out = list(out)
        return out


    def condor_rm(self, jobid_l):
        """
        :param list jobid_l: list of strings "ClusterId.ProcId"
        """
        self.schedd.act(htcondor.JobAction.Remove, jobid_l)
    
    
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
        """
        if type(attribute_l) is not list:
            raise IncorrectInputType("attribute_l", list)
        if constraint_l is not None and\
           type(constraint_l) is not list:
            raise IncorrectInputType("constraint_l", list)

        constraint_str = self._build_constraint_str(constraint_l)
        outlist = self.collector.query(htcondor.AdTypes.Startd, constraint_str, attribute_l)
        return outlist


    def condor_submit(self, jdl_str, n):
        """
        performs job submission from a string representation 
        of the submit file. The string containing the submit file should not
        contain the "queue" statement, as the number of jobs is being passed
        as a separate argument.
        :param str jdl_str: single string with the content of the submit file
        :param int n: number of jobs to submit
        """
        submit_d = {}
        for line in jdl_str.split('\n'):
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

        submit = htcondor.Submit(submit_d)
        with self.schedd.transaction() as txn:
            submit.queue(txn, n)


    def _build_constraint_str(self, constraint_l=None):
        """
        """
        if constraint_l:
            constraint_str = " && ".join(constraint_l)
        else:
            constraint_str = "true"
        return constraint_str

# =============================================================================
#   Exceptions
# =============================================================================

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


