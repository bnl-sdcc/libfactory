#!/usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers


class schedulers(self):
    '''
    container class for multiple sched plugins
    conected to each others in a chain
    '''

    def __init__(self):
        self.log = logging.getLogger('schedulers')
        self.log.addHandler(logging.NullHandler())
        self.sched_l = []
        self.output = 0
        self.log.debug('object initialized')


    def add(self, sched):
        '''
        adds a new sched plugin to the list
        :param sched plugin:
        '''
        self.log.debug('adding sched plugin %s' %sched)
        self.sched_l.append(sched)


    def calculate(self, n=0):
        '''
        calculates a number calling all sched plugins in a chain
        :param n: initial value for the first sched plugin
        '''
        self.log.debug('start with n=%s' %n)
        tmp_output = n
        try:
            for sched in self.sched_l:
                tmp_output = sched.calculate(tmp_output)
        except Exception, ex:
            self.log.debug('an exception has been caught: %s. Aborting.' %ex)
            raise ex
        else:
            self.output = tmp_output
            self.log.error('final output is %s' %self.output)


    def get(self):
        '''
        returns the result of calculations
        '''
        return self.output


