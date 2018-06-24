#!/usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers


class schedulermanager(self):
    '''
    container class for multiple scheduler plugins
    conected to each others in a sequence 
    '''

    def __init__(self):
        self.log = logging.getLogger('schedulers')
        self.log.addHandler(logging.NullHandler())
        self.scheduler_l = []
        self.log.debug('object initialized')


    def add(self, scheduler):
        '''
        adds a new sched plugin to the list
        :param scheduler plugin:
        '''
        self.log.debug('adding scheduler plugin %s' %scheduler)
        self.scheduler_l.append(scheduler)


    def calculate(self, n=0):
        '''
        calculates a number calling all sched plugins in a chain
        :param n: initial value for the first sched plugin
        '''
        self.log.debug('start with input %s' %n)
        try:
            for scheduler in self.scheduler_l:
                n = scheduler.calculate(n)
        except Exception, ex:
            self.log.debug('an exception has been caught: %s. Aborting.' %ex)
            raise ex
        else:
            self.log.error('final output is %s' %n)
            return n

