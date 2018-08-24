#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers
from thread import _thread
import time


# ============================================================================== 
#
#               This is a prototype. Work in progress
#
# ============================================================================== 

class querystatusbase(_thread):

    def __init__(self, queryplugin):
        '''
        :param queryplugin: any plugin to implement Status query
        '''
        self.log = logging.getLogger('_querystatus')
        self.log.addHandler(logging.NullHandler())
        self.queryplugin = queryplugin
        self.currentinfo = None
        self.log.debug('object initialized')


    def _run(self):
        '''
        main _thread loop
        '''
        self.log.debug('start')
        try:
            self.update()
            self.currentinfo = self.queryplugin.getinfo()
        except Exception as ex:
            self.log.error('exception captured: %s' %ex)
        self.log.debug('end')


    def update(self):
        '''
        updates status information
        '''
        raise NotImplementedError


    def getinfo(self):
        '''
        gets the current info status
        '''
        self.log.debug('start')
        if not self.currentinfo:
            self.log.debug('there is not yet avaiable status info. Returning None')
            return None
        else:
            return self.currentinfo
