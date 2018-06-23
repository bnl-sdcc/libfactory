#! /usr/bin/env python

__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers
from thread import _thread
import time




# ##########################################################
#
#                THIS IS A PROTOTYPE
# ----------------------------------------------------------
#
#   It requires:
#
#       -- the plugin class to implement __eq__()
#
#       -- the plugin class to implement interval()
#
#       -- the plugin class to implement update()
#
#       -- the plugin class to implement getinfo()
#
# ##########################################################




class _querystatus(_thread):

    def __init__(self, queryplugin):
        '''
        :param queryplugin: any plugin to implement Status query
        '''
        self.log = logging.getLogger('_querystatus')
        self.log.addHandler(logging.NullHandler())
        self.queryplugin = queryplugin
        self._thread_loop_interval = self.queryplugin.interval()
        self.currentinfo = None
        self.log.debug('object initialized')


    def _run(self):
        '''
        main _thread loop
        '''
        self.log.debug('start')
        try:
            self.queryplugin.update()
            self.currentinfo = self.queryplugin.getinfo()
        except Exception, ex:
            self.log.error('exception captured: %s' $ex)
        self.log.debug('end')


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

        



# ==============================================================================
#                       Singleton wrapper
# ==============================================================================

class querystatus(object):

    # -------------------------------------------
    #
    #               WARNING
    #
    #  this implementation requires the 
    #  query plugins to implement __eq__
    #  in order to compare the new one with
    #  the stored keys
    # -------------------------------------------

    instances = {}

    def __new__(cls, queryplugin):
        keys = querystatus.instances.keys():
        if queryplugin not in keys:
            new_querystatus = _querystatus(queryplugin)
            querystatus.instances[queryplugin] = new_querystatus
            return new_querystatus
        else:    
            for key in keys:
                if queryplugin == key:
                    return queryplugin.instances[key]
