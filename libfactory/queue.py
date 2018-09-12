#!/bin/env python
#
# Classes for managing load balancing and ordered fill in an arbitrary tree. 
#

import logging
import sys

from ConfigParser import ConfigParser
import StringIO

from libfactory.info import StatusInfo



class NotImplementedException(Exception):
    pass


class QTreeNode(object):
    '''
    Common code for any Tree Node. 
    
    '''
    def submit(self, n):
        raise NotImplementedException()

    def getInfo(self):
        raise NotImplementedException()    
    
    def isFull(self):
        raise NotImplementedException()

    def __repr__(self):
        s =""
        s +='[%s]' % self.section
        return s

    
    def printtree(self, depth = 0):
        s = ""
        s += " " * depth * 4
        s += "%s" % self
        #s += "\n"
        print(s)
        for ch in self.children:
            ch.printtree(depth + 1)


class LBQueue(QTreeNode):
    '''
    A Load-Balancing queue node.
    Will attempt to evenly split active items across children.
     
    '''
    def __init__(self, config, section):
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.childlist = [] # default
        childstr = self.config.get(section, 'childlist')
        for ch in childstr.split(','):
            self.childlist.append(ch.strip())
        self.log.debug("childlist = %s" % self.childlist)
        self.children = []
        self.parent = None
        
    
    def submit(self, n):
        tosub = n / len(self.children)
        for ch in self.children:
            self.log.debug("Submitting %s to %s" % (tosub, ch.section))
            ch.submit(tosub)

    def isFull(self):
        '''
        Queue is only full if ALL children are full. 
        '''
        full = True
        for ch in self.children:
            if not ch.isFull():
                full = False
        return full
    

class OFQueue(QTreeNode):
    '''
    An Ordered Fill queue node. 
    '''
    def __init__(self, config,  section):
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.childlist = [] # default
        childstr = self.config.get(section, 'childlist')
        for ch in childstr.split(','):
            self.childlist.append(ch.strip())
        self.log.debug("childlist = %s" % self.childlist)
        self.children = []
        self.parent = None


    def submit(self, n):
        for ch in self.children:
            if not ch.isFull():
                self.log.debug("Submitting %s to %s" % (n, ch.section))
                ch.submit(n)

    def isFull(self):
        '''
        Queue is only full if ALL children are full. 
        '''
        full = True
        for ch in self.children:
            if not ch.isFull():
                full = False
        return full


class SubmitQueue(QTreeNode):
    def __init__(self, config,  section):
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.parent = None
        self.batchpluginname = config.get(section, 'batchplugin')
        bp = getattr(sys.modules[__name__], self.batchpluginname)
        bpo = bp(config, section)
        self.log.debug("Set batchplugin to %s" % bpo)
        self.batchplugin = bpo
        self.childlist = []
        self.children = []
    
    def submit(self, n):
        self.batchplugin.submit(n, label=self.section)
       
    def isFull(self):
        '''
        q is full if pending > 10
        '''
        full = False
        info = self.batchplugin.getInfo()
        #pending = info[self.section]['idle']
        #if pending > 10:
        #    full = True
        return full




class QueuesFactory(object):
    ''' processes config file and returns list of tree objects'''
    
    def __init__(self, config):
        self.log = logging.getLogger()
        self.queuesbyname = {}
        self.allqueues = []
        cp = ConfigParser()
        buf = StringIO.StringIO(config)
        cp.readfp(buf)
        for section in cp.sections():
            self.log.debug('handling section %s' % section)
            klassname = cp.get(section, 'klass')
            ko = getattr(sys.modules[__name__], klassname)
            #ko = __import__('.', globals(), locals(), klassname)
            qo = ko(cp, section)
            self.queuesbyname[section] = qo
            self.allqueues.append(qo)
        self.log.debug('Collected all queues. Queuesbyname = %s '%  self.queuesbyname)

        for qname in self.queuesbyname.keys():
            qo = self.queuesbyname[qname]
            for ch in qo.childlist:
                self.log.debug("Adding child %s to %s" % (ch, qname))
                co = self.queuesbyname[ch]
                qo.children.append(co)
                self.log.debug("Setting parent of %s to %s" % (co.section, qo.section))
                co.parent = qo

        self.rootlist = []
        for qo in self.allqueues:
            if qo.parent is None:
                self.rootlist.append(qo)
        
        for qo in self.rootlist:
            qo.printtree()

                    
        
    def getRootList(self):
        return self.rootlist
    


class MockBatchPlugin(object):
    '''
    Programmable mock batch plugin so behavior can be repeatable for testing.    
    '''

    instance = None
    
    def __new__(cls, *k, **kw):
        if MockBatchPlugin.instance is None:
            logging.debug("Making new MockBatchPlugin object...")
            MockBatchPlugin.instance = MockBatchPluginImpl(*k, **kw)          
        return MockBatchPlugin.instance


class MockBatchPluginImpl(object):
        
    def __init__(self, config, section):
            self.log = logging.getLogger()
            self.config = config
            self.section = section
            self.log = logging.getLogger()
            self.batchinfo = {}
    
    def process(self):
        '''
        Go through all idle jobs at various labels and decide which to run or complete. 
        '''
        pass
    
       
    def submit(self, n, label=None):
        self.log.debug("Getting %s jobs with label %s" % (n, label))
        if label == None:
            pass
        else:
            try:
                self.batchinfo[label]['idle'] += n
            except KeyError:
                self.batchinfo[label] = {}
                self.batchinfo[label]['idle'] = n
                self.batchinfo[label]['running'] = 0
                self.batchinfo[label]['complete'] = 0
            

    def getInfo(self):
        return self.batchinfo

    def __repr__(self):
        s = ''
        for target in self.batchinfo.keys():
            s += '[%s] idle=%s running=%s complete=%s ' % (target, 
                                                           self.batchinfo[target]['idle'],
                                                           self.batchinfo[target]['running'],
                                                           self.batchinfo[target]['complete'],
                                                           )
        return s


def test():
    logging.debug("Starting test...")
    config = '''[DEFAULT]
[DEFAULT]
childlist = None
maxtransferpercycle = 5
minpending = 2
batchplugin = None



[lbroot1]
klass = LBQueue
childlist = lbnode1, ofnode1, subE
root = True

[lbnode1]
klass = LBQueue
childlist = subC, subD

[ofnode1]
klass = OFQueue
childlist = subA, subB


[subA]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subB]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subC]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subD]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subE]
klass = SubmitQueue
batchplugin = MockBatchPlugin

'''
    qf = QueuesFactory(config)
    rl = qf.getRootList()
    for qo in rl:
        qo.submit(40)
    


    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    test()







