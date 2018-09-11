#!/bin/env python
#
# Classes for managing load balancing and ordered fill in an arbitrary tree. 
#

import logging
import sys

from ConfigParser import ConfigParser
import StringIO


class NotImplementedException(Exception):
    pass


class QTreeNode(object):
    '''
    
    '''
    def submit(self, n):
        pass

    def getInfo(self):
        pass    
    
    def isFull(self):
        return False

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
        pass   
    

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
        pass


class SubmitQueue(QTreeNode):
    def __init__(self, config,  section):
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.parent = None
        self.childlist = []
        self.children = []



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

                    
        
    def getFinalQueueList(self):
        pass
    


class MockBatchPlugin(object):

    instance = None
    
    def __init__(self):
        if instance is not None:
            return instance
        else:
            self.log = logging.getLogger()
            self.batchinfo = {}
    
    def process(self):
        '''
        Go through all idle jobs at various sites and decide which to run or complete. 
        '''
        pass
       
    def submit(self, n, label=None):
        if label == None:
            pass
        else:
            self.batchinfo[label]['idle'] += n
            

    def getInfo(self):
        pass




def test():
    logging.debug("Starting test...")
    config = '''[DEFAULT]
[DEFAULT]
childlist = None


[ofroot1]
klass = OFQueue
childlist = lbnode1, subthree
root = True

[lbnode1]
klass = LBQueue
childlist = subone, subtwo

[subone]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subtwo]
klass = SubmitQueue
batchplugin = MockBatchPlugin

[subthree]
klass = SubmitQueue
batchplugin = MockBatchPlugin

'''
    qf = QueuesFactory(config)



    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    test()







