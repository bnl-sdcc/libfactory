#!/bin/env python
#
# Classes for managing load balancing and ordered fill in an arbitrary tree. 
#

import logging
import sys

from ConfigParser import ConfigParser
import StringIO

from libfactory.info import StatusInfo
from IPython.lib.editorhooks import idle

class NotImplementedException(Exception):
    pass

class QTreeNode(object):
    '''
    Common code for any Tree Node. 
    
    '''    
    def submit(self, n):
        raise NotImplementedException()

    def rebalance(self, n):
        raise NotImplementedException()

    def getInfo(self):
        raise NotImplementedException()    
    
    def isFull(self):
        raise NotImplementedException()

    def __repr__(self):
        s =""
        s +='[%s]' % self.label
        return s

    
    def printtree(self, depth = 0):
        s = ""
        s += " " * depth * 4
        s += "%s" % self
        #s += "\n"
        print(s)
        for ch in self.children:
            ch.printtree(depth + 1)

    def getInfo(self):
        '''
        { 'label' : 
            { 'idle' : X,
              'running': Y,
              'complete': Z
            }
        }
        '''
        info = {}
        for ch in self.children:
            chinfo = ch.getInfo()



class LBQueue(QTreeNode):
    '''
    A Load-Balancing queue node.
    Will attempt to evenly split active items across children.
     
    '''
    def __init__(self, config, section):
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.label = section
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
        self.label = section
        self.childlist = [] # default
        childstr = self.config.get(section, 'childlist')
        for ch in childstr.split(','):
            self.childlist.append(ch.strip())
        self.log.debug("childlist = %s" % self.childlist)
        self.children = []
        self.parent = None


    def submit(self, n):
        
        if n > 0:
            for ch in self.children:
                if not ch.isFull():
                    self.log.debug("Submitting %s to %s" % (n, ch.section))
                    ch.submit(n)
        else:
            pass
    
   
    def rebalance(self, n):
        self.log.debug("Rebalance called for [%s]" % self.label) 


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
        self.label = section
        self.parent = None
        self.batchpluginname = config.get(section, 'batchplugin')
        bp = getattr(sys.modules[__name__], self.batchpluginname)
        bpo = bp(config, section)
        self.log.debug("Set batchplugin to %s" % bpo)
        self.batchplugin = bpo
        try:
            self.mock = config.get(section, 'mock')
        except:
            self.mock = None
        
        self.childlist = []
        self.children = []
    
    def submit(self, n):
        self.batchplugin.submit(n, label=self.section, mock=self.mock)
       
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

    def __repr__(self):
        site = None
        try:
            site = self.batchplugin.sites[self.section]
        except KeyError:
            pass
        s =""
        s +='[%s] mock=%s %s ' % (self.section, self.mock, site)
        return s

    def getInfo(self):
        site = None
        try:
            site = self.batchplugin.sites[self.section]
        except KeyError:
            pass
        i = {}
        i[self.label] = {}        
        
        if site is not None:
            i[self.label]['idle'] = site.idle 
            i[self.label]['running'] = site.runnning
            i[self.label]['complete'] = site.complete
        else:
            i[self.label]['idle'] = 0 
            i[self.label]['running'] = 0
            i[self.label]['complete'] = 0            
        return i



class QueuesFactory(object):
    ''' processes config file and returns list of tree objects'''
    
    def __init__(self, cp):
        self.log = logging.getLogger()
        self.queuesbyname = {}
        self.allqueues = []
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
    


#
#   Mock infrastructure. Should move to /test
#

class MockSite(object):
    
    def __init__(self, label, mock):
        '''
        mock = maxXYZ-lenXYZ-
        max = max jobs concurrent at site
        len = length of jobs in cycles
        
        '''
        
        self.log=logging.getLogger()
        self.label = label
        self.mock = mock
        self.max = 9999 # max jobs at site
        self.joblen = 10 # cycles per job
        
        fields = self.mock.split('-')
        for field in fields:
            if field[:3] == 'max':
                self.max = int(field[3:])
        self.idle = 0
        self.running = 0
        self.complete = 0

    
    def __repr__(self):
        s = "max=%s idle=%s running=%s complete=%s" % (self.max, self.idle, self.running, self.complete)    
        return s

class MockBatchPlugin(object):
    '''
    Programmable mock batch plugin so behavior can be repeatable for testing.    
    
    Jobs sumbitted to labels via submit(n, label) go into IDLE state with that label.
    When .process() is called, one cycle is run  on all labelled jobs. 
   
    
    Pre-programmed labels. If you submit to these labels, they will behave as the label described. 
    Non-preprogrammed labels will continue accepting and running jobs.   
    
    max10      max 10 jobs then full
    max50      max 50 jobs  then full
    max100
         
    
       
    
    
    
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
            self.cycles = 0
            self.log = logging.getLogger()
            self.sites = {}
            
    
    def process(self):
        '''
        Go through all idle jobs at various labels and decide which to run or complete. 
        '''
        for label in self.sites.keys():
            s = self.sites[label]
            idle = s.idle
            if idle > 0: 
                if s.running < s.max:
                    avail = s.max - s.running
                    if idle < avail:
                        torun = idle
                    else:
                        torun = avail
                    self.log.info("Starting %s jobs on [%s]" % (torun, s.label))
                    s.running = s.running + torun  
                    s.idle = s.idle - torun
        self.cycles += 1
                    
    
       
    def submit(self, n, label, mock=None):
        self.log.debug("Getting %s jobs label=%s" % (n, label))
        try:
            siteobj = self.sites[label]
        except KeyError:
            siteobj = MockSite(label, mock)
            self.sites[label] = siteobj
        siteobj = self.sites[label]
        siteobj.idle += n
                    

    def getInfo(self):
        batchinfo = {}
        for sitelabel in self.sites.keys():
            site = self.sites[sitelabel]
            batchinfo[site.label] = {}
            batchinfo[site.label]['idle'] = site.idle
            batchinfo[site.label]['running'] = site.running
            batchinfo[site.label]['complete'] = site.complete
        return batchinfo

    def __repr__(self):
        s = ''
        s += '[MockBatchPlugin] cycles=%s ' % self.cycles
        for target,site in sorted(self.sites.items()):
            #for target in self.sites.keys():
            site = self.sites[target]
            s += '[%s] idle=%s running=%s complete=%s ' % (site.label, 
                                                           site.idle,
                                                           site.running,
                                                           site.complete,
                                                           )
        return s


def test_submit(submitlist=[10]):
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
mock=max10

[subB]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max10

[subC]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max10

[subD]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max50

[subE]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max100

'''   
     
    cp = ConfigParser()
    buf = StringIO.StringIO(config)
    cp.readfp(buf)

    mbp = MockBatchPlugin(cp, 'test')    
    qf = QueuesFactory(cp)
    rl = qf.getRootList()

    for subnumber in submitlist:
        for qo in rl:
            qo.submit(subnumber)
        for qo in rl:
            qo.printtree()
        mbp.process()    
        
    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.WARN)
    submitlist = [40 , 20, 0, 0]
    test_submit(submitlist)







