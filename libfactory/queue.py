#!/bin/env python
#
# Classes for managing load balancing and ordered fill in an arbitrary tree. 
#

import getopt
import logging
import random
import sys
import threading

from ConfigParser import ConfigParser
import StringIO

from libfactory.info import StatusInfo
from IPython.lib.editorhooks import idle

class NotImplementedException(Exception):
    pass

class QTreeNode(threading.Thread):
    '''
    Common code for any Tree Node. 
    '''
    STATES = ['idle','running','complete']
        
    def enqueue(self, n):
        raise NotImplementedException()

    def rebalance(self):
        raise NotImplementedException()
    
    def isFull(self):
        raise NotImplementedException()

    def run(self):
        raise NotImplementedException()

    def __repr__(self):
        s =""
        s +='[%s] full=%s ' % (self.label, self.isFull())
        return s

    def printtree(self, depth = 0):
        s = ""
        s += " " * depth * 4
        s += "%s" % self
        s += "\n"
        for ch in self.children:
            s += ch.printtree(depth + 1)
        return s
    
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
        info[self.label] = {}
        for state in QTreeNode.STATES:
            info[self.label][state] = 0 

        for ch in self.children:
            chinfo = ch.getInfo()
            k = chinfo.keys()[0]
            dict = chinfo[k]
            for state in QTreeNode.STATES:
                info[self.label][state] += dict[state]            
            #print(dict)
        return info


class LBQueue(QTreeNode):
    '''
    A Load-Balancing queue node.
    Will attempt to evenly split active items across children.
     
    '''
    def __init__(self, config, section):
        threading.Thread.__init__(self)
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.label = section
        self.childlist = [] # default
        self.isroot = self.config.getboolean(section, 'isroot')
        self.maxtransfer = self.config.getint(section, 'maxtransfer')
        childstr = self.config.get(section, 'childlist')
        for ch in childstr.split(','):
            self.childlist.append(ch.strip())
        self.log.debug("childlist = %s" % self.childlist)
        self.children = []
        self.parent = None
        
    
    def enqueue(self, n):
        '''
        
        '''
        if n == 0:
            self.log.info("Submit 0. Doing nothing.")
        if n > 0:
            openchildren = []       
            for ch in self.children:
                if not ch.isFull():
                    openchildren.append(ch)
            numopen = len(openchildren)
            self.log.debug("[%s] %s open children." % (self.label, numopen))
            if numopen > 0:
                if numopen == 1:
                    tosub = n
                elif numopen > 1:
                    tosub = n/len(openchildren)
                    if tosub == 0:               # Deal with very low numbers without ciel() or randomization. 
                        tosub = 1
                for ch in openchildren:
                    self.log.debug("Submitting %s to %s" % (tosub, ch.label))
                    ch.enqueue(tosub)
            else:
                if self.isroot:
                    self.log.info("[%s] Submitted to when full. Unable to handle more jobs")
                else:
                    self.log.error("[%s] Submitted to when full. Always call isFull() before submitting." % self.label)      
        elif n < 0:
            self.log.info("Submitting negative number retire()")
       
        
            
    def isFull(self):
        '''
        Queue is only full if ALL children are full. 
        '''
        full = True
        for ch in self.children:
            if not ch.isFull():
                full = False
        return full
    
    def rebalance(self):
        '''
        Collect info from children and move jobs from one to another if needed. 
        '''
        self.log.debug("Rebalance called for [%s]" % self.label)
        overflow = 0
        for ch in self.children:
            overflow += ch.rebalance()
        self.log.debug("[%s] %d overflow" % (self.label, overflow) ) 
        if overflow > 0 and not self.isFull():
            self.enqueue(overflow)
            overflow = 0        
        return overflow

        

    def run(self):
        '''
        Will be used if this is a root node. 
        '''
    

class OFQueue(QTreeNode):
    '''
    An Ordered Fill queue node. 
    '''
    def __init__(self, config,  section):
        threading.Thread.__init__(self)
        self.log = logging.getLogger()
        self.config = config
        self.section = section
        self.label = section
        self.isroot = self.config.getboolean(section, 'isroot')
        self.maxtransfer = self.config.getint(section, 'maxtransfer')
        self.childlist = [] # default
        childstr = self.config.get(section, 'childlist')
        for ch in childstr.split(','):
            self.childlist.append(ch.strip())
        self.log.debug("childlist = %s" % self.childlist)
        self.children = []
        self.parent = None


    def enqueue(self, n):
        if n == 0:
            self.log.info("Submit 0. Doing nothing.")
        if n > 0:
            openchildren = []       
            for ch in self.children:
                if not ch.isFull():
                    openchildren.append(ch)
            
            if len(openchildren) > 0:
                for ch in self.children:
                    if not ch.isFull():
                        self.log.debug("Submitting %s to %s" % (n, ch.section))
                        ch.enqueue(n)
            else:
                self.log.error("[%s] Submitted to when full. Always call isFull() before submitting." % self.label) 
        elif n < 0:
            self.log.info("Submitting negative number retire()")

   
    def rebalance(self):
        '''
        Collect info from children and move jobs from one to another if needed. 
        '''
        self.log.debug("Rebalance called for [%s]" % self.label)
        overflow = 0
        for ch in self.children:
            overflow += ch.rebalance()
        if overflow > 0 and not self.isFull():
            self.enqueue(overflow)
            overflow = 0        
        self.log.debug("[%s] %d overflow" % (self.label, overflow) ) 
        return overflow
    

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
        self.isroot = self.config.getboolean(section, 'isroot')
        self.maxtransfer = self.config.getint(section, 'maxtransfer')
        self.batchpluginname = config.get(section, 'batchplugin')
        bp = getattr(sys.modules[__name__], self.batchpluginname)
        bpo = bp(config, section)
        self.log.debug("Set batchplugin to %s" % bpo)
        self.batchplugin = bpo
        try:
            self.mock = config.get(section, 'mock')
        except:
            self.mock = None
        self.minfullpending = config.getint(section, 'minfullpending')
        
        self.childlist = []
        self.children = []

    
    def enqueue(self, n):
        if not self.isFull():
            self.batchplugin.submit(n, 
                                    label=self.section, 
                                    mock=self.mock)
        else:
            self.log.error("[%s] Submitted to when full. Always call isFull() before submitting." % self.label) 
       
    def isFull(self):
        '''
        isFull means that all targets have full queues AND minpending is idle at each target. 
        '''
        # for testing delegate to relevant site. 
        try:
            site = self.batchplugin.sites[self.section]
            if site.isFull() and site.idle >= self.minfullpending:
                return True
        except KeyError:
            return False
        return False 
        
        # eventually calculate based on job startup timing info, pending time, etc. 
        #info = self.batchplugin.getInfo()
        

    def __repr__(self):
        site = None
        try:
            site = self.batchplugin.sites[self.section]
        except KeyError:
            pass
        s =""
        s +='[%s] mock=%s full=%s %s ' % (self.section, self.mock, self.isFull(),  site)
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
            i[self.label]['running'] = site.running
            i[self.label]['complete'] = site.complete
        else:
            for state in QTreeNode.STATES:
                i[self.label][state] = 0 
        return i

    def rebalance(self):
        '''
        Collect info from children and move jobs from one to another if needed. 
        '''
        self.log.debug("Rebalance called for [%s]: Leaf node. No sub-call." % self.label)
        site = None
        overflow = 0
        try:
            site = self.batchplugin.sites[self.section]
            if site.isFull() and site.idle > self.minfullpending: 
                overflow = site.idle - self.minfullpending
                self.log.debug("[%s] idle=%d minfullpending=%s overflow=%d" % (self.label,
                                                                               site.idle,
                                                                               self.minfullpending,
                                                                               overflow))
                site.idle = site.idle - overflow
                self.log.debug("[%s] Rebalancing %d idle jobs." % (self.label, overflow))
        except KeyError:
            pass
        return overflow
        

class QueueManager(object):
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
            #qo.printtree()
            pass
                           
    def getRootList(self):
        return self.rootlist
 
    def getPrintTree(self):
        s = ''
        for qo in self.rootlist:
            s += qo.printtree() 
        return s
    
    
    
    
    
    
    
#############################################################################################
#   Mock/ Test infrastructure. Should move to /test
##############################################################################################

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
            
    def isFull(self):
        full = False
        if self.running >= self.max:
            full = True
        return full
    
    
    def __repr__(self):
        s = "max=%s full=%s idle=%s running=%s complete=%s" % (self.max, self.isFull(), self.idle, self.running, self.complete)    
        return s

class MockBatchPlugin(object):
    '''
    Programmable mock batch plugin so behavior can be repeatable for testing.    
    
    Jobs sumbitted to labels via enqueue(n, label) go into IDLE state with that label.
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
        
    def __init__(self, config, section, seed = None, completefactor = .10):
            self.log = logging.getLogger()
            self.config = config
            self.section = section
            self.cycles = 0
            self.sites = {}
            if seed is None:
                random.seed(1234)  # with given input, should produce identical runs...
            else: 
                random.seed(seed)
            self.completefactor = completefactor
    
    
    def processComplete(self):
        '''
        Go through all idle jobs at various labels and decide which to complete. 
        '''
        for label in self.sites.keys():
            s = self.sites[label]
            running = s.running
            
            tocomplete = 0
            if running > 0:
                for i in range(0,running):
                    rval = random.random()
                    if rval < self.completefactor:
                        tocomplete += 1
                self.log.debug("[%s] Moving %d of %d jobs to complete." % (s.label, tocomplete, running))
                s.running = s.running - tocomplete
                s.complete = s.complete + tocomplete


    def processIdle(self):
        '''
        Go through all idle jobs at various labels and decide which to complete. 
        '''        
        for label in self.sites.keys():
            s = self.sites[label]
            running = s.running                        
            idle = s.idle
            if idle > 0: 
                if s.running < s.max:
                    avail = s.max - s.running
                    if idle < avail:
                        torun = idle
                    else:
                        torun = avail
                    self.log.info("[%s] Moving %d jobs to running. " % (s.label, torun))
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


def test_submit(submitlist=[10], completefactor = .10):
    logging.debug("Starting test...")
    config = '''[DEFAULT]
[DEFAULT]
childlist = None
minpending = 2
batchplugin = None
maxtransfer = 5
isroot=False
wmsplugin = None
minfullpending = 2


[lbroot1]
klass = LBQueue
childlist = lbnode1, ofnode1, subE
isroot = True

[lbnode1]
klass = LBQueue
childlist = subC, subD

[ofnode1]
klass = OFQueue
childlist = subA, subB


[subA]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max5

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
mock=max20

[subE]
klass = SubmitQueue
batchplugin = MockBatchPlugin
mock=max30

'''   
     
    cp = ConfigParser()
    buf = StringIO.StringIO(config)
    cp.readfp(buf)

    mbp = MockBatchPlugin(cp, 'test')    
    qm = QueueManager(cp)
    rl = qm.getRootList()
    for qo in rl:
        qo.printtree()

    for subnumber in submitlist:
        thiscycle = mbp.cycles +1
        print("cycle: %d do submission." % thiscycle)
        for qo in rl:
            qo.enqueue(subnumber)
        print(qm.getPrintTree())
        
        print("cycle: %d  finish jobs." % thiscycle)
        mbp.processComplete()
        print(qm.getPrintTree())

        print("cycle: %d run jobs." % thiscycle)
        mbp.processIdle()   
        print(qm.getPrintTree())
        
        print("cycle: %d rebalance" % thiscycle)
        for qo in rl:
            qo.rebalance()
        print(qm.getPrintTree()) 
           
    print("%d cycles completed. " % (mbp.cycles))
    
    
if __name__ == '__main__':
        
    fconfig_file = None
    debug = 0
    verbose = 0
    submitlist = "10,20,0,0,0,10,0,0"
    completefactor = .10
    usage = """Usage: queue.py [OPTIONS]  
    OPTIONS: 
        -h --help                   Print this message
        -d --debug                  Debug messages
        -v --verbose                Verbose information
        -c --config                 Config file [None]
        -s --submitlist             Number to submit [10,20,0,0,0,10,0,]
        -C --completefactor         Chance of completion/cycle [.10]
    """
    
    
    # Handle command line options
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 
                                   "hdvtc:s:C:", 
                                   ["help", 
                                    "debug", 
                                    "verbose",
                                    "config=",
                                    "submitlist=",
                                    "completefactor",
                                    ])
    except getopt.GetoptError as error:
        print( str(error))                     
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)                     
            sys.exit()            
        elif opt in ("-d", "--debug"):
            debug = 1
        elif opt in ("-v", "--verbose"):
            verbose = 1
        elif opt in ("-c", "--config"):
            fconfig_file = arg
        elif opt in ("-s", "--submitlist"):
            submitlist = arg
        elif opt in ('-C', "--completefactor"):
            completefactor = float(arg)
    
    substrlist = submitlist.split(",")
    sublist = []
    for item in substrlist:
        item = item.strip()
        num = int(item)
        sublist.append(num)
        
    if verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    elif debug:
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    else:
        logging.basicConfig(stream=sys.stdout, level=logging.WARN)
    
    test_submit(sublist, completefactor)







