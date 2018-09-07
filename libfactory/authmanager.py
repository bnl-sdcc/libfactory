#!/usr/bin/env python
"""
    A credential management component for AutoPyFactory 

"""
import logging
import math
import os
import pwd, grp
import sys
import threading
import time
import socket


# Added to support running module as script from arbitrary location. 
from os.path import dirname, realpath, sep, pardir
fullpathlist = realpath(__file__).split(sep)
prepath = sep.join(fullpathlist[:-2])
sys.path.insert(0, prepath)

import pluginmanager

import autopyfactory
###from autopyfactory.plugins.auth.X509 import X509
###from autopyfactory.plugins.auth.SSH import SSH
from autopyfactory.apfexceptions import InvalidAuthFailure
from autopyfactory.configloader import Config, ConfigsDiff


class AuthManager(object):
    """
        Manager to maintain multiple credential Handlers, one for each target account.
        For some handlers, if they need to perform periodic checks, they will be run
        as threads. Others, which only hold information, will just be objects.  
    
    """
    def __init__(self, factory=None):
        
        self.log = logging.getLogger('autopyfactory')
        self.log.info("Creating new authmanager...")
        self.aconfig = Config() 
        self.handlers = []
        self.factory = factory
        if factory:
            self.sleep = int(self.factory.fcl.get('Factory', 'authmanager.sleep'))
        else:
            self.sleep = 5


    def reconfig(self, newconfig):  

        hdiff = ConfigsDiff(self.aconfig, newconfig)
        self.aconfig = newconfig
        self._addhandlers(hdiff.added())


    def _addhandlers(self, newsections):

        for sect in newsections:
            try:
                pclass = self.aconfig.get(sect, 'plugin')
            except Exception, e:
                self.log.warn("No plugin attribute for section %s" % sect)
            if pclass == 'X509':
                self.log.debug("Creating X509 handler for %s" % sect )
                authpluginname = self.aconfig.get(sect, 'plugin')
                x509h = pluginmanager.getplugin(['autopyfactory', 'plugins', 'authmanager', 'auth'], authpluginname, self, self.aconfig, sect)
                self.handlers.append(x509h)
            elif pclass == 'SSH':
                self.log.debug("Creating SSH handler for %s" % sect )
                authpluginname = self.aconfig.get(sect, 'plugin')
                sshh = pluginmanager.getplugin(['autopyfactory', 'plugins', 'authmanager', 'auth'], authpluginname, self, self.aconfig, sect)
                self.handlers.append(sshh)
            else:
                self.log.warn("Unrecognized auth plugin %s" % pclass )
    
        
    def activate(self):
        """ 
        start all Handlers, if needed
        """ 

        for ah in self.handlers:
            if isinstance(ah, threading.Thread) :
                self.log.debug("Handler [%s] is a thread. Starting..." % ah.name)
                ah.start()
            else:
                self.log.debug("Handler [%s] is not a thread. No action." % ah.name)

      
    def listNames(self):
        """
            Returns list of valid names of Handlers in this Manager. 
        """
        names = []
        for h in self.handlers:
            names.append(h.name)
        return names

#
#    API for X509Handler 
#
    def getProxyPath(self, profilelist):
            """
            Check all the handlers for matching profile name(s).
            profiles argument is a list 
            """
            pp = None
            for profile in profilelist:
                self.log.debug("Getting proxy path for profile %s" % profile)
                ph = None
                for h in self.handlers:
                    self.log.debug("Finding handler. Checking %s" % h.name)
                    if h.name == profile:
                        ph = h
                        break
                    
                if ph:  
                    self.log.debug("Found handler %s. Getting proxypath..." % ph.name)
                    pp = ph.getProxyPath()
                    self.log.debug("Proxypath is %s" % pp)
                    if pp:
                        break
            if not pp:
                subject = "Proxy problem on %s" % self.factory.factoryid
                messagestring = "Unable to get valid proxy from configured profiles: %s" % profilelist
                self.factory.sendAdminEmail(subject, messagestring)
                raise InvalidAuthFailure("Problem getting proxy for profile %s" % profilelist)
            
            return pp

#
#   API for SSHKeyHandler
#

    def getSSHKeyPair(self, profile):
        """
         Returns tuple (public, private, pass) key/phrase string from profile. 
        """
        pass
        
        
    def getSSHKeyPairPaths(self, profile):
        """
        Returns tuple (public, private, pass) key/passfile paths to files from profile. 
        """
        h = self._getHandler(profile)
        pub = h.getSSHPubKeyFilePath()
        priv = h.getSSHPrivKeyFilePath()
        pasf = h.getSSHPassFilePath()
        self.log.info('Got file paths for pub, priv, pass for SSH profile %s' % profile)
        return (pub,priv,pasf)


    def _getHandler(self, profile):
        """
        
        """
        handler = None
        for h in self.handlers:
            self.log.debug("Finding handler. Checking %s" % h.name)
            if h.name == profile:
                self.log.debug("Found handler for %s" % h.name)
                handler = h
        if handler is None:
            raise InvalidAuthFailure('No handler for %s ' % profile)
        return handler
        

if __name__ == '__main__':

    import getopt
    import sys
    import os
    from ConfigParser import ConfigParser, SafeConfigParser
    
    debug = 0
    info = 0
    aconfig_file = None
    default_configfile = os.path.expanduser("~/etc/auth.conf")     
    usage = """Usage: authmanager.py [OPTIONS]  
    OPTIONS: 
        -h --help                   Print this message
        -d --debug                  Debug messages
        -v --verbose                Verbose information
        -c --config                 Config file [~/etc/auth.conf]"""
    
    # Handle command line options
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 
                                   "c:hdvt", 
                                   ["config=",
                                    "help", 
                                    "debug", 
                                    "verbose",
                                    ])
    except getopt.GetoptError, error:
        print( str(error))
        print( usage )                          
        sys.exit(1)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(usage)                     
            sys.exit()            
        elif opt in ("-c", "--config"):
            aconfig_file = arg
        elif opt in ("-d", "--debug"):
            debug = 1
        elif opt in ("-v", "--verbose"):
            info = 1
            
    # Check python version 
    major, minor, release, st, num = sys.version_info
    
    # Set up logging, handle differences between Python versions... 
    # In Python 2.3, logging.basicConfig takes no args
    #
    FORMAT23="[ %(levelname)s ] %(asctime)s %(filename)s (Line %(lineno)d): %(message)s"
    FORMAT24=FORMAT23
    FORMAT25="[%(levelname)s] %(asctime)s %(module)s.%(funcName)s(): %(message)s"
    FORMAT26=FORMAT25
    
    if major == 2:
        if minor ==3:
            formatstr = FORMAT23
        elif minor == 4:
            formatstr = FORMAT24
        elif minor == 5:
            formatstr = FORMAT25
        elif minor == 6:
            formatstr = FORMAT26
        elif minor == 7:
            formatstr = FORMAT26
    
    log = logging.getLogger('autopyfactory')
    hdlr = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(FORMAT23)
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    
    if debug: 
        log.setLevel(logging.DEBUG) # Override with command line switches
    if info:
        log.setLevel(logging.INFO) # Override with command line switches
    log.debug("Logging initialized.")      
    
    # Read in config file
    aconfig=ConfigParser()
    if not aconfig_file:
        aconfig_file = os.path.expanduser(default_configfile)
    else:
        aconfig_file = os.path.expanduser(aconfig_file)
    got_config = aconfig.read(aconfig_file)
    log.debug("Read config file %s, return value: %s" % (aconfig_file, got_config))
    
    am = AuthManager(aconfig)
    am.reconfig(aconfig)
    log.info("Authmanager created. Starting handlers...")
    am.startHandlers()
    #am.start()
    
    try:
        while True:
            time.sleep(2)
            #log.debug('Checking for interrupt.')
    except (KeyboardInterrupt): 
        log.debug("Shutdown via Ctrl-C or -INT signal.")
        
