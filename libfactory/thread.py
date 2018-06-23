
__author__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov"

import logging
import logging.handlers
import threading
import time


class _thread(threading.Thread):
    
    def __init__(self):
        
        self.log = logging.getLogger('_thread')
        self.log.addHandler(logging.NullHandler())

        self.Lock = threading.Lock()

        threading.Thread.__init__(self) # init the thread
        self.stopevent = threading.Event()
        # to avoid the thread to be started more than once
        self._thread_started = False 
        # recording last time the actions were done
        self._thread_last_action = 0
        # time to wait before checking again if the threads has been killed
        self._thread_abort_interval = 1
        # time to wait before next loop
        self._thread_loop_interval = 1
        self.log.debug('object _thread initialized')

         
    def start(self):
        # this methods is overriden
        # to prevent the thread from being started more than once.
        # That could happen if the final threading class
        # implements the design pattern Singleton.
        # In that cases, multiple copies of the same object
        # may be instantiated, and eventually "started"
        
        if not self._thread_started:
            self.log.debug('starting thread')
            self._thread_started = True
            threading.Thread.start(self)


    def run(self):
        self.log.debug('starting run()')
        self._prerun()
        self._mainloop()
        self._postrun()
        self.log.debug('leaving run()')
    

    def _prerun(self):
        """
        actions to be done before starting the main loop
        """
        # default implementation is to do nothing
        pass

    
    def _postrun(self):
        """
        actions to be done after the main loop is finished
        """
        # default implementation is to do nothing
        pass

    
    def _mainloop(self):
        while not self.stopevent.isSet():
            try:                       
                if self._check_for_actions():
                    self._run()
                    self._thread_last_action = int( time.time() )
            except Exception, ex:
                self.log.warning("an exception has been captured during thread main loop: %s" % ex)
                self.log.error(traceback.format_exc(None))
                if self._propagate_exception():
                    raise ex
                if self._abort_on_exception():
                    self.join()
                self._thread_last_action = int( time.time() )
            self._wait_for_abort()


    def _check_for_actions(self):
        """
        checks if a new loop of action should take place
        """
        # default implementation
        now = int(time.time())
        check = (now - self._thread_last_action) > self._thread_loop_interval
        return check


    def _wait_for_abort(self):
        """
        waits for the loop to be aborted because the thread has been killed
        """
        time.sleep( self._thread_abort_interval )


    def _propagate_exception(self):
        """
        boolean to decide if the Exception needs to be propagated. 
        Defaults to False.
        """
        # reimplement this method if response is not unconditionally False
        return False 


    def _abort_on_exception(self):
        """
        boolean to decide if the Exception triggers the thread to be killed. 
        Defaults to False.
        """
        # reimplement this method if response is not unconditionally False
        return False 


    def _run(self):
        raise NotImplementedError


    def join(self,timeout=None):
        if not self.stopevent.isSet():
            self.log.debug('joining thread')
            self.stopevent.set()
            self._join()
            threading.Thread.join(self, timeout)


    def _join(self):
        pass
