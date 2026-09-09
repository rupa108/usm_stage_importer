from java.util import GregorianCalendar, Calendar
from java.sql import Timestamp


def calc_date(reference_date, years=None, months=None, days=None, minutes=None, seconds=None, convert=True):
    """
    Helper function to calculate a date starting from a reference date. Typical examples:

    1) yesterday
    >>> today = VM.getFunctionProvider().getCurrentDate()
    >>> yesterday = calc_date(today, days=-1)
    2) tomorrow
    >>> today = VM.getFunctionProvider().getCurrentDate()
    >>> tomorrow = calc_date(today, days=1)
    3) Five minutes ago
    >>> now = VM.getFunctionProvider().getCurrentTimestamp()
    >>> five_minutes_ago = calc_date(now, minutes=-5)

    Args:
        reference_date (java.sql.Date, java.sql.Timestamp): The date from where the calculation starts
        years (int): Distance in years
        months (int): Distance in months
        days (int): Distance in days
        minutes (int): Distance in minutes
        seconds (int): Distance in seconds
        convert (bool): converts output java.sql.Timestamp if True

    Returns:
        (java.util.Date) if convert==False
        (java.sql.Timestamp) if convert==True
    """
    calendar = GregorianCalendar()
    calendar.setTime(reference_date)
    if years:
        calendar.add(Calendar.YEAR, years)
    if months:
        calendar.add(Calendar.MONTH, months)
    if days:
        calendar.add(Calendar.DATE, days)
    if minutes:
        calendar.add(Calendar.MINUTE, minutes)
    if seconds:
        calendar.add(Calendar.SECOND, seconds)

    result = calendar.getTime()

    if convert:
        result = Timestamp(result.getTime())

    return result


from org.json import JSONArray, JSONObject

def json_array_to_list(json_array): #type: (json_array: org.json.JSONArray) -> list
    """Recursively converts a java json array to a python list
    """
    result = []
    for val in json_array:
        if isinstance(val, JSONArray):
            result.append(json_array_to_list(val))
        elif isinstance(val, JSONObject):
            result.append(json_obj_to_dict(val))
        else:
            result.append(val)
    return result

def json_obj_to_dict(json_obj):  #type: (json_obj: org.json.JSONObject) -> dict
    """Recursively converts a java json object to a python dict
    """
    result = {}
    for k in json_obj.keys():
        val = json_obj.get(k)
        if isinstance(val, JSONArray):
            result[k] = json_array_to_list(val)
        elif isinstance(val, JSONObject):
            result[k] = json_obj_to_dict(val)
        else:
            result[k]= val
    return result

################################################################
# Mutex
from vm.tools.boa import pyBOT, PyBO
import uuid
from time import sleep
from contextlib import contextmanager

fp = VM.getFunctionProvider()


class ResourceBusyException(Exception):
    pass


class Mutex(object):
    """
    Synchronization mechanism based on the ideas of "Mutually Exclusive Locking".
    This can be used for restricting access to a ressource to one processing
    workflow or method. It is important to understand, that this does noc enforce anything.
    Every processing method has to implement checking the lock on it's own and react
    accordingly. e.g. abort if lock is busy.

    The methods Mutex.acquire() and Mutex.release() are public and may be used
    in complex scenarios howerver the recommended use of Mutex() is as context
    manager. This should be sufficiant for 99.9% of all cases.

    Usage:
    >>> with Mutex("MyAppLock") as lock_acquired:
    >>>     if lock_acquired:
    >>>         do_processing()

    Timeout (seconds):
    If the timeout is set in the constructor this overwrites all other timout
    settings:

    Global Timeout:
    Can be set via Mainparameter
    path = xconcurrency
    parameter = GlobalMutexTimeout
    defaults to 300 if Mainparameter is not set or does not exist.

    Individual Timeout:
    The timeout for each Mutex can be set by Mainparameter
    path = xconcurrency
    parameter pattern = Mutex{name}Timeout

    Transaction handling:
    Reading and writing lock information from an to the database is done in
    new transactions for ervery call, so that other processes acessing Mutex will
    always get current information.
    """

    main_param_path = "xconcurrency"
    main_param_global_timeout = "GlobalMutexTimeout"
    main_param_pattern =  "Mutex{name}Timeout"
    global_timeout_default = 300
    bo_type_name = "XMutexLock"

    def __init__(self, name, uuid=None, timeout=None):
        self.name = name
        self.uuid = uuid

        if timeout:
            self.timeout = timeout
        else:
            self.timeout = self._get_default_timeout()

    def _get_default_timeout(self):
        path = type(self).main_param_path
        param = type(self).main_param_pattern.format(name=self.name)
        tr = VM.createTransaction()
        timeout_sec = VM.getMainParameter(path, param, None, tr)

        if not timeout_sec:
            param = type(self).main_param_global_timeout

        timeout_sec = VM.getMainParameter(path, param, None, tr)
        tr.doCommit()
        if not timeout_sec:
            timeout_sec = type(self).global_timeout_default

        return timeout_sec

    def __enter__(self):
        return self if self.acquire() else None

    def __exit__(self, type, value, traceback):
        self.release()

    def acquire(self):
        """
        Public method. Acquire the lock.
        """
        result = None
        tr = VM.createTransaction()
        name = self.name
        my_uuid = uuid.uuid4().hex
        now = fp.getCurrentTimestamp()
        MutexLock = pyBOT(type(self).bo_type_name, tr=tr)
        lock = MutexLock.findFirst(mutexName=name) #type: PyBO["XMutexLock"]
        if not lock:
            lock = MutexLock.create(
                mutexName=name,
            )
            released = True
        else:
            released = lock.dateReleased is not None

        timeout = lock.dateTimeout
        if released or (timeout and timeout.before(now)):
            lock.uuid = my_uuid
            self.uuid = my_uuid
            lock.dateTimeout = calc_date(now, seconds=self.timeout)
            lock.dateAcquired = now
            lock.dateReleased = None

            tr.doCommit()

            result = my_uuid

        else:
            result = None

        return result

    def release(self):
        """
        Public method. Release the lock. The apropriate uuid must be set!
        """
        tr = VM.createTransaction()
        MutexLock = pyBOT(type(self).bo_type_name, tr=tr)
        now = fp.getCurrentTimestamp()
        if self.uuid:
            lock = MutexLock.findFirst(mutexName=self.name, uuid=self.uuid)
            if lock:
                lock.uuid = None
                self.uuid = None
                now = fp.getCurrentTimestamp()
                lock.dateReleased = now
                tr.doCommit()


def get_spin_lock(mutex_name, wait_timeout):
    """
    Continuously tries to get the lock for <wait_tiemout> seconds.
    If it can't be acquired during this time, raise ResourceBusyException.
    """
    mutex = Mutex(mutex_name)
    t = 0
    while True:
        uuid = mutex.acquire()
        if uuid:
            return uuid, mutex
        else:
            if t < wait_timeout:
                t +=1
                sleep(1)
            else:
                raise ResourceBusyException(mutex_name)


@contextmanager
def managed_spinlock(mutex_name, wait_timeout):
    """
    Use this as a contextmanager for obtaining a spin lock.

    Usage:
    >>> with managed_spinlock("MyLock", 300) as lock:
    >>>     do_something()

    """
    uuid = None
    mutex = None
    try:
        uuid, mutex = get_spin_lock(mutex_name, wait_timeout)
        yield uuid
    finally:
        if mutex:
            mutex.release()

#######################################################################