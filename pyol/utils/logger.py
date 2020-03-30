'''
# This is a module that wraps the Python's logging module, it provides
# five logging functions (debug, warning, error, info, critical).

# Usage:
import logger

# To log a message, please use one of the debug, critical, info, error,
# warning functions. They are ordered in the following way (from highest
# to lowest in terms of the severity of the log message):

# 1. critical - critical error that forces the program to exit
# 2. error - an error that makes the program unable to proceed
# 3. warning - an error that can be negated/handled by the program
# 4. info - information about the program's current state
# 5. debug - should only be used during development for debugging/testing,
             as debug log statements won't be available in production

# Example:
logger.error('YOUR ERROR MESSAGE)

# What It Prints:
[ERROR YYYY-MM-DD HH:mm:SS.sss COMPONENT (THREAD_NAME)] filename:linenum YOUR ERROR MESSAGE

# This message will be logged (both displayed on the console, and saved
# in the log file for the current server's session).

# Other than the message string, any *args and **kwargs you passed to
# this function will then be passed to the corresponding function in
# Python's logging module. As for what other arguments you can pass to
# these functions, please refer to the documentation for Python logging module:
# https://docs.python.org/2/library/logging.html#logging.debug

'''

import datetime
import inspect
import logging
import os
import pprint
import re
import sys
import time
from functools import wraps

import coloredlogs
# import tzlocal

disabled = False


def set_logger(log_fn):
    global _log_fn
    _log_fn = log_fn


class Formatter(coloredlogs.ColoredFormatter):
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        # tzinfo = tzlocal.get_localzone()
        # return tzinfo.localize(dt)
        return dt

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        t = ct.strftime("%a %b %d %H:%M:%S %Z %Y")
        return t


def _get_formatter(extra_info='(%(threadName)-8.8s)'):
    f = Formatter('[%(levelname)-5.5s %(asctime)s %(name)-6.6s {}] %(message)s'
                  .format(extra_info))
    return f


_dir_to_logger_name = {
    'worker': 'Worker',
}

_logging_degree = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def setup_logging(log_dir):
    root_logger = logging.getLogger()
    formatter = _get_formatter()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    root_logger.setLevel(logging.ERROR)

    # Change this to make logging severity/verbosity.
    min_lv = logging.DEBUG

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    filename = f"log_{time.strftime('%y%m%d_%H%M%S')}"
    path = os.path.join(log_dir, filename)

    # Create a symlink to the new log file. Makes it easy to inspect logs.
    # We comment this out for now as it creates issues with Docker on Windows.
    # latest_symlink = os.path.join(log_dir, 'latest')
    # if os.access(latest_symlink, os.F_OK):
    #     os.remove(latest_symlink)
    # os.symlink(filename, latest_symlink)

    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # set up all kinds of loggers
    for _dirname, logger_name in list(_dir_to_logger_name.items()):
        logger = logging.getLogger(logger_name)
        logger.setLevel(min_lv)


def _get_dir_name(path):
    # examples of paths that will be passed to this function:
    # - dc/core/chat_server/server.py
    # - dc/analytics_platform/ipython/ipython_backend.py
    # this function will extract the name of the directory immediately under dc
    # and thus return "chat_server" in the first case, "analytics_platform" in
    # the second case
    dirs = path.split(os.sep)
    return dirs[1] if len(dirs) > 1 else dirs[0]


def _get_logger():
    dirname = inspect.stack()[2][1] if len(inspect.stack()) > 1 else ''
    dirs = dirname.split(os.sep)
    if len(dirs) > 1:
        dirname = dirs[1]
    logger_name = _dir_to_logger_name.get(_get_dir_name(dirname), 'Worker')
    return logging.getLogger(logger_name)


escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


def _preformat_msg(msg):
    msg = escape.sub('', msg)
    caller_stk = inspect.stack()[2]
    return '{}:{} {}'.format(os.path.basename(
        caller_stk[1]), caller_stk[2], msg)


def debug(*args, **kwargs):
    ''' Logs a debug message
    :param msg: the message string to be logged
    :param args: argument(s) that will be joined into a single log message
    :param kwargs: refer to documentation for Python logging module*
    *  https://docs.python.org/2/library/logging.html#logging.debug
    '''
    msg = _join_string(*args)
    if not disabled:
        _get_logger().debug(_preformat_msg(msg), **kwargs)


def info(*args, **kwargs):
    ''' Similar to debug function, but logs an info message '''
    msg = _join_string(*args)
    if not disabled:
        _get_logger().info(_preformat_msg(msg), **kwargs)


def error(*args, **kwargs):
    ''' Similar to debug function, but logs an error message '''
    msg = _join_string(*args)
    if not disabled:
        _get_logger().error(_preformat_msg(msg), **kwargs)


def critical(*args, **kwargs):
    ''' Similar to debug function, but logs a critical message '''
    msg = _join_string(*args)
    if not disabled:
        _get_logger().critical(_preformat_msg(msg), **kwargs)


def warning(*args, **kwargs):
    ''' Similar to debug function, but logs a warning message '''
    msg = _join_string(*args)
    if not disabled:
        _get_logger().warning(_preformat_msg(msg), **kwargs)


def _join_string(*args):
    ''' Join the elements in args into a single string '''
    result = ''
    for e in args:
        result += pprint.pformat(e) if not isinstance(e, str) else e
        if not isinstance(e, str) or not e.endswith('\n'):
            result += ' '
    return result


def log_print(*args):
    ''' Temporary solution used to replace all the existing print
    statements. It acts like a print function (i.e. it will print
    out all the arguments passed in, each is separated by a space
    character). Messages logged by this function are all of the
    'debug' severity (i.e. same as messages logged by a call to
    the debug function).
    Not recommended. Developers should use one of debug, info, error
    warning, critical to log any newly added messages.
    '''
    _get_logger().debug(_preformat_msg(' '.join(str(e) for e in args)))


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        info(f"{func.__module__}.{func.__name__} finished in {end_time - start_time:.2f} seconds")

        return result

    return wrapper

# By defualt set the logger at current directory.
setup_logging(".")