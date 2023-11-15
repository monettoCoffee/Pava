import signal
import threading
import time

from pava.decorator.base_decorator import BaseDecorator
from pava.utils.object_utils import time_sleep

"""
不适用于多线程环境 待改造
"""


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException


def timeout_limit(timeout_seconds):
    if timeout_seconds <= 0:
        raise Exception("[TimeoutLimit] Invalid timeout seconds '%s'!" % timeout_seconds)

    def inner_function(function):
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)
            try:
                res = function(*args, **kwargs)
                signal.alarm(0)
                return res
            except TimeoutException:
                raise Exception(
                    "[TimeoutLimit] Function calling timeout '%s', seconds is '%s'" % (
                        function.__name__, timeout_seconds))

        return wrapper

    return inner_function


if __name__ == '__main__':
    @timeout_limit(10)
    def do(a=1, b=5):
        print(a)
        print(b)
        time_sleep(1)
        time_sleep(100)

    # threading.Thread(target=do).start()
    # threading.Thread(target=do).start()
    # threading.Thread(target=do).start()
