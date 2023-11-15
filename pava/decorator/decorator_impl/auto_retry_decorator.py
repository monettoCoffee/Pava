# coding=utf-8
import time

from pava.component.p_log import PLog

from pava.decorator.base_decorator import BaseDecorator
from pava.utils.object_utils import time_sleep


class AutoRetry(BaseDecorator):

    @classmethod
    def execute(cls, function_object, retry_times, retry_interval, *args, **kwargs):
        if retry_times < 0:
            raise Exception("[AutoRetry] Invalid retry times '%s'!" % retry_times)

        result = None
        while retry_times > -1:
            try:
                result = function_object(*args, **kwargs)
                return result
            except Exception as e:
                if retry_times == 0:
                    raise e
                else:
                    PLog.gets().info(
                        "[AutoRetry] Occurred Exception when executing '%s', exception: %s, retrying..." % (
                            function_object.__name__, str(e))
                    )
                    if retry_interval > 0:
                        time_sleep(retry_interval)
                    retry_times -= 1
        return result


def auto_retry(retry_times, retry_interval):
    def prepare(function):
        def auto_retry_wrapper(*args, **kwargs):
            return AutoRetry.execute(function, retry_times, retry_interval, *args, **kwargs)

        return auto_retry_wrapper

    return prepare


if __name__ == '__main__':
    @auto_retry(1, 1)
    def raise_exception(a, b=6):
        print(a + b)
        raise Exception("R")


    raise_exception(1, 2)
