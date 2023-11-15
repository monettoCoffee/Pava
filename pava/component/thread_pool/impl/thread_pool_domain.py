# coding=utf-8
from pava.component.p_log import PLog
from pava.component.thread_pool.core.thread_pool_executor_wrapper import ThreadPoolExecutorWrapper

from pava.component.thread_pool.interface.abstract_thread_pool_domain import AbstractThreadPoolDomain
from pava.decorator.decorator_impl.synchronized_decorator import synchronized
from pava.utils.async_utils import cycle_execute
from pava.utils.object_utils import *

"""
线程池域对象
对 Futures 模块中 ThreadPoolExecutor 的一次封装
支持 实时获取执行中的线程的数量等信息
维护了 线程任务的执行信息 5分钟更新一次
"""

THREAD_POOL_LOCK_KEY_PREFIX = "ThreadPoolDomain_"
THREAD_POOL_LOCK_KEY_COUNT = 0


@synchronized(THREAD_POOL_LOCK_KEY_PREFIX)
def get_thread_pool_domain(pool_worker_size=2):
    global THREAD_POOL_LOCK_KEY_COUNT
    THREAD_POOL_LOCK_KEY_COUNT += 1

    thread_pool_operation_lock_key = THREAD_POOL_LOCK_KEY_PREFIX + str(THREAD_POOL_LOCK_KEY_COUNT)

    class ThreadPoolDomain(AbstractThreadPoolDomain):
        type_check(int)

        def __init__(self, pool_worker_size):
            self._pool_worker_size = pool_worker_size
            # 保存自身任务的线程池
            self._thread_pool = ThreadPoolExecutorWrapper(max_workers=pool_worker_size)
            self._task_dict = dict()
            self.task_list = list()
            cycle_execute("%s_maintain_task_list" % id(self), self._maintain_task_list, 30)

        def get_running_task_quantity(self):
            return len(self._task_dict)

        def get_pool_worker_size(self):
            return self._pool_worker_size

        @synchronized(thread_pool_operation_lock_key)
        def add_task(self, function_object, thread_task_name=None, args=None, kwargs=None):
            if str_is_blank(thread_task_name):
                raise Exception("[ThreadPoolDomain] Task name cant not be blank, when add task!")
            if thread_task_name in self._task_dict:
                raise Exception("[ThreadPoolDomain] Task name '%s' already exists!" % thread_task_name)

            args = tuple() if args is None else args
            kwargs = dict() if kwargs is None else kwargs

            thread_pool_task = ThreadPoolTask(thread_task_name, function_object, self, args, kwargs)

            self._task_dict[thread_task_name] = thread_pool_task
            self.task_list.append(thread_pool_task)

            task = self._thread_pool.submit(thread_pool_task.execute)
            return task

        @synchronized(thread_pool_operation_lock_key)
        def after_execute_callback_(self, task):
            """
            异步任务完成执行 回调方法
            :param task: 异步任务实体类
            :type task: ThreadPoolTask
            """
            self._task_dict.pop(task.task_name)

        @synchronized(thread_pool_operation_lock_key)
        def _maintain_task_list(self):
            """
            维护线程池中的任务信息 将 超过5分钟的 已经完成的任务 或 异常的任务 清除
            """
            task_list = self.task_list
            index = len(task_list)
            while index > 0:
                index -= 1
                task = task_list[index]
                if task.end_time is not None and get_current_timestamp() - task.end_time > 5 * 60:
                    task_list.pop(index)
                if task.exception_time is not None and get_current_timestamp() - task.exception_time > 5 * 60:
                    task_list.pop(index)

    class ThreadPoolTask(object):
        @type_check(None, [str, NoneType], None, AbstractThreadPoolDomain, None, None)
        def __init__(self, task_name, function_object, thread_pool_domain, args, kwargs):
            """
            :type task_name: str
            :param task_name: 异步任务对应的名字

            :type function_object: types.FunctionType
            :param function_object: 异步任务对应的函数

            :type thread_pool_domain: AbstractThreadPoolDomain
            :param thread_pool_domain: 这个 Task 所处的线程池

            """
            self.task_name = task_name
            self.submit_time = int(time.time())
            self.execute_time = None
            self.end_time = None
            self.exception_time = None
            self.exception_str = None
            self.function_object = function_object
            self.thread_pool_domain = thread_pool_domain
            self.args = args
            self.kwargs = kwargs

        def execute(self):
            self.execute_time = int(time.time())
            try:
                result = self.function_object(*self.args, **self.kwargs)
                self.end(exception=None)
                return result
            except Exception as e:
                self.end(exception=e)
                PLog.gets().exception(e)
                raise e

        def end(self, exception=None):
            if exception is None:
                self.end_time = get_current_timestamp()
            else:
                self.exception_time = get_current_timestamp()
                self.exception_str = str(exception)
            self.thread_pool_domain.after_execute_callback_(self)

        def to_dict(self):
            return {
                "task_name": self.task_name,
                "submit_time": self.submit_time,
                "execute_time": self.execute_time,
                "end_time": self.end_time,
                "exception_time": self.exception_time,
                "execute_str": self.exception_str
            }

    return ThreadPoolDomain(pool_worker_size)


def hello(a, b=1):
    print(a)
    print(b)
    return "11222333"


if __name__ == '__main__':
    p = get_thread_pool_domain()  # type: AbstractThreadPoolDomain
    p.add_task(hello, thread_task_name="4", args=(5, 6))
    print(p.get_running_task_quantity())
    print(p.get_running_task_quantity())
