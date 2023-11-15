# coding=utf-8
import threading

from pava.decorator.base_decorator import BaseDecorator
from threading import Lock

"""
单线程同步修饰器
对比原生的 Lock 支持线程重入 以及 仅用 Key 就可以实现多对象单锁
多个线程运行同一个函数时 仅有一个线程可以运行
基于 threading.Lock 使用方式设计参考 Java Synchronized
使用示例请参考 main
"""


class Synchronized(BaseDecorator):
    __lock_object_dict = dict()
    __generate_lock = Lock()

    __thread_lock = Lock()
    __thread_and_lock_key = dict()

    @classmethod
    def execute(cls, function, synchronize_lock_key, *args, **kwargs):
        # 首先尝试拿到锁对象进行上锁处理
        lock_object = cls.__lock_object_dict.get(synchronize_lock_key, None)
        # 如果锁对象没有拿到的话
        if lock_object is None:
            # 单线程处理 生成这个锁对象 仅仅生成 所以极快就会被释放
            lock_object = cls.__generate_lock_object(synchronize_lock_key)

        # Python 的 Lock() 对象是不支持重入的 这里要判断是否是同一个线程 以支持重入
        if cls.is_same_thread(synchronize_lock_key):
            # 如果是同一个线程的话 那么允许直接执行业务逻辑 实现类似重入的效果
            result = function(*args, **kwargs)
            # 执行业务逻辑完毕 直接返回
            return result
        else:
            # 尝试获取锁 进行竞争
            with lock_object:
                # 在执行业务逻辑前 将这个 线程 与 锁 做绑定关系 方便后面可以重入
                cls.__thread_and_lock_key[synchronize_lock_key] = str(threading.current_thread())
                # 执行业务逻辑
                result = function(*args, **kwargs)
                # 执行完后 立即解除 线程 与 锁 的绑定关系
                cls.__thread_and_lock_key[synchronize_lock_key] = None
                # 返回业务执行结果
                return result

    @classmethod
    def is_same_thread(cls, synchronize_lock_key):
        # 当前想要获取锁的线程的标记
        current_thread = str(threading.current_thread())
        # 检查当前线程标记 是否与 当前持有这个锁的标记 一致
        thread_of_lock = cls.__thread_and_lock_key.get(synchronize_lock_key, None)
        if thread_of_lock is not None:
            return current_thread == thread_of_lock
        with cls.__thread_lock:
            # 当 当前线程获取 Key 对应的锁的线程标记 获取不到时 双重检查
            thread_of_lock = cls.__thread_and_lock_key.get(synchronize_lock_key, None)
            # 确实没有标记的情况下 标记为自己
            if thread_of_lock is None:
                cls.__thread_and_lock_key[synchronize_lock_key] = current_thread
                # 这里返回 False 是要让当前线程尝试获取锁 因为一定是可以成功获取的
                return False
            return current_thread == thread_of_lock

    @classmethod
    def __generate_lock_object(cls, synchronize_lock_key):
        with cls.__generate_lock:
            lock_object = cls.__lock_object_dict.get(synchronize_lock_key, None)
            if lock_object is None:
                lock_object = Lock()
                cls.__lock_object_dict[synchronize_lock_key] = lock_object
            return lock_object


def synchronized(synchronize_lock_key, enable=True):
    def prepare_synchronized(function):
        def synchronized_wrapper(*args, **kwargs):
            if enable is False:
                return function(*args, **kwargs)
            return Synchronized.execute(function, synchronize_lock_key, *args, **kwargs)

        return synchronized_wrapper

    return prepare_synchronized


if __name__ == '__main__':
    import time
    from threading import Thread

    # 测试多线程对 test_number 进行加减操作
    test_number = 0


    # 可自行测试 未带有 @synchronized 的 run 以验证结果
    class ObjectWithSynchronized(object):

        def __init__(self, concurrent_lock_key):
            """
            :param concurrent_lock_key: 由于 synchronized 是按照 synchronize_lock_key 字符串进行加减锁
                                        故 多个对象要区分 synchronize_lock_key 以让多个对象的同名函数并行运行
                                        同样地, 也可以使用多个对象相同 synchronize_lock_key 使得同一时间仅有单实例运行
            """
            self.concurrent_lock_key = concurrent_lock_key
            wrapper = self

            class InlineClass(object):
                @synchronized("%s#run" % concurrent_lock_key, enable=True)
                def run(self):
                    global test_number
                    for _ in range(1000000):
                        test_number += 1

            self._instance = InlineClass()

        def run(self):
            # 由于在外部无法获取 self 故 采用内部封装函数 使得 synchronize_lock_key 可以为 concurrent_lock_key
            self._instance.run()

    # test_object = ObjectWithSynchronized("test_object_01")
    #
    # t = time.time()
    # thread_01 = async_execute(test_object.run)
    # thread_02 = async_execute(test_object.run)
    # thread_03 = async_execute(test_object.run)
    # thread_04 = async_execute(test_object.run)
    #
    # thread_01.result()
    # thread_02.result()
    # thread_03.result()
    # thread_04.result()
    #
    # print(test_number)
    # print(time.time() - t)
