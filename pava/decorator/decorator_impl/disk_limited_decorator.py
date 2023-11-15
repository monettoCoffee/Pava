# coding=utf-8
from pava.decorator.base_decorator import BaseDecorator
from pava.entity.disk_size_domain import DiskSizeDomain
from pava.utils.object_utils import *


class DiskLimited(BaseDecorator):

    @classmethod
    def execute(cls, function_object, disk_path, limited_mb, *args, **kwargs):
        if limited_mb == 0:
            return
        if str_is_blank(disk_path):
            raise Exception("[DiskLimited] disk_path is blank!")
        if limited_mb < 0:
            raise Exception("[DiskLimited] limited_mb %s invalid!" % limited_mb)
        free_disk_space = DiskSizeDomain.get_disk_free_space(disk_path)
        if free_disk_space <= limited_mb:
            raise Exception("[DiskLimited] Function '%s' needs '%smb' to execute, but disk '%s' only has '%s'" %
                            (function_object.__name__, limited_mb, disk_path, free_disk_space)
                            )
        return function_object(*args, **kwargs)


def disk_limited(disk_path, limited_mb):
    def disk_limited_prepare(function):
        def disk_limited_wrapper(*args, **kwargs):
            return DiskLimited.execute(function, disk_path, limited_mb, *args, **kwargs)

        return disk_limited_wrapper

    return disk_limited_prepare


if __name__ == "__main__":
    @disk_limited("/", 4000)
    def fun(a, b=1):
        return a + b


    print(fun(1, b=43))
