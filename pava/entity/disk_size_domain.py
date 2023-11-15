# coding=utf-8
import os
from pava.utils.object_utils import str_is_blank


class DiskSizeDomain(object):

    @classmethod
    def get_disk_free_space(cls, disk_path="/"):
        return cls.__get_disk_object(disk_path).free / 1024 ** 2

    @classmethod
    def __get_disk_object(cls, disk_path):
        if not os.path.exists(disk_path):
            os.makedirs(disk_path)

        if str_is_blank(disk_path):
            raise Exception("[DiskDomain] Disk path is blank!")
        import psutil
        disk_object = psutil.disk_usage(disk_path)
        if disk_object is None:
            raise Exception("[DiskDomain] Disk object is None!")
        return disk_object


if __name__ == '__main__':
    print(DiskSizeDomain.get_disk_free_space())
