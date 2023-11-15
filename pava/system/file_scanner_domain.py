# coding=utf-8

from pava.component.p_log import PLog

from pava.entity.file_domain import FileDomain
from pava.utils.object_utils import *
from pava.utils.system_utils import path_is_file

"""
系统文件扫描器
注意: 为了防止运行时权限不足 应该以 root 模式运行
"""


class FileScannerDomain(object):
    @type_check(None, str, [set, NoneType])
    def __init__(self, scan_path, ignore_path=None):
        if str_is_blank(scan_path):
            raise Exception("Scan path must not blank.")
        if path_is_file(scan_path):
            raise Exception("Path must be directory not file.")
        self._scan_path = scan_path
        self._ignore_path = None
        self.set_ignore_path(ignore_path)

    def set_ignore_path(self, ignore_path):
        self._ignore_path = set()
        if ignore_path is None:
            ignore_path = set()
        for ignore_path_element in ignore_path:
            # xxx/abc/ 形式 消除后面的 / 使之变为 xxx/abc
            if ignore_path_element != "/" and ignore_path_element.endswith(os.sep):
                ignore_path_element = ignore_path_element[:-1]
            self._ignore_path.add(ignore_path_element)

    def execute(self):
        root_file_entity = FileDomain(self._scan_path)
        if root_file_entity.path_is_file():
            return root_file_entity
        elif not root_file_entity.path_is_dir():
            raise Exception("Unknown file type %s" % root_file_entity.file_absolute_path)
        # 当 root_file_entity 是目录的处理逻辑
        # 记录 绝对路径 和 file_entity 映射关系
        absolute_path_entity = dict()

        scan_task = [root_file_entity]
        for task_entity in scan_task:
            # todo 这里是不是可以删除?
            if path_is_ignore(task_entity, self._ignore_path):
                PLog.gets().debug("[file_scanner_domain] Info: path %s is be ignore" % task_entity.file_absolute_path)
                continue
            if task_entity.path_is_file():
                continue
            elif task_entity.path_is_dir():
                absolute_path_entity[task_entity.file_absolute_path] = task_entity

                # 在 file_entity 中进行过滤逻辑
                def path_filter(file_entity):
                    """
                    根据文件名称做过滤
                    :param file_entity: 扫描到的文件对象
                    :type file_entity: FileDomain
                    :return: 是否在返回结果中进行过滤
                    :rtype: bool
                    """
                    # 按照绝对路径过滤
                    if file_entity.file_absolute_path in self._ignore_path:
                        return True
                    # 按照文件名进行过滤
                    if file_entity.file_name in self._ignore_path:
                        return True
                    return False

                children_file_entity = task_entity.get_children_file_domain(path_filter)
                for child_file_entity in children_file_entity:
                    scan_task.append(child_file_entity)
            else:
                PLog.gets().warning(
                    "[file_scanner_domain] Warning: path %s is unknown type" % task_entity.file_absolute_path
                )
        return root_file_entity


@type_check(FileDomain, set)
def path_is_ignore(file_entity, ignore_path):
    # 按照绝对路径过滤
    if file_entity.file_absolute_path in ignore_path:
        return True
    # 按照文件名进行过滤
    if file_entity.file_name in ignore_path:
        return True
    return False
