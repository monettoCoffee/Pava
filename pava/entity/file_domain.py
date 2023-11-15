# coding=utf-8
from pava.component.p_log import PLog

from pava.utils.object_utils import *
from pava.utils.system_utils import path_is_dir, path_is_file

"""
文件实体对象
使用 文件的绝对路径, 文件大小, 文件注释 来描述某些文件对象
存在一些常用的文件操作方法
"""

CHECK_PATH = os.sep + os.sep


class FileDomain(object):
    @type_check(None, str, str, str)
    def __init__(self, file_absolute_path, file_name=None, comment=None):
        """
        创建文件实体对象
        :param file_absolute_path: 文件路径
        :param comment: 文件注释
        """
        if str_is_blank(file_absolute_path):
            raise Exception("[FileDomain] Path is blank")

        # 禁止例如 // 文件名后缀 或 前缀
        if file_absolute_path.endswith(CHECK_PATH) or file_absolute_path.startswith(CHECK_PATH):
            raise Exception("[FileDomain] Path %s is invalid" % file_absolute_path)

        # xxx/abc/ 形式 消除后面的 / 使之变为 xxx/abc
        if file_absolute_path != "/" and file_absolute_path.endswith(os.sep):
            file_absolute_path = file_absolute_path[:-1]

        # 获取文件名
        if str_is_blank(file_name):
            file_name = file_absolute_path.split(os.sep)[-1]
        self.file_name = file_name  # type: str

        # 文件的绝对路径
        self.file_absolute_path = file_absolute_path

        # 文件的注释
        self.comment = comment

        # 文件大小
        self.file_size = None

        # 目录下的子文件 (如果是目录的话)
        self._children_file_domain = None

        # 文件类型
        self.file_type = None

        # 刷新文件信息 进行初始化
        self.refresh()

    def _build_file_size(self, get_directory_size=False):
        """
        获取该文件的文件大小
        :param get_directory_size: 如果是文件夹 是否获取内部文件的总大小
        """
        # 确保文件是存在的
        if not self.get_exist_status():
            return
        # 如果是单个文件 则直接获取大小
        if self.path_is_file():
            self.file_size = os.path.getsize(self.file_absolute_path)

        # 如果是目录 允许获得目录大小 则 获取子文件大小
        elif self.path_is_dir() and get_directory_size:
            # todo 获取子文件大小的情况怎么处理
            pass

    def get_exist_status(self):
        """
        获取文件存在状态 并保存到 self.exist_status
        :return: 文件存在状态
        """
        return os.path.exists(self.file_absolute_path)

    def create(self):
        """
        创建空白文件 文件必须不存在
        """
        if self.get_exist_status():
            raise Exception("[FileDomain] Path %s is exist, Cant create repeatable" % self.file_absolute_path)
        with open(self.file_absolute_path, "w"):
            pass

    def write(self, content):
        """
        向文件覆盖写入内容
        当文件已经存在了 那么建立 tmp 文件 随后进行 替换
        :param content: 要写入的内容
        """
        if self.get_exist_status():
            temp_file = FileDomain(self.file_absolute_path + "_tmp", self.file_name + "_tmp")
            temp_file.write(content)
            self.delete()
            temp_file.rename(self.file_absolute_path)
        else:
            with open(self.file_absolute_path, 'a') as self_file:
                self_file.write(content)
        self.refresh()

    def append(self, content, check_file_exist=False):
        """
        向文件中追加内容
        :param check_file_exist: 是否检查文件存在状态
        :param content: 追加的内容
        """
        if check_file_exist and not self.get_exist_status():
            raise Exception("[FileDomain] Path %s not exist, Cant append" % self.file_absolute_path)
        with open(self.file_absolute_path, 'a') as self_file:
            self_file.write(content)

    def delete(self):
        """
        删除文件
        """
        if not self.get_exist_status():
            raise Exception("[FileDomain] Path %s not exist, Cant delete" % self.file_absolute_path)
        os.remove(self.file_absolute_path)

    def rename(self, new_file_path):
        """
        重命名文件
        :param new_file_path: 新名字
        """
        os.rename(self.file_absolute_path, new_file_path)

    def path_is_dir(self):
        return path_is_dir(self.file_absolute_path)

    def path_is_file(self):
        return path_is_file(self.file_absolute_path)

    def _get_file_type(self):
        if self.get_exist_status() and self.path_is_dir():
            return "directory"
        else:
            if "." in self.file_name:
                return self.file_name.split(".")[-1]
        return ""

    def get_children_file_domain(self, filter_function=None):
        """
        获取所有子目录文件的 FileDomain 对象
        :param filter_function: 文件过滤函数
        :return: 子文件的 FileDomain 对象构成的 list
        :rtype: list
        """
        if self.path_is_file():
            raise Exception("[FileDomain] Can't get child files because path %s is not dir" % self.file_name)
        if self._children_file_domain is None:
            self._children_file_domain = list()
            # Darwin 系统下 即使使用 Root 权限 依然会出现无法访问的情况 故 加 except
            try:
                for path in os.listdir(self.file_absolute_path):
                    absolute_path = self.file_absolute_path + "/" if self.file_absolute_path != "/" else self.file_absolute_path
                    child_file_domain = FileDomain(absolute_path + path, file_name=path)
                    # 对子文件进行过滤
                    if filter_function is not None and filter_function(child_file_domain):
                        continue
                    self._children_file_domain.append(child_file_domain)
            except OSError as os_error:
                PLog.gets().exception(os_error)
            self._children_file_domain.sort(FileDomain._default_compare_file_domain)
        return self._children_file_domain

    def get_parent_file_domain(self):
        """
        获取父路径的实体对象
        :return:
        """
        # '/' 和 非法路径检测
        if len(self.file_absolute_path) < 2:
            raise Exception("[FileDomain] Path '%s' haven't parent path" % self.file_absolute_path)
        parent_file_domain = FileDomain(get_absolute_path_parent_path(self.file_absolute_path))

        # 理论上父路径必须是目录
        if parent_file_domain.get_exist_status() and parent_file_domain.path_is_file():
            raise Exception("[FileDomain] Parent path '%s' is not directory" % parent_file_domain.file_absolute_path)
        return parent_file_domain

    def refresh(self):
        """
        刷新文件信息
        """
        self._build_file_size()
        self.file_type = self._get_file_type()
        if self.get_exist_status() and self.path_is_dir() and self._children_file_domain is not None:
            self.get_children_file_domain()

    def create_dir(self):
        if self.get_exist_status() is False:
            os.makedirs(self.file_absolute_path)

    def get_file_content(self):
        if self.get_exist_status() is False:
            raise Exception("[FileDomain] File '%s' not exists, cannot get content!" % self.file_name)
        with open(self.file_absolute_path, "r") as file:
            lines = file.readlines()
            return str_list_compose(lines)

    @type_check(None, int)
    def to_dict(self, with_children_file_domain_level=1):
        """
        将 FileDomain 对象转化为 Dict
        :param with_children_file_domain_level: 获取几级子目录 默认获取直接子目录
        :return: dict 对象
        """
        dict_object = {
            # 文件的绝对路径
            "file_absolute_path": self.file_absolute_path,
            # 文件的文件名
            "file_name": self.file_name,
            # 文件的注释
            "comment": self.comment,
            # 文件大小
            "file_size": self.file_size,
            # 文件类型
            "file_type": self.file_type,
        }
        if self.path_is_file() or with_children_file_domain_level == 0:
            return dict_object
        children_file_domain = list()
        # 目录下的子文件 (如果是目录的话)
        dict_object["children_file_domain"] = children_file_domain
        for child_domain in self.get_children_file_domain():
            children_file_domain.append(child_domain.to_dict(with_children_file_domain_level - 1))
        return dict_object

    @classmethod
    def _default_compare_file_domain(cls, file_domain_a, file_domain_b):
        """
        默认文件类型排序方法
        :type file_domain_a: FileDomain
        :type file_domain_b: FileDomain
        :rtype: int
        """
        # 目录在文件的排序前面
        if file_domain_a.path_is_dir() and file_domain_b.path_is_file():
            return -1
        if file_domain_a.path_is_file() and file_domain_b.path_is_dir():
            return 1

        # 如果两者名字都带有 . 那么先去掉 .
        file_name_a = file_domain_a.file_name
        file_name_b = file_domain_b.file_name
        while file_name_a.startswith(".") and file_name_b.startswith("."):
            file_name_a = file_name_a[1:]
            file_name_b = file_name_b[1:]

        # 有 . 的 在没 . 的后面
        if file_name_a.startswith(".") and not file_name_b.startswith("."):
            return 1
        if not file_name_a.startswith(".") and file_name_b.startswith("."):
            return -1

        # 直接比较字符串大小
        return 1 if file_name_a > file_name_b else -1

    def __str__(self):
        dict_object = self.to_dict()
        return object_to_json(dict_object)


if __name__ == '__main__':
    t = FileDomain("/Users/monetto")
    pass
