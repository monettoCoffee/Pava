# coding=utf-8
from __future__ import print_function

import base64, json, os, sys, time, uuid, csv

from pava.system.shell_domain import ShellDomain

if sys.version_info.major == 2:
    from functools import cmp_to_key
    from types import FunctionType, NoneType

else:
    NoneType = type(None)

from pava.decorator.decorator_impl.type_check_decorator import type_check

if sys.version_info.major == 2:
    exec ("reload(sys)")
    sys.setdefaultencoding('utf8')


class Pair(object):
    """
    基础 K-V 数据结构
    """

    def __init__(self, key=None, value=None):
        self.key = key
        self.value = value


def list_is_empty(list_param):
    """
    检查 list 是否 是 空list
    注意: 某些 容器 重写了 Len 方法 但是不属于 List 不要加强制检查
    :return: 空list 则为 True
    """
    return list_param is None or len(list_param) == 0


def list_not_empty(list_param):
    """
    检查 list 是否 不是 空list
    注意: 某些 容器 重写了 Len 方法 但是不属于 List 不要加强制检查
    :return: 空list 则为 False
    """
    return not list_is_empty(list_param)


@type_check([NoneType, dict])
def dict_is_empty(dict_param):
    """
    检查 dict 是否 是 空dict
    :return: 空dict 则为 True
    """
    return dict_param is None or len(dict_param) == 0


@type_check([NoneType, dict])
def dict_not_empty(dict_param):
    """
    检查 dict 是否 不是 空dict
    :return: 空dict 则为 False
    """
    return not dict_is_empty(dict_param)


def str_is_blank(str_param):
    """
    检查字符串是否 是 空字符串
    :return: 空字符串则为 True
    """
    if str_param is None:
        return True
    if type(str_param) is not str and not object_type_is_unicode(str_param):
        raise Exception("[str_is_blank] str param cannot is '%s' type" % type(str_param))
    return len(str_param.strip()) == 0


def str_not_blank(str_param):
    """
    检查字符串是否 不是 空字符串
    :return: 空字符串则为 False
    """
    return not str_is_blank(str_param)


@type_check([NoneType, int, float, str, long])
def get_int_value(param):
    """
    返回 param 的 int 值
    None 当作 0
    :param param: 可以传入 int str 类型
    :return: 参数对应的 int 值
    """
    if type(param) == int:
        return param
    elif type(param) == float:
        return int(param)
    elif str_is_blank(param):
        return 0
    return int(float(param.strip()))


@type_check([NoneType, int, float, str, long], int)
def get_float_value(param, round_number=1):
    """
    返回 param 的 float 值
    None 当作 0
    :param param: 可以传入 int str 类型
    :param round_number: 保留几位小数 默认1位
    :type round_number: int
    :return: 参数对应的 float 值
    :rtype: float
    """

    param_type = type(param)
    if param_type == int:
        result = float(param)
    elif param_type == float:
        result = param
    elif str_is_blank(param):
        result = 0.0
    else:
        result = float(param.strip())
    return round(result, round_number)


@type_check([list, tuple], str, int)
def str_list_compose(str_list, sep_character='', end_index=None):
    result = ""
    if end_index is None:
        end_index = len(str_list) - 1
    else:
        if 0 > end_index or end_index == len(str_list):
            raise Exception("Invalid end index %s when compose" % end_index)
    for index, str_element in enumerate(str_list):
        if index > end_index:
            return result
        if str_is_blank(str_element):
            continue
        if result == "":
            result = str_element
            continue
        result = result + sep_character + str_element
    return result


__DEFAULT_SHELL_DOMAIN = ShellDomain()


@type_check(str, str)
def execute_command(command, shell_interpreter="/bin/sh"):
    """
    执行系统命令
    :param command: Shell 命令
    :return: 命令执行结果
    """
    return __DEFAULT_SHELL_DOMAIN.execute(command)


@type_check(str, [str, NoneType], [str, NoneType])
def extract_str(original_str, begin_mark=None, end_mark=None):
    """
    从 original_str 提取从 begin_mark 到 end_mark 中间的字符串
    如果不存在 返回 None
    :param original_str: 被提取的字符串
    :param begin_mark: 起始字符串
    :param end_mark: 终止字符串
    :return: 提取到的字符串
    """
    # 检查是否存在起始位
    if begin_mark is None:
        begin_mark = ""
    begin_index = original_str.find(begin_mark)
    if begin_index == -1:
        return None
    # 获取起始位之后的字符串
    original_str = original_str[begin_index + len(begin_mark):]
    if end_mark is None or end_mark == "":
        return original_str
    # 检查是否存在末位标记 以及 是否满足 起始位 与 末位 为顺序
    end_index = original_str.find(end_mark)
    if end_index == -1:
        return None
    return original_str[:end_index]


@type_check(str, str)
def replace_to_str_end(original_str, replace_str):
    """
    返回指定字符串 replace_str 的位置 之后的字符串
    :param original_str: 指定字符串
    :param replace_str: 需要替换走的字符串
    :return: replace_str 之后的字符串
    """
    replace_index = original_str.find(replace_str)
    if replace_index == -1:
        return ""
    return original_str[replace_index + len(replace_str):]


def run_if_true(expression, execute_function):
    if expression:
        execute_function()


@type_check(str)
def str_to_ascii_list(original_str):
    ascii_list = list()
    if str_is_blank(original_str):
        return ascii_list
    for char_element in original_str:
        ascii_list.append(ord(char_element))
    return ascii_list


@type_check(list)
def ascii_list_to_str(ascii_list):
    if len(ascii_list) == 0:
        return ""
    result = ""
    for ascii_element in ascii_list:
        result = result + chr(ascii_element)
    return result


def underscore_to_camel(container):
    """
    将 list 或 dict 中的 dict 下划线 key 转为驼峰形式
    :param container: 需要转换的对象
    :return: 转换后的对象
    """
    container_type = type(container)

    # 转化 str 的逻辑
    if container_type is str:
        if "_" not in container:
            return container
        key_list = container.split("_")
        new_str = ""
        for index, key_element in enumerate(key_list):
            # 防止 key 中带有 __
            if str_is_blank(key_element):
                continue
            if index == 0:
                # 第一个不需要转大写
                new_str = new_str + key_element
            else:
                # 转换的主要逻辑
                new_str = new_str + key_element[0].upper() + key_element[1:]
        # 转换后如果没意义 则使用原 key
        if str_is_blank(new_str):
            return container
        return new_str

    # 转化 dict 的逻辑
    if container_type is dict:
        new_dict = dict()
        # 依次对每个 key 进行转换
        for key, value in container.items():
            if type(value) != str and not object_type_is_unicode(value):
                value = underscore_to_camel(value)
            new_dict[underscore_to_camel(key)] = value
        return new_dict

    # 转化 list 的逻辑
    if container_type is list:
        if list_is_empty(container):
            return container
        return [underscore_to_camel(element) for element in container]

    # 转化 tuple 的逻辑
    if container_type is tuple:
        if list_is_empty(container):
            return container
        return tuple([underscore_to_camel(element) for element in container])

    return container


def camel_to_underscore(container):
    """
    将 list 或 dict 中的 dict 驼峰形式 key 转为 下划线形式
    :param container: 需要转换的对象
    :return: 转换后的对象
    """
    container_type = type(container)

    # 转化 str 的逻辑
    if container_type is str:
        new_str = ""
        for index, character in enumerate(container):
            if character.isupper():
                if index == 0:
                    new_str += character.lower()
                else:
                    new_str = new_str + "_" + character.lower()
                continue
            new_str += character
        return new_str

    # 转化 dict 的逻辑
    if container_type is dict:
        new_dict = dict()
        # 依次对每个 key 进行转换
        for key, value in container.items():
            if type(value) != str and not object_type_is_unicode(value):
                value = camel_to_underscore(value)
            new_dict[camel_to_underscore(key)] = value
        return new_dict

    # 转化 list 的逻辑
    if container_type is list:
        if list_is_empty(container):
            return container
        return [camel_to_underscore(element) for element in container]

    # 转化 tuple 的逻辑
    if container_type is tuple:
        if list_is_empty(container):
            return container
        return tuple([camel_to_underscore(element) for element in container])

    return container


@type_check(list, None)
def list_sorted(list_object, compare_function):
    """
    list 排序方法 返回排序好的 list
    :param list_object: 需要排序的 list
    :param compare_function: 比较函数 下为示例
        # def numeric_compare(x, y):
        #     if x > y :
        #         return 1
        #     else:
        #         return -1
    :return: 排序完成的 list
    """
    if list_is_empty(list_object):
        return list_object
    if sys.version_info.major == 2:
        return sorted(list_object, key=cmp_to_key(compare_function))
    else:
        # todo 3 的没实现
        pass
    return list_object


@type_check([str, None], str)
def str_to_base64(string_object, encode="utf8"):
    if str_is_blank(string_object):
        return ""
    return base64.b64encode(string_object.encode(encode))


@type_check([str, None])
def base64_to_str(bast64_string):
    if str_is_blank(bast64_string):
        return ""
    return base64.b64decode(bast64_string)


def object_type_is_unicode(data_object):
    if sys.version_info.major != 2:
        return False
    data_object_type = type(data_object)
    return data_object_type is not str and eval("data_object_type is unicode")


__TRANS_UTF8_IGNORE_TYPE_SET = {int, str, float, NoneType, FunctionType}


@type_check(None, str)
def trans_object_properties_to_utf8(element, encode="utf8"):
    element_type = type(element)
    if element_type in __TRANS_UTF8_IGNORE_TYPE_SET:
        return element
    if object_type_is_unicode(element):
        return element.encode(encode)
    # dict 对象 转换内部属性
    elif element_type is dict:
        for key, value in element.items():
            new_key = trans_object_properties_to_utf8(key)
            new_value = trans_object_properties_to_utf8(value)

            element.pop(key)
            element[new_key] = new_value

        return element

    elif element_type is list:
        return [trans_object_properties_to_utf8(list_element) for list_element in element]
    elif element_type is tuple:
        return tuple([trans_object_properties_to_utf8(tuple_element) for tuple_element in element])
    elif element_type is set:
        return set([trans_object_properties_to_utf8(set_element) for set_element in element])

    # 除 dict 以外的对象 依次转换属性
    element_properties = dir(element)
    for property in element_properties:
        if property.startswith("_"):
            continue
        if not object_type_is_unicode(property):
            continue
        property_value = getattr(element, property)
        if not object_type_is_unicode(property_value):
            continue
        property_value = property_value.encode(encode)
        setattr(element, property, property_value)
    return element


__OBJECT_TO_DICT_IGNORE_SET = {"__class__", "__delattr__", "__dict__", "__doc__", "__format__", "__getattribute__",
                               "__hash__",
                               "__init__", "__module__", "__new__", "__reduce__", "__reduce_ex__", '__repr__',
                               '__setattr__',
                               '__sizeof__', '__str__', '__subclasshook__', '__weakref__'}

__DIRECT_TO_DICT_TYPE = {str, int, float, bool, NoneType}


def object_to_dict(to_dict_object):
    """
    将 Object 里面的属性反射出来 构建成一个 Dict
    :param to_dict_object: 参考的 Param
    :rtype: dict
    """
    # 如果有 to_dict() 方法 则优先调用 to_dict()
    if hasattr(to_dict_object, "to_dict"):
        dict_result = to_dict_object.to_dict()
        if dict_result is None:
            raise Exception(
                "[object_to_dict] Object return a none value when call to_object(), please check to_object() function!"
            )
        return dict_result

    # 首先分析 Param 类型 看是否可以构造出 Dict
    object_type = type(to_dict_object)

    # 递归的方法会调用这里 免去转换过程 直接返回为 Value
    if object_type in __DIRECT_TO_DICT_TYPE:
        return to_dict_object
    elif object_type_is_unicode(to_dict_object):
        return trans_object_properties_to_utf8(to_dict_object)

    # 如果是 List 类型的 依次转变里面的元素为 Dict
    elif object_type is list:
        result_list = [object_to_dict(element) for element in to_dict_object]
        result_list.sort()
        return result_list

    # 如果是 Tuple 类型的 同样依次转变里面的元素为 Dict
    elif object_type is tuple:
        return tuple([object_to_dict(element) for element in to_dict_object])

    # 如果是 Set 类型的 同样依次转变里面的元素为 Dict
    elif object_type is set:
        return set([object_to_dict(element) for element in to_dict_object])

    # 如果是 Dict 类型的 那么依次将里面的 K-V 进行转变
    elif object_type is dict:
        new_dict = dict()
        for key, value in to_dict_object.items():
            new_key = object_to_dict(key)
            new_value = object_to_dict(value)
            new_dict[new_key] = new_value
        return new_dict

    # 具体开始拆分 Object 转换为 Dict 的方法 先 Dir 获取所有的成员变量
    properties_list = dir(to_dict_object)

    # 判断是否需要处理有冲突的 Key 例如 同时存在 name, _name, __name
    need_use_original_member_name_as_dict_key = False
    properties_name_set = set()
    for property in properties_list:
        # 如果 property 是 Object 自带的成员变量 且 属于 Class Static 成员变量 那么忽略不计
        if property in __OBJECT_TO_DICT_IGNORE_SET or hasattr(to_dict_object.__class__, property):
            continue

        # JSON Property 表示是经过处理的 Property 类似于 JSON 里面的 Key
        json_property = property

        # 以 __ 开头的私有变量是这种形式的
        private_properties_prefix = "_" + to_dict_object.__class__.__name__ + "__"
        if property.startswith(private_properties_prefix):
            json_property = extract_str(property, private_properties_prefix)

        # 以 _ 开头的私有变量 名字里面直接是 _ 开头的
        elif property.startswith("_"):
            json_property = property[1:]

        # 如果 已经有冲突的名字了 则直接按照原成员属性进行序列化
        if json_property in properties_name_set:
            need_use_original_member_name_as_dict_key = True
            break
        else:
            properties_name_set.add(json_property)

    # 依次对 Properties 记录到 return_dict 中
    return_dict = {}
    for property in properties_list:
        # 如果 property 是 Object 自带的成员变量 且 属于 Class Static 成员变量 那么忽略不计
        if property in __OBJECT_TO_DICT_IGNORE_SET or hasattr(to_dict_object.__class__, property):
            continue

        # JSON Property 表示是经过处理的 Property 类似于 JSON 里面的 Key
        json_property = property

        # 如果需要开启真实成员变量名字作为 Key 则保留 __ 私有属性符号
        if need_use_original_member_name_as_dict_key:
            private_properties_prefix = "_" + to_dict_object.__class__.__name__
        else:
            private_properties_prefix = "_" + to_dict_object.__class__.__name__ + "__"

        # 对成员变量进行处理
        if property.startswith(private_properties_prefix):
            json_property = extract_str(property, private_properties_prefix)
        elif property.startswith("_"):
            # 如果需要开启真实成员变量名字作为 Key 则不需要在这里进行处理 保留 _
            if need_use_original_member_name_as_dict_key:
                pass
            else:
                json_property = property[1:]

        # 获取 Dict K-V 中的 V
        value_of_property = try_get_attr(to_dict_object, property)
        return_dict[json_property] = object_to_dict(value_of_property)
    return return_dict


def object_to_json(data_object):
    return json.dumps(object_to_dict(data_object))


def json_to_object(json_string):
    return trans_object_properties_to_utf8(json.loads(json_string))


def json_to_python_object(json_string):
    return camel_to_underscore(json_to_object(json_string))


@type_check(str)
def get_absolute_path_parent_path(path):
    """
    获取绝对路径的的父路径
    :param path:
    :return:
    """
    # 检测 '/' 和 非法路径
    if len(path) < 2:
        raise Exception("Can't get parent path by path '%s'" % path)
    # 获取路径层级
    path_list = path.split(os.sep)
    path_list_length = len(path_list)
    # 拼接路径
    return os.sep + str_list_compose(path_list, os.sep, path_list_length - 2)


@type_check([float, int])
def float_number_to_int_maybe(float_number):
    """
    将 6.0 形式转化为 6
    """
    if type(float_number) == int:
        return float_number
    float_number_str = str(float_number)
    has_decimal = False
    reach_point = False
    for character in float_number_str:
        if reach_point and character != "0":
            has_decimal = True
        if character == ".":
            reach_point = True
    if has_decimal:
        return float_number
    return int(float_number)


__NUMBER_STR_SET = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}


@type_check([str, int])
def character_is_number(character):
    if type(character) == int:
        return True
    return character in __NUMBER_STR_SET


def time_sleep(sleep_seconds):
    sleep_seconds = get_float_value(sleep_seconds, round_number=2)
    time.sleep(sleep_seconds)


def main_thread_hang():
    time_sleep(128)


def get_uuid():
    return str(uuid.uuid1())


@type_check(None, str)
def try_get_attr(be_get_attr_object, attr_name):
    """
    尝试获取 Object 的某个属性 自动适应私有属性
    """
    try:
        result = getattr(be_get_attr_object, attr_name)
        return result

    except:
        if not attr_name.startswith("_"):
            try:
                result = getattr(be_get_attr_object, "_" + attr_name)
                return result
            except:
                pass

            try:
                result = getattr(be_get_attr_object, "__" + attr_name)
                return result
            except:
                pass

        return None


@type_check([tuple, list, str], [tuple, list, str])
def find_list_element_in_other_list(list_a, list_b):
    for element in list_a:
        if element in list_b:
            return element
    return None


@type_check(None, str)
def export_object_properties_to_csv(export_object_list, csv_path):
    """
    将 Object 的属性解析为 CSV 导出 CSV 文件
    导出 CSV 文件的 Class 应该尽量为如下格式
    但是不强制 会使用反射机制自动判断应该如何进行导出

    class TestCSV(object):
    def __init__(self):
        self.name = "123"
        self.proxy = "4455"

    def to_csv(self):
        return [
            Pair(key="proxy", value="代理"),
            Pair(key="name", value="名字")
        ]

    :param export_object_list: 需要导出的 Object List
    :param csv_path: 导出到哪个 CSV 路径
    """
    if export_object_list is None:
        raise Exception("[export_object_properties_to_csv] Export object is none, can not be export to %s!" % csv_path)
    if str_is_blank(csv_path):
        raise Exception("[export_object_properties_to_csv] Csv path is blank, can not be export to %s!" % csv_path)

    # 如果非 List 类型的数据 则自动转变为 List
    if type(export_object_list) is not list:
        export_object_list = [export_object_list]

    # 无可用导出数据
    if len(export_object_list) == 0:
        raise Exception("[export_object_properties_to_csv] List is empty, can not be export to %s!" % csv_path)

    export_object = export_object_list[0]
    title_key_list = list()
    value_key_list = list()

    # 如果定义了 to_csv 方法 会以方法中定义的顺序和映射为准
    if hasattr(export_object, "to_csv"):
        key_value_pair_list = export_object.to_csv()
        for pair_element in key_value_pair_list:  # type: Pair
            title_key_list.append(pair_element.value)
            value_key_list.append(pair_element.key)
    else:
        # 否则 默认策略 Key 就是 Object Properties 名字
        object_dict = object_to_dict(export_object)
        for key in object_dict:
            title_key_list.append(key)
            value_key_list.append(key)

    # 创建出 CSV 文件
    with open(csv_path, "w") as csv_file:
        csv_writer = csv.writer(csv_file)

        # 首先先把表头写上
        csv_writer.writerow(title_key_list)

        # 遍历数据 List
        for element in export_object_list:
            # 把数据对象都转换成 K-V 形式
            data_object = object_to_dict(element)
            # 构建写入 CSV 的每行的数据
            value_list = list()
            for value_key in value_key_list:
                value_list.append(str(data_object.get(value_key, "")))
            # 数据行写入 CSV
            csv_writer.writerow(value_list)
