# coding=utf-8
from pava.decorator.base_decorator import BaseDecorator

"""
强制类型检查装饰器
在执行函数前强制检查参数类型
使用示例请参考 main
对应位置参数使用 type(None) 则表示不检查
"""


class TypeCheck(BaseDecorator):

    @classmethod
    def execute(cls, function, check_parameter_type_tuple, args, kwargs):
        """
        函数的参数类型检查 类封装
        :param function: 要检查的函数对象
        :param check_parameter_type_tuple: 预期函数类型的tuple
        :param args: 函数必填的参数传入的值
        :param kwargs: 函数可选参数传入的值
        """
        parameter_name_list = function.__code__.co_varnames
        parameter_count = function.__code__.co_argcount

        # PartA. 检查参数是否多传 (Python 自身不会报错)
        if len(args) + len(kwargs) > parameter_count:
            raise Exception("[TypeCheck] Function '%s' only has %s parameter but get %s" % (
                get_function_name(function), parameter_count, len(args) + len(kwargs)))
        # PartB. 检查参数是否都参与了检查 (不需要检查的参数要用 None 占位)
        if len(check_parameter_type_tuple) != parameter_count:
            raise Exception("[TypeCheck] Function '%s' has %s parameter but check %s" % (
                get_function_name(function), parameter_count, len(check_parameter_type_tuple)))
        # PartC. 必填参数检查
        index = 0
        for parameter_value in args:
            target_type = check_parameter_type_tuple[index]
            parameter_value_type = type(parameter_value)
            # target_type 为 None 时 表示该参数不加入检查
            if target_type is None:
                index += 1
                continue
            # target_type 与 实际传入类型相同时 检查通过
            elif parameter_value_type == target_type:
                index += 1
                continue
            # 检查允许存在多个预期类型
            elif type(target_type) == list:
                check_pass = False
                # 对 list 中的预期参数类型进行逐个检查
                for target_type_list_element in target_type:
                    if target_type_list_element == parameter_value_type:
                        check_pass = True
                        break
                if check_pass:
                    index += 1
                    continue
                else:
                    # list 中的指定类型检查不通过的情况
                    raise Exception("[TypeCheck] Function '%s' parameter '%s' should be type of '%s' instead of '%s'" % (
                        get_function_name(function), parameter_name_list[index], target_type,
                        type(parameter_value)))
            # 检查传入类型是否为指定类型的子类
            # 部分 IDE 会存在类型检查不通过的情况 故改用 eval 执行
            elif eval("isinstance(parameter_value, target_type)"):
                index += 1
                continue
            # 强制检查类型 且 类型不相同 且 非子类的情况
            else:
                raise Exception("[TypeCheck] Function '%s' parameter '%s' should be type of '%s' instead of '%s'" % (
                    get_function_name(function), parameter_name_list[index], target_type,
                    type(parameter_value)))
        # PartD. 传入的额外参数检查
        while index < parameter_count:
            parameter_name = parameter_name_list[index]
            parameter_value = kwargs.get(parameter_name, None)
            target_type = check_parameter_type_tuple[index]
            # 直接判断类型是否相同
            parameter_value_type = type(parameter_value)
            if target_type is None or parameter_value is None or parameter_value_type == target_type:
                index += 1
                continue
            check_pass = False
            if type(target_type) == list:
                for type_element in target_type:
                    if type_element == parameter_value_type:
                        check_pass = True
                        index += 1
                        break
            if not check_pass:
                raise Exception("[TypeCheck] Function '%s' parameter '%s' should be type of '%s' instead of '%s'" % (
                    get_function_name(function), function.__code__.co_varnames[index], target_type,
                    type(parameter_value)))
        # 参数默认值
        # print("default parameter value: " + str(func.__defaults__))


def type_check(*check_parameter_type_tuple):
    """
    函数的参数类型检查 装饰器封装
    :param check_parameter_type_tuple: 预期函数类型的tuple
    """

    def prepare_type_check(function):
        """
        :param function: 要检查的函数对象
        """

        def type_check_wrapper(*args, **kwargs):
            """
            :param args: 函数必填的参数传入的值
            :param kwargs: 函数可选参数传入的值
            """
            TypeCheck.execute(function, check_parameter_type_tuple, args, kwargs)
            return function(*args, **kwargs)

        return type_check_wrapper

    return prepare_type_check


def get_function_name(function):
    """
    如果 function_name 是魔法函数 就只能从错误栈信息中找是哪里出错了
    这里加入文件名 方便看
    :param function: 函数对象
    :return: 优化后的函数名
    """
    function_name = function.__name__
    # 这里加上 文件名 好看一些
    if function_name.startswith("__") and function_name.endswith("__"):
        function_name = "(%s) %s" % (function.func_code.co_filename.split("/")[-1], function_name)
    return function_name


if __name__ == '__main__':
    # 在想要使用强制类型检查的函数上 加入 @type_check 即可
    @type_check([int, str], int, None)
    def test_function(can_be_multi_type_arg, int_type_arg, not_check_arg):
        print("Execute!")


    test_function("Hello", 1, dict())
    test_function("Hello", "World")
