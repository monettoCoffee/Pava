# coding=utf-8
import bisect

from pava.utils.object_utils import *

# 记录 类型 与 默认值的映射关系
TYPE_CODE_DEFAULT_VALUE_DICT = {
    int: "0",
    str: "\"\"",
    float: "0.0",
    list: "list()"
}


class ClassPrototype(object):
    def __init__(self):
        self.class_name = "_" + str(id(self))
        self.class_properties_list = list()
        self.class_properties_dict = dict()


class PropertyPrototype(object):
    def __init__(self):
        self.property_name = ""
        self.property_value_type = None

    def __lt__(self, other_property_prototype):
        return self.property_name < other_property_prototype.property_name


class PythonCodeGenerator(object):
    __INVALID_CHARACTER_DICT = {
        "0": "zero_", "1": "one_", "2": "two_", "3": "three_", "4": "four_", "5": "five_",
        "6": "six_", "7": "seven_", "8": "eight_", "9": "nine_",
        "\\": "backslash_", "/": "forward_slash_", "%": "percentage_",
        "!": "exclamation_", "@": "at_", "#": "sharp_", "$": "dollar_", "^": "caret_",
        "&": "and_", "*": "asterisk_", "(": "left_bracket_", ")": "right_bracket_",
        "[": "mid_left_bracket_", "{": "big_left_bracket_", "]": "mid_right_bracket_",
        "}": "big_right_bracket_", ";": "semicolon_", ":": "colon_", "'": "apostrophe_",
        "\"": "quotation_", ",": "comma_", "<": "smaller_", ".": "point_", ">": "bigger_",
        "?": "question_", "|": "or_", "-": "subtraction_", "=": "equal_", "+": "plus_"
    }

    def __init__(self):
        self.class_prototype_list = list()

    @classmethod
    @type_check(None, str)
    def generate_python_struct_from_json(cls, json_str):
        # 进行参数检查 以及 基本的序列化
        if str_is_blank(json_str):
            raise Exception("[PythonCodeGenerator] Json is blank! Please check param!")
        python_object = json_to_python_object(json_str)
        type_python_object = type(python_object)

        # 如果是 List 则以里面的 0 元素为准
        if type_python_object is list:
            if len(python_object) == 0:
                raise Exception("[PythonCodeGenerator] Json list is empty! Please check param!")
            python_object = python_object[0]

        # 必须是 Dict K-V 形式的
        if type(python_object) is not dict:
            raise Exception("[PythonCodeGenerator] Json cannot be unmarshal to dict! Please check param!")

        # 切换到 从 Dict 转化流程
        return cls.generate_python_struct_from_dict(python_object)

    @classmethod
    @type_check(None, dict, None)
    def generate_python_struct_from_dict(cls, dict_object, python_code_generator=None):
        """
        :type dict_object: dict
        :type python_code_generator: PythonCodeGenerator
        """
        # 记录本次所有 Class Property
        if python_code_generator is None:
            python_code_generator = PythonCodeGenerator()

        # todo 复用变为最后压缩其本身
        # 创建 Class 原型
        class_prototype = ClassPrototype()

        # 遍历 Dict K-V 构建出 原形
        for key, value in dict_object.items():
            # Key 必须是 Str 类型的
            if type(key) is not str:
                raise Exception("[PythonCodeGenerator] Dict key '%s' is not str!" % str(key))
            property_prototype = PropertyPrototype()
            property_prototype.property_name = key

            # 对 Value 进行检查 同时赋值默认值
            type_value = type(value)

            # 如果属于 Dict 类型的 那么检查 ClassProperty 是否可以进行复用
            if type_value is dict:
                property_prototype.property_value_type = cls.generate_python_struct_from_dict(value,
                                                                                              python_code_generator)

            # 如果存在默认值 则以默认值为准
            elif type_value in TYPE_CODE_DEFAULT_VALUE_DICT:
                property_prototype.property_value_type = type_value
            else:
                raise Exception("[PythonCodeGenerator] Unexpected value type '%s' when from dict!" % str(type_value))

            # 按照名字排序有序插入
            bisect.insort(class_prototype.class_properties_list, property_prototype)
            # 记录 Key 与 Value 类型 方便成员 Class 进行复用
            class_prototype.class_properties_dict[key] = type_value

        python_code_generator.class_prototype_list.append(class_prototype)
        return cls._resolve_class_prototype_to_code(class_prototype)

    @classmethod
    @type_check(None, list, dict)
    def _check_class_property_can_be_multiplex(cls, class_prototype_list, dict_object):
        for class_prototype in class_prototype_list:  # type: ClassPrototype
            a = 5
            for key, value in dict_object.items():
                b = 6
                multiplex_class_property_type = class_prototype.class_properties_dict.get(key, None)
                if multiplex_class_property_type is None:
                    break
            # todo 构建一个每个 CP 的 Dict 吧...

    @classmethod
    @type_check(None, ClassPrototype)
    def _resolve_class_prototype_to_code(cls, class_prototype):
        """
        解析 ClassPrototype Object 到代码
        :type class_prototype: ClassPrototype
        """
        # 其所依赖的 Class 的代码
        result_code_str = "\n"
        # ClassPrototype 对应的 Class 的代码
        append_code_str = "class %s(object):\n" % class_prototype.class_name
        append_code_str = append_code_str + "    def __init__(self):\n"

        # 默认自动给成员变量加入 _ 这里判断是否可以使用这个策略
        auto_add_private_members = True
        same_property_name_set = set()
        for property_prototype in class_prototype.class_properties_list:  # type: PropertyPrototype
            # 主要是判断 加 _ 之后 名字是否有冲突
            if not property_prototype.property_name.startswith("_"):
                same_name = "_" + property_prototype.property_name
            else:
                same_name = property_prototype.property_name
            if same_name in same_property_name_set:
                auto_add_private_members = False
                break
            else:
                same_property_name_set.add(same_name)

        # 遍历 组装代码结构
        for property_prototype in class_prototype.class_properties_list:
            property_value_type = property_prototype.property_value_type

            # 所有成员 名字统一变更为私有化 方便区分非法字符 与 成员类型限定
            type_code_default_value = TYPE_CODE_DEFAULT_VALUE_DICT.get(property_value_type, None)
            property_name = property_prototype.property_name  # type: str

            # 检查是否存在非法字符 不可以作为 Python 成员变量名的
            invalid_character_in_property_name = find_list_element_in_other_list(
                property_name,
                PythonCodeGenerator.__INVALID_CHARACTER_DICT.keys()
            )

            # 依次替换非法字符
            while invalid_character_in_property_name is not None:
                replace_element = PythonCodeGenerator.__INVALID_CHARACTER_DICT.get(invalid_character_in_property_name)
                property_name = property_name.replace(
                    invalid_character_in_property_name,
                    # 使用 ID 尽量保证单次生成不重复
                    replace_element + str(id(list())) + "_",
                    1
                )

                invalid_character_in_property_name = find_list_element_in_other_list(
                    property_name,
                    PythonCodeGenerator.__INVALID_CHARACTER_DICT.keys()
                )

            # 尽可能封装 Property 的 成员变量
            if auto_add_private_members and not property_name[0].startswith("_"):
                property_name = "_" + property_name

            # 当找到类型对应的默认值了 执行进行赋值操作
            if type_code_default_value is not None:
                append_code_str = append_code_str + "        self.%s = %s\n" % (
                    property_name, str(type_code_default_value)
                )

            # 如果是 Class 嵌套 Class 的场景
            elif isinstance(property_value_type, ClassPrototype):
                # 在前面追加所依赖的类的定义
                result_code_str = cls._resolve_class_prototype_to_code(
                    property_prototype.property_value_type) + "\n" + result_code_str

                # 再把这个成员添加进去
                append_code_str = append_code_str + "        self.%s = %s()\n" % (
                    property_name, str(property_value_type.class_name)
                )

        return result_code_str + append_code_str


class TestObject2(object):
    def __init__(self):
        self.name = "111"
        self._name = "222"
        self.__name = "333"
        self.___name = "444"
        self.____name = "555"

        class T3():
            def __init__(self):
                self.name = "111"

        self.next_scc = T3()


if __name__ == '__main__':
    j = object_to_json(TestObject2())
    pro = PythonCodeGenerator.generate_python_struct_from_json(
        j
    )

    a = list()  # type: type([ClassPrototype()])

    print(pro)
