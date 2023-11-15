# coding=utf-8
from functools import wraps

from pava.decorator.base_decorator import BaseDecorator
from pava.utils.object_utils import *

"""
用于 SQLAlchemy 返回的 Model 实体对象
将对象精简到业务只想要的属性 减少性能成本
需要 Model 实体存在 Entry Class (__data_entry__)
参与转换: Object, List, Dict(Only-Value)
"""

MODEL_AND_ENTRY = dict()
ENTRY_AND_DICT = dict()


class ETLModel(BaseDecorator):
    @classmethod
    def execute(cls, execute_result):
        if execute_result is None:
            return execute_result

        execute_result_type = type(execute_result)

        data_entry_prototype = MODEL_AND_ENTRY.get(execute_result_type, None)
        if data_entry_prototype is None:
            data_entry_prototype = getattr(execute_result, "__data_entry__")
            MODEL_AND_ENTRY[execute_result_type] = data_entry_prototype

        data_entry = data_entry_prototype()

        data_entry_dict = ENTRY_AND_DICT.get(data_entry_prototype, None)
        if data_entry_dict is None:
            data_entry_dict = object_to_dict(data_entry)
            ENTRY_AND_DICT[data_entry_prototype] = data_entry_dict

        for key in data_entry_dict:
            try:
                value = getattr(execute_result, key)
                setattr(data_entry, key, trans_object_properties_to_utf8(value))
            except Exception as e:
                pass
        return data_entry


def etl_model(function):
    @wraps(function)
    def extract_transform_load(*args, **kwargs):
        execute_result = function(*args, **kwargs)
        execute_result_type = type(execute_result)
        if execute_result_type == list:
            result = [ETLModel.execute(element) for element in execute_result]
            return result
        elif execute_result_type == dict:
            result = dict()
            for key, value in execute_result.items():
                result[trans_object_properties_to_utf8(key)] = ETLModel.execute(value)
            return result
        else:
            return ETLModel.execute(execute_result)

    return extract_transform_load
