# -*- coding: UTF-8 -*-
import os

import setuptools

setuptools.setup(
    # 项目名
    name='pava',
    # 版本号
    version='2023.1',
    # 关键词
    keywords='Java',
    # 简单描述
    description='A library with something like java',
    long_description=open(
        os.path.join(
            os.path.dirname(__file__),
            'README.rst'
        )
    ).read(),
    # 作者
    author='monetto',
    # 作者邮箱
    author_email='j3jzy@163.com',
    # 项目地址
    url='https://gitee.com/monetto/pava',
    # 开源协议
    license='GPL',
    packages=setuptools.find_packages()
)
