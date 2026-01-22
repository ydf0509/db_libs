# coding=utf-8
from setuptools import setup, find_packages

filepath = 'README.md'
print(filepath)

setup(
    name='db_libs',  #
    version="1.3",
    description=(
        "'redis', 'mongo', 'elasticsearch', 'mysql', 'sqlachemy', '线程安全的数据库封装，享元模式支持无限实例化调用'"
    ),
    keywords=["database", 'redis', 'mongo', 'elasticsearch', 'mysql', 'sqlachemy'],
    # long_description=open('README.md', 'r',encoding='utf8').read(),
    long_description_content_type="text/markdown",
    long_description=open(filepath, 'r', encoding='utf8').read(),
    # data_files=[filepath],
    author='bfzs',
    author_email='ydf0509@sohu.com',
    maintainer='ydf',
    maintainer_email='ydf0509@sohu.com',
    license='BSD License',
    packages=find_packages(),
    include_package_data=True,
    platforms=["all"],
    url='',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=['nb_log',
                      'decorator_libs',
                      'redis',
                      'pymongo',
                      'elasticsearch',
                      'threadpool_executor_shrink_able',
                      'redis2',
                      'redis3',
                      'redis5',
                      'pymysql',
                      # 'records',
                      'dbutils==3.1.0',
                      ]
)
"""
打包上传

打包上传（推荐方式）：
python setup.py sdist & twine upload dist/db_libs-1.3.tar.gz

或者使用 build 模块（更现代的方式）：
pip install build twine
python -m build
twine upload dist/*

安装：
python -m pip install db_libs --upgrade -i https://pypi.org/simple
"""

