# coding=utf8
"""
@author:Administrator
@file: mongo_lib.py
@time: 2020/06
"""

import pymongo
import decorator_libs  # pip install decorator_libs


@decorator_libs.flyweight
class MongoClientFlyWeight(pymongo.MongoClient):
    def my_fun(self):
        print('扩展示例')
        self.database_names()


@decorator_libs.lru_cache()
def get_col(mongo_connect_url, database_name, col_name):
    """
    缓存更进一步，节省执行的代码行数。操作更快。
    :param mongo_connect_url:
    :param database_name:
    :param col_name:
    :return:
    """
    return MongoClientFlyWeight(mongo_connect_url).get_database(database_name).get_collection(col_name)


if __name__ == '__main__':
    """
    测试无限实例化。
    使不使用享元模式，时间差距很大，相隔30多倍。
    """
    # with decorator_libs.TimerContextManager():
    #     pass
    #     for i in range(1000):
    #         pymongo.MongoClient().get_database('testdb').get_collection('test_col').insert({"a": i})

    with decorator_libs.TimerContextManager():
        for i in range(100000):
            MongoClientFlyWeight().get_database('testdb').get_collection('test_col')#.insert({"a": i})

    with decorator_libs.TimerContextManager():
        for i in range(100000):
            get_col('mongodb://127.0.0.1', 'testdb', 'test_col')#.insert({"a": i})
