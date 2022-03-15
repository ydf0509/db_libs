from multiprocessing import Process
from pymongo import MongoClient

from db_libs.mongo_fork_safe import get_col


def col3():
    return get_col('testdb', 'col3')


client = MongoClient(host='127.0.0.1')  # 在linux子进程用父进程的mongo链接是错误方式，这是举例子。


def test_child_process_insert():
    # client.get_database('testdb').get_collection('testcol').insert_one({"a":1})   # 如果在子进程中运行这个函数，这样操作在linux + pymongo 4.xx报错，在3.xx会警告。
    # get_col('testdb','testcol2').insert_one({"a":1})  # 如果在子进程中运行这个函数，这样操作在linux win都完美运行。
    col3().insert_one({"b": 2})  # 如果在子进程中运行这个函数，这样操作在linux win都完美运行。


if __name__ == '__main__':
    [Process(target=test_child_process_insert).start() for i in range(3)]
