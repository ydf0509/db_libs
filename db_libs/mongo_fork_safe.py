import os
from multiprocessing import Process
from pymongo.collection import Collection
from pymongo import MongoClient

"""
此模块封装的pymongo是在linux上子进程安全的。使用方式见 db_libs/mongo_fork_safe.py
在win上无所谓都正常。

在linux上 MongoClient 实例化即使传参 connect=False，如果在父进程中已经操作了mongo，在子进程中仍然报错。
因为在父进程中只要操作了mongo，就会去连接mongo服务，connect=False就被破坏了。

只有下面这种每次操作mongo，都不使用Collection类型的全局变量/实例属性 ,每次都动态 get_col() ,每一次操作mongo前都判断pid的方式才进程安全
"""

pid__col_map = {}


def get_col(db: str, col: str, mongo_connect_url='mongodb://127.0.0.1') -> Collection:
    """封装一个函数，判断pid"""
    pid = os.getpid()
    key = (pid, mongo_connect_url, db, col)
    if key not in pid__col_map:
        pid__col_map[key] = MongoClient(mongo_connect_url,connect=False).get_database(db).get_collection(col)
    return pid__col_map[key]
