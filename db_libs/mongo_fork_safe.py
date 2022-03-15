import os
from multiprocessing import Process
from pymongo.collection import Collection
from pymongo import MongoClient

"""
此模块封装的pymongo是在linux上子进程安全的。使用方式见 db_libs/mongo_fork_safe.py
在win上无所谓都正常。
"""

pid__col_map = {}


def get_col(db: str, col: str, mongo_connect_url='mongodb://127.0.0.1') -> Collection:
    """封装一个函数，判断pid"""
    pid = os.getpid()
    key = (pid, mongo_connect_url, db, col)
    if key not in pid__col_map:
        pid__col_map[key] = MongoClient(mongo_connect_url).get_database(db).get_collection(col)
    return pid__col_map[key]
