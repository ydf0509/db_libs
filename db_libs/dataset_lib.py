
import os
import dataset
import threading

pid__db_map = {}
_lock = threading.Lock()

def get_db(connect_url) -> dataset.Database:
    """封装一个函数，判断pid"""
    pid = os.getpid()
    key = (pid, connect_url,)
    if key not in pid__db_map:
        with _lock:
            if key not in pid__db_map:
                pid__db_map[key] =  dataset.connect(connect_url)
    return pid__db_map[key]


def get_table(connect_url, table_name) -> dataset.Table:
    """封装一个函数，判断pid"""
    db = get_db(connect_url)
    return db[table_name]