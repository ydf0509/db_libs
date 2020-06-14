# coding=utf8
"""
@author:Administrator
@file: mysql_lib.py
@time: 2020/06
"""
import datetime
import nb_log
import pymysql
from DBUtils.PooledDB import PooledDB  # 1.3版本
import decorator_libs

class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class ObjectCusor(pymysql.cursors.DictCursor, nb_log.LoggerMixin, nb_log.LoggerLevelSetterMixin):
    """
    比字典式的cursor，返回结果除了能用 ["xx"]来获取字段的值以外，还可以使用 .xx的方式获取字段的值。
    """
    dict_type = Row

    def mogrify(self, query, args=None):
        query_str = super().mogrify(query, args)
        self.logger.debug(query_str)
        return query_str


class CursorContext:
    def __init__(self, conn_pool: PooledDB, is_show_execute_sql=False):
        """
        :param conn_pool: 连接池
        :param is_show_execute_sql: 是否打印执行的语句
        """
        self.conn = conn_pool.connection()  # type: pymysql.Connection
        self.cursor = self.conn.cursor(ObjectCusor)  # type: ObjectCusor                #pymysql.cursors.Cursor
        if not is_show_execute_sql:
            self.cursor.set_log_level(20)
        else:
            self.cursor.set_log_level(10)

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()
        return False


if __name__ == '__main__':
    # pymysql.connections.Connection
    pool = PooledDB(
        creator=pymysql,  # 使用链接数据库的模块
        maxconnections=50,  # 连接池允许的最大连接数，0和None表示不限制连接数
        mincached=5,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
        maxcached=10,  # 链接池中最多闲置的链接，0和None不限制
        maxshared=0,  # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
        blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
        maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
        setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
        ping=0,
        # ping MySQL服务端，检查是否服务可用。
        # 如：0 = None = never,
        # 1 = default = whenever it is requested,
        # 2 = when a cursor is created,
        # 4 = when a query is executed,
        # 7 = always
        host='127.0.0.1',
        port=3306,
        user='root',
        password='123456',
        database='sqlachemy_queues',
        charset='utf8',
        # cursorclass=pymysql.cursors.DictCursor, # 固定使用自定义的 ObjectCusor，包含了DictCursor的所有功能。
    )

    """
    conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
    cur = conn.cursor()
    sql = "select * from queue_test58 limit 10"
    r = cur.execute(sql)
    r = cur.fetchall()
    print(r)
    cur.close()
    conn.close()
    """
    # nb_log.LogManager().get_logger_and_add_handlers()
    with CursorContext(pool, is_show_execute_sql=True) as cursor:
        cursor.execute("select * from sqlachemy_queues.queue_test58 limit 3")
        for row in cursor.fetchall():
            print(row['status'])  # 两种方式都可以获取表中的status字段的值。
            print(row.status)
        cursor.execute('INSERT INTO sqlachemy_queues.queue_test58(body,publish_timestamp,status) VALUES (%s,%s, %s)',
                       args=('bodytest', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'teststatus'))
        print(cursor.rowcount, cursor.lastrowid)

    with CursorContext(pool) as cursor:
        cursor.executemany('INSERT INTO sqlachemy_queues.queue_test58(body,publish_timestamp,status) VALUES (%s,%s, %s)',
                           args=[('bodytest', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'teststatus')] * 10)
        print(cursor.rowcount, cursor.lastrowid)


    def test_threads():
        """测试多线程"""
        with CursorContext(pool, is_show_execute_sql=True) as cursor:
            cursor.execute('INSERT INTO sqlachemy_queues.queue_test58(body,publish_timestamp,status) VALUES (%s,%s, %s)',
                           args=('bodytest', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'teststatus'))


    from threadpool_executor_shrink_able import BoundedThreadPoolExecutor

    thread_pool = BoundedThreadPoolExecutor(10)
    with decorator_libs.TimerContextManager():
        for _ in range(3000):
            thread_pool.submit(test_threads)
        thread_pool.shutdown()
