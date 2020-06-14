# coding=utf8
"""
@author:Administrator
@file: sqla_lib.py
@time: 2020/06
"""
from datetime import datetime
import decorator_libs
import nb_log

import sqlalchemy
from pymysql import PY2
from pymysql.cursors import Cursor
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.scoping import ScopedSession
from threadpool_executor_shrink_able import BoundedThreadPoolExecutor

logger_show_pymysql_execute_sql = nb_log.LogManager('logger_show_pymysql_execute_sql').get_logger_and_add_handlers(log_filename='logger_show_pymysql_execute_sql.log')


def _my_mogrify(self, query, args=None):
    """
    Returns the exact string that is sent to the database by calling the
    execute() method.

    This method follows the extension to the DB API 2.0 followed by Psycopg.
    """
    conn = self._get_db()
    if PY2:  # Use bytes on Python 2 always
        query = self._ensure_bytes(query, encoding=conn.encoding)

    if args is not None:
        query = query % self._escape_args(args, conn)
    logger_show_pymysql_execute_sql.debug(query)
    return query


Cursor.mogrify = _my_mogrify


class _SessionContext(Session):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()
        return False


class SqlaReflectHelper(nb_log.LoggerMixin):
    """
    反射数据库中已存在的表
    """
    session_factory_of_scoped = None

    def __init__(self, sqla_engine: Engine):
        nb_log.LogManager('sqlalchemy.engine.base.Engine').remove_all_handlers()
        if sqla_engine.echo:
            # 将日志自动记录到硬盘根目录的/pythonlogs/sqla_execute.log
            nb_log.LogManager('sqlalchemy.engine.base.Engine').get_logger_and_add_handlers(10, log_filename='sqla_execute.log')
        else:
            nb_log.LogManager('sqlalchemy.engine.base.Engine').get_logger_and_add_handlers(30, log_filename='sqla_execute.log')
        self.engine = sqla_engine
        Base = automap_base()
        Base.prepare(self.engine, reflect=True)
        self.base_classes = Base.classes
        self.base_classes_keys = Base.classes.keys()
        self.logger.debug(self.base_classes_keys)

        self.show_tables_and_columns()

    def show_tables_and_columns(self):
        for table_name in self.base_classes_keys:
            self.logger.debug(table_name)
            model = getattr(self.base_classes, table_name)
            self.logger.debug(model.__table__.columns.keys())

    def get_session_factory(self):
        return sessionmaker(bind=self.engine)

    def get_session_factory_of_scoped(self) -> ScopedSession:
        session_factory = sessionmaker(bind=self.engine, class_=_SessionContext)  # 改成了自定义的Session类
        return ScopedSession(session_factory)

    @property
    def session(self):
        if not self.__class__.session_factory_of_scoped:
            self.__class__.session_factory_of_scoped = self.get_session_factory_of_scoped()
        return self.__class__.session_factory_of_scoped()


if __name__ == '__main__':
    """
    例如 ihome_area2的表结果如下。
    
    create table ihome_area2
(
    create_time datetime    null,
    update_time datetime    null,
    id          int auto_increment
        primary key,
    name        varchar(32) not null
);

    """

    enginex = create_engine(
        'mysql+pymysql://root:123456@127.0.0.1:3306/aj?charset=utf8',
        max_overflow=10,  # 超过连接池大小外最多创建的连接
        pool_size=50,  # 连接池大小
        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle=3600,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
        echo=True)
    sqla_helper = SqlaReflectHelper(enginex)
    Ihome_area2 = sqla_helper.base_classes.ihome_area2  # ihome_area2是表名。


    def f1():
        with sqla_helper.session as ss:
            ss  # type: _SessionContext

            print(ss)

            print(ss.query(sqlalchemy.func.count(Ihome_area2.id)).scalar())

            # 使用orm方式插入
            ss.add(Ihome_area2(create_time=datetime.now(), update_time=datetime.now(), name='testname'))

            print(ss.query(sqlalchemy.func.count(Ihome_area2.id)).scalar())

            # 使用占位符语法插入，此种可以防止sql注入
            ss.execute(f'''INSERT INTO ihome_area2 (create_time, update_time, name) VALUES (:v1,:v2,:v3)''', params={'v1': '2020-06-14 19:15:14', 'v2': '2020-06-14 19:15:14', 'v3': 'testname00'})

            # 直接自己拼接完整字符串，不使用三方包占位符的后面的参数，此种会引起sql注入，不推荐。
            cur = ss.execute(f'''INSERT INTO ihome_area2 (create_time, update_time, name) VALUES ('2020-06-14 19:15:14','2020-06-14 19:15:14', 'testname')''', )

            # 这样也可以打印执行的语句
            # noinspection PyProtectedMember
            print(cur._saved_cursor._executed)


    def f2():
        ss = sqla_helper.get_session_factory()()
        print(ss)
        print(ss.query(sqlalchemy.func.count(sqla_helper.base_classes.ihome_area.id)).scalar())
        ss.add(sqla_helper.base_classes.ihome_area(create_time=datetime.now(), update_time=datetime.now(), name='testname'))
        ss.commit()
        print(ss.query(sqlalchemy.func.count(sqla_helper.base_classes.ihome_area.id)).scalar())
        ss.close()


    with decorator_libs.TimerContextManager():
        t_pool = BoundedThreadPoolExecutor(10)  # 封装mysql，切记一定要测试多线程下的情况。
        for _ in range(500):
            # f1()
            t_pool.submit(f1)
        t_pool.shutdown()
