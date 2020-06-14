#coding=utf8
"""
@author:Administrator
@file: sqla_lib.py
@time: 2020/06
"""
import nb_log
import sqlalchemy
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, Query, scoped_session, sessionmaker

metadata = MetaData()
engine = create_engine(
    'mysql+pymysql://root:123456@127.0.0.1:3306/aj?charset=utf8',
    max_overflow=20,  # 超过连接池大小外最多创建的连接
    pool_size=30,  # 连接池大小
    pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
    pool_recycle=3600,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    echo = True)
# 反射数据库单表
Admin = Table('ihome_area', metadata, autoload=True, autoload_with=engine)
print(Admin.columns.keys())
# session = Session(engine)
session = scoped_session(sessionmaker(bind=engine))
print(type(Admin))

'''反射数据库所有的表'''
Base = automap_base()
Base.prepare(engine, reflect=True)
print(Base.classes)
print(Base.classes.keys())
Admin2 = Base.classes.ihome_area
print(type(Admin2))

res = session.query(Admin2).filter(Admin2.id==1).first() # type: Query                    ##type:  sqlalchemy.ext.declarative.api.DeclarativeMeta
print(res.__dict__)


print(res.name,res.create_time)

cursor = session.execute("""SELECT ihome_area.create_time AS ihome_area_create_time, ihome_area.update_time AS ihome_area_update_time, ihome_area.id AS ihome_area_id, ihome_area.name AS ihome_area_name 
FROM ihome_area 
 LIMIT 1""")
result = cursor.fetchall()
print(len(result))
"""

#  sqlalchemy  反射
 
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
 
uri = 'mysql+pymysql://root:sp@2016@10.1.1.1:3306/dev'
 
engine = create_engine(uri, echo=False)
 
metadata = MetaData(engine)
 
# ①：反射单个表
# apply_info = Table('apply_info', metadata, autoload=True)
# apply_info.columns.keys()  # 列出所有的列名
# *********************************************************************
# ②：反射整个数据库
# 使用reflect()方法，它不会返回任何值
# metadata.reflect(bind=engine)
 
# metadata.tables.keys()  # 获取所有的表名
# # 虽然反射了整个数据库，还是需要再添加一次具体的表反射.
# apply_info = metadata.tables['apply_info']
# *********************************************************************
 
# ③：基于ORM的反射
 
Base = automap_base()
# Base = declarative_base()
# apply_info = Base.metadata.tables['apply_info']
 
Base.prepare(engine, reflect=True)
Base.classes.keys()  # 获取所有的对象名
# 获取表对象
apply_info = Base.classes.apply_info
# *********************************************************************
 
 
Session = sessionmaker(bind=engine)
 
session = Session()
 
# 插入数据
# session.add(apply_info(email_address="foo@bar.com", name="foo"))
# session.commit()
 
# keys = apply_info.__table__.columns.keys()
#
# rows = session.query(apply_info)
#
# data = [getattr(rows, key) for key in keys]
 
# mysql 查询的结果，可以通过dot.号访问。而oracle 不可以。
————————————————
版权声明：本文为CSDN博主「Op小剑」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/xie_0723/java/article/details/84901502

"""


"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy import ForeignKey,Sequence,MetaData,Table
from sqlalchemy.orm import relationship,sessionmaker

engine=create_engine('mysql+pymysql://root:zcisno.1@localhost/blog?charset=utf8')
Base=declarative_base()

md = MetaData(bind=engine) #引用MetaData

class Test(Base):
    __table__ = Table("test", md, autoload=True)#自动加载表结构


Session=sessionmaker(bind=engine)
session=Session()

t1=Test(name='live1111')

session.add(t1)
session.commit()
"""

'''
from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy import make_url
from server import create_app


def get_mysql_conn_url(config):
    """
    :description: 生成sqlalchemy使用的连接url
    :param hy_config: hy_config
    :return: url
    """
    mysql_conn_map = dict(
        dialect="mysql",
        driver="pymysql",
        host=config["MYSQL_HOST"],
        port=config["MYSQL_PORT"],
        database=config["MYSQL_DB"],
        user=config["MYSQL_USER"],
        password=config["MYSQL_PASSWORD"],
    )
    s = "{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}?charset=utf8".format(**mysql_conn_map)
    # s = "{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}".format(**mysql_conn_map)
    url = make_url(s)
    return url

def create_mysql_ORM(app):
    """
    创建MySQL的ORM对象并反射数据库中已存在的表，获取所有存在的表对象
    :param app: app:flask实例
    :return: (db:orm-obj, all_table:数据库中所有已存在的表的对象(dict))
    """
    # 创建mysql连接对象
    url = get_mysql_conn_url(config=app.config)
    app.config["SQLALCHEMY_DATABASE_URI"] = url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True  # 每次请求结束时自动commit数据库修改
    app.config["SQLALCHEMY_ECHO"] = False   # 如果设置成 True，SQLAlchemy将会记录所有发到标准输出(stderr)的语句,这对调试很有帮助.
    app.config["SQLALCHEMY_RECORD_QUERIES"] = None  # 可以用于显式地禁用或者启用查询记录。查询记录 在调试或者测试模式下自动启用。
    app.config["SQLALCHEMY_POOL_SIZE"] = 5  # 数据库连接池的大小。默认是数据库引擎的默认值(通常是 5)。
    app.config["SQLALCHEMY_POOL_TIMEOUT"] = 10  # 指定数据库连接池的超时时间。默认是 10。
    """
    自动回收连接的秒数。
    这对 MySQL 是必须的，默认 情况下 MySQL 会自动移除闲置 8 小时或者以上的连接。 
    需要注意地是如果使用 MySQL 的话， Flask-SQLAlchemy 会自动地设置这个值为 2 小时。
    """
    app.config["SQLALCHEMY_POOL_RECYCLE"] = None
    """
    控制在连接池达到最大值后可以创建的连接数。
    当这些额外的 连接回收到连接池后将会被断开和抛弃。
    """
    app.config["SQLALCHEMY_MAX_OVERFLOW"] = None
    # 获取SQLAlchemy实例对象
    db = SQLAlchemy(app)

    # 反射数据库中已存在的表，并获取所有存在的表对象。
    db.reflect()
    all_table = {table_obj.name: table_obj for table_obj in db.get_tables_for_bind()}

    return db, all_table

app = create_app()
db, all_table = create_mysql_ORM(app=app)
————————————————
版权声明：本文为CSDN博主「四月的水」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/weixin_40238625/java/article/details/88177492
'''

# 连接池
'''

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from models import Student,Course,Student2Course

engine = create_engine(
        "mysql+pymysql://root:123456@127.0.0.1:3306/s9day120?charset=utf8",
        max_overflow=0,  # 超过连接池大小外最多创建的连接
        pool_size=5,  # 连接池大小
        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
        pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
    )
SessionFactory = sessionmaker(bind=engine)
session = scoped_session(SessionFactory)


def task():
    """"""
    # 方式一：
    """
    # 查询
    # cursor = session.execute('select * from users')
    # result = cursor.fetchall()

    # 添加
    cursor = session.execute('INSERT INTO users(name) VALUES(:value)', params={"value": 'wupeiqi'})
    session.commit()
    print(cursor.lastrowid)
    """
    # 方式二：
    """
    # conn = engine.raw_connection()
    # cursor = conn.cursor()
    # cursor.execute(
    #     "select * from t1"
    # )
    # result = cursor.fetchall()
    # cursor.close()
    # conn.close()
    """

    # 将连接交还给连接池
    session.remove()


from threading import Thread

for i in range(20):
    t = Thread(target=task)
    t.start()
'''