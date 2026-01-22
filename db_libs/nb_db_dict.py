# coding=utf8
"""
@author: nb_db_dict
@file: nb_db_dict.py
@desc: 类似 dataset 包的功能，支持 SQLAlchemy 2.0
       直接保存字典到数据库表，无需建表，无需写 insert 语句
"""
import os
import threading
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Union, Any, Iterator

from sqlalchemy import (
    create_engine, MetaData, Table, Column, inspect,
    Integer, BigInteger, String, Text, Float, Boolean, DateTime, Date,
    JSON, Numeric, LargeBinary,
    text, insert, update, delete, select, and_
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import CreateTable


# 存储进程级别的 Database 实例，实现享元模式
_pid_db_map: Dict[tuple, 'Database'] = {}
_lock = threading.Lock()


def connect(url: str, **kwargs) -> 'Database':
    """
    连接数据库，返回 Database 实例
    使用享元模式，相同的 url 返回同一个 Database 实例（按进程区分）
    
    :param url: SQLAlchemy 连接 URL，如 'mysql+pymysql://user:pass@host/db?charset=utf8mb4'
                或 'sqlite:///mydb.db' 或 'postgresql://user:pass@host/db'
    :param kwargs: 传递给 create_engine 的额外参数
    :return: Database 实例
    """
    pid = os.getpid()
    key = (pid, url)
    if key not in _pid_db_map:
        with _lock:
            if key not in _pid_db_map:
                _pid_db_map[key] = Database(url, **kwargs)
    return _pid_db_map[key]


class Database:
    """
    数据库连接类，类似 dataset.Database
    """
    
    def __init__(self, url: str, **engine_kwargs):
        """
        初始化数据库连接
        
        :param url: SQLAlchemy 连接 URL
        :param engine_kwargs: 传递给 create_engine 的额外参数
        """
        # 设置一些默认参数
        default_kwargs = {
            'pool_pre_ping': True,  # 连接前 ping，避免连接失效
            'pool_recycle': 3600,   # 1小时后回收连接
        }
        default_kwargs.update(engine_kwargs)
        
        self._engine: Engine = create_engine(url, **default_kwargs)
        self._metadata: MetaData = MetaData()
        self._tables: Dict[str, 'DbTable'] = {}
        self._lock = threading.Lock()
        
    @property
    def engine(self) -> Engine:
        """返回 SQLAlchemy Engine"""
        return self._engine
    
    @property
    def metadata(self) -> MetaData:
        """返回 MetaData"""
        return self._metadata
    
    @property
    def tables(self) -> List[str]:
        """返回数据库中所有表名"""
        inspector = inspect(self._engine)
        return inspector.get_table_names()
    
    def __getitem__(self, table_name: str) -> 'DbTable':
        """
        通过 db['table_name'] 的方式获取表对象
        如果表不存在，会在第一次插入数据时自动创建
        """
        if table_name not in self._tables:
            with self._lock:
                if table_name not in self._tables:
                    self._tables[table_name] = DbTable(self, table_name)
        return self._tables[table_name]
    
    def __contains__(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables
    
    def get_table(self, table_name: str) -> 'DbTable':
        """获取表对象的另一种方式"""
        return self[table_name]
    
    def create_table(self, table_name: str, primary_id: str = 'id', 
                     primary_type: str = 'Integer', primary_increment: bool = True) -> 'DbTable':
        """
        创建表（如果不存在）
        
        :param table_name: 表名
        :param primary_id: 主键列名，默认 'id'
        :param primary_type: 主键类型，'Integer' 或 'BigInteger' 或 'String'
        :param primary_increment: 是否自增
        :return: DbTable 实例
        """
        table = self[table_name]
        table._ensure_table_exists(primary_id, primary_type, primary_increment)
        return table
    
    def drop_table(self, table_name: str):
        """删除表"""
        if table_name in self.tables:
            table = Table(table_name, self._metadata, autoload_with=self._engine)
            table.drop(self._engine)
            if table_name in self._tables:
                del self._tables[table_name]
    
    def query(self, sql: str, **params) -> List[Dict]:
        """
        执行原生 SQL 查询
        
        :param sql: SQL 语句
        :param params: 参数
        :return: 结果列表
        """
        with self._engine.connect() as conn:
            result = conn.execute(text(sql), params)
            if result.returns_rows:
                return [dict(row._mapping) for row in result]
            conn.commit()
            return []
    
    def execute(self, sql: str, **params):
        """
        执行原生 SQL（增删改）
        
        :param sql: SQL 语句
        :param params: 参数
        """
        with self._engine.connect() as conn:
            conn.execute(text(sql), params)
            conn.commit()
    
    def close(self):
        """关闭数据库连接"""
        self._engine.dispose()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class DbTable:
    """
    数据库表类，类似 dataset.Table
    支持直接保存字典，自动创建/更新表结构
    """
    
    # Python 类型到 SQLAlchemy 类型的映射
    TYPE_MAP = {
        int: BigInteger,
        float: Float,
        bool: Boolean,
        str: Text,  # 默认用 Text，避免长度限制问题
        bytes: LargeBinary,
        datetime: DateTime,
        date: Date,
        Decimal: Numeric(precision=20, scale=6),
        dict: JSON,
        list: JSON,
        type(None): Text,
    }
    
    def __init__(self, db: Database, table_name: str):
        self._db = db
        self._table_name = table_name
        self._table: Optional[Table] = None
        self._columns: Dict[str, Column] = {}
        self._lock = threading.Lock()
        self._primary_id = 'id'
        self._load_table_if_exists()
    
    @property
    def table(self) -> Optional[Table]:
        """返回 SQLAlchemy Table 对象"""
        return self._table
    
    @property
    def columns(self) -> List[str]:
        """返回表的所有列名"""
        if self._table is not None:
            return [c.name for c in self._table.columns]
        return []
    
    @property
    def name(self) -> str:
        """返回表名"""
        return self._table_name
    
    def _load_table_if_exists(self):
        """如果表存在，加载表结构"""
        try:
            inspector = inspect(self._db.engine)
            if self._table_name in inspector.get_table_names():
                self._table = Table(
                    self._table_name, 
                    self._db.metadata, 
                    autoload_with=self._db.engine,
                    extend_existing=True
                )
                for col in self._table.columns:
                    self._columns[col.name] = col
                    if col.primary_key:
                        self._primary_id = col.name
        except NoSuchTableError:
            pass
    
    def _ensure_table_exists(self, primary_id: str = 'id', 
                             primary_type: str = 'Integer',
                             primary_increment: bool = True):
        """确保表存在，如果不存在则创建"""
        if self._table is not None:
            return
        
        with self._lock:
            if self._table is not None:
                return
            
            self._primary_id = primary_id
            self._primary_increment = primary_increment
            
            # 确定主键类型
            # 注意：SQLite 只有 INTEGER 类型才支持自增
            dialect = self._db.engine.dialect.name
            if primary_type == 'BigInteger':
                if dialect == 'sqlite':
                    # SQLite 中使用 Integer 才能自增
                    pk_type = Integer if primary_increment else BigInteger
                else:
                    pk_type = BigInteger
            elif primary_type == 'String':
                pk_type = String(255)
                primary_increment = False
            else:
                pk_type = Integer
            
            # 创建表
            columns = [
                Column(primary_id, pk_type, primary_key=True, autoincrement=primary_increment)
            ]
            
            self._table = Table(
                self._table_name,
                self._db.metadata,
                *columns,
                extend_existing=True
            )
            self._table.create(self._db.engine, checkfirst=True)
            
            for col in self._table.columns:
                self._columns[col.name] = col
    
    def _infer_column_type(self, value: Any) -> type:
        """根据值推断 SQLAlchemy 列类型"""
        if value is None:
            return Text
        
        # 对于字符串，根据长度选择类型
        if isinstance(value, str):
            if len(value) <= 255:
                return String(255)
            elif len(value) <= 65535:
                return Text
            else:
                return Text  # 大文本
        
        # 对于整数，根据大小选择类型
        if isinstance(value, int) and not isinstance(value, bool):
            if -2147483648 <= value <= 2147483647:
                return Integer
            else:
                return BigInteger
        
        return self.TYPE_MAP.get(type(value), Text)
    
    def _add_column(self, col_name: str, col_type):
        """添加新列到表中"""
        if col_name in self._columns:
            return
        
        with self._lock:
            if col_name in self._columns:
                return
            
            # 使用 ALTER TABLE 添加列
            # 获取类型的 SQL 表示
            if isinstance(col_type, type):
                col_type = col_type()
            
            type_str = col_type.compile(dialect=self._db.engine.dialect)
            
            sql = f'ALTER TABLE {self._table_name} ADD COLUMN {col_name} {type_str}'
            
            with self._db.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            
            # 重新加载表结构
            self._table = Table(
                self._table_name,
                self._db.metadata,
                autoload_with=self._db.engine,
                extend_existing=True
            )
            for col in self._table.columns:
                self._columns[col.name] = col
    
    def _ensure_columns(self, data: Dict):
        """确保表中存在数据所需的所有列"""
        for key, value in data.items():
            if key not in self._columns:
                col_type = self._infer_column_type(value)
                self._add_column(key, col_type)
    
    def insert(self, data: Dict, ensure: bool = True) -> Optional[int]:
        """
        插入一条记录
        
        :param data: 要插入的字典数据
        :param ensure: 是否确保列存在（自动添加缺失的列）
        :return: 插入记录的 ID（如果有自增主键）
        """
        if not data:
            return None
        
        # 确保表存在
        if self._table is None:
            self._ensure_table_exists()
        
        # 确保列存在
        if ensure:
            self._ensure_columns(data)
        
        # 过滤掉不存在的列
        filtered_data = {k: v for k, v in data.items() if k in self._columns}
        
        with self._db.engine.connect() as conn:
            stmt = insert(self._table).values(**filtered_data)
            result = conn.execute(stmt)
            conn.commit()
            
            # 尝试获取插入的 ID
            try:
                return result.inserted_primary_key[0]
            except (IndexError, TypeError, AttributeError):
                return None
    
    def insert_many(self, rows: List[Dict], ensure: bool = True) -> int:
        """
        批量插入多条记录
        
        :param rows: 字典列表
        :param ensure: 是否确保列存在
        :return: 插入的记录数
        """
        if not rows:
            return 0
        
        # 确保表存在
        if self._table is None:
            self._ensure_table_exists()
        
        # 收集所有可能的列
        if ensure:
            all_keys = set()
            for row in rows:
                all_keys.update(row.keys())
            
            sample_data = {}
            for row in rows:
                for key in all_keys:
                    if key in row and row[key] is not None and key not in sample_data:
                        sample_data[key] = row[key]
            
            self._ensure_columns(sample_data)
        
        # 过滤数据
        filtered_rows = []
        for row in rows:
            filtered_row = {k: v for k, v in row.items() if k in self._columns}
            if filtered_row:
                filtered_rows.append(filtered_row)
        
        if not filtered_rows:
            return 0
        
        with self._db.engine.connect() as conn:
            conn.execute(insert(self._table), filtered_rows)
            conn.commit()
        
        return len(filtered_rows)
    
    def upsert(self, data: Dict, keys: List[str], ensure: bool = True) -> bool:
        """
        插入或更新记录（如果 keys 指定的列值已存在，则更新）
        
        :param data: 要插入/更新的字典数据
        :param keys: 用于查找现有记录的列名列表
        :param ensure: 是否确保列存在
        :return: 是否成功
        """
        if not data or not keys:
            return False
        
        # 确保表存在
        if self._table is None:
            self._ensure_table_exists()
        
        # 确保列存在
        if ensure:
            self._ensure_columns(data)
        
        # 构建查询条件
        conditions = []
        for key in keys:
            if key in data and key in self._columns:
                col = self._table.c[key]
                conditions.append(col == data[key])
        
        if not conditions:
            return self.insert(data, ensure=False) is not None
        
        # 检查记录是否存在
        with self._db.engine.connect() as conn:
            stmt = select(self._table).where(and_(*conditions))
            result = conn.execute(stmt)
            existing = result.fetchone()
            
            if existing:
                # 更新
                update_data = {k: v for k, v in data.items() 
                              if k in self._columns and k not in keys}
                if update_data:
                    stmt = update(self._table).where(and_(*conditions)).values(**update_data)
                    conn.execute(stmt)
                    conn.commit()
                return True
            else:
                # 插入
                filtered_data = {k: v for k, v in data.items() if k in self._columns}
                stmt = insert(self._table).values(**filtered_data)
                conn.execute(stmt)
                conn.commit()
                return True
    
    def update(self, data: Dict, keys: List[str], ensure: bool = True) -> int:
        """
        更新记录
        
        :param data: 要更新的数据（包含查询条件和要更新的值）
        :param keys: 用于定位记录的列名列表
        :param ensure: 是否确保列存在
        :return: 更新的记录数
        """
        if not data or not keys:
            return 0
        
        if self._table is None:
            return 0
        
        if ensure:
            self._ensure_columns(data)
        
        # 构建查询条件
        conditions = []
        for key in keys:
            if key in data and key in self._columns:
                col = self._table.c[key]
                conditions.append(col == data[key])
        
        if not conditions:
            return 0
        
        # 更新数据
        update_data = {k: v for k, v in data.items() 
                      if k in self._columns and k not in keys}
        
        if not update_data:
            return 0
        
        with self._db.engine.connect() as conn:
            stmt = update(self._table).where(and_(*conditions)).values(**update_data)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount
    
    def delete(self, **kwargs) -> int:
        """
        删除记录
        
        :param kwargs: 查询条件
        :return: 删除的记录数
        """
        if self._table is None:
            return 0
        
        conditions = []
        for key, value in kwargs.items():
            if key in self._columns:
                col = self._table.c[key]
                conditions.append(col == value)
        
        with self._db.engine.connect() as conn:
            if conditions:
                stmt = delete(self._table).where(and_(*conditions))
            else:
                stmt = delete(self._table)
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount
    
    def find(self, _limit: Optional[int] = None, _offset: Optional[int] = None,
             _order_by: Optional[Union[str, List[str]]] = None,
             **kwargs) -> List[Dict]:
        """
        查询记录
        
        :param _limit: 限制返回记录数
        :param _offset: 偏移量
        :param _order_by: 排序字段，可以是字符串或列表，前缀 '-' 表示降序
        :param kwargs: 查询条件
        :return: 查询结果列表
        """
        if self._table is None:
            return []
        
        stmt = select(self._table)
        
        # 添加查询条件
        conditions = []
        for key, value in kwargs.items():
            if key in self._columns:
                col = self._table.c[key]
                conditions.append(col == value)
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        # 排序
        if _order_by:
            if isinstance(_order_by, str):
                _order_by = [_order_by]
            
            for order_col in _order_by:
                if order_col.startswith('-'):
                    col_name = order_col[1:]
                    if col_name in self._columns:
                        stmt = stmt.order_by(self._table.c[col_name].desc())
                else:
                    if order_col in self._columns:
                        stmt = stmt.order_by(self._table.c[order_col])
        
        # 分页
        if _limit is not None:
            stmt = stmt.limit(_limit)
        if _offset is not None:
            stmt = stmt.offset(_offset)
        
        with self._db.engine.connect() as conn:
            result = conn.execute(stmt)
            return [dict(row._mapping) for row in result]
    
    def find_one(self, **kwargs) -> Optional[Dict]:
        """
        查询单条记录
        
        :param kwargs: 查询条件
        :return: 查询结果字典或 None
        """
        results = self.find(_limit=1, **kwargs)
        return results[0] if results else None
    
    def all(self) -> List[Dict]:
        """返回表中所有记录"""
        return self.find()
    
    def count(self, **kwargs) -> int:
        """
        统计记录数
        
        :param kwargs: 查询条件
        :return: 记录数
        """
        if self._table is None:
            return 0
        
        from sqlalchemy import func
        
        stmt = select(func.count()).select_from(self._table)
        
        conditions = []
        for key, value in kwargs.items():
            if key in self._columns:
                col = self._table.c[key]
                conditions.append(col == value)
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        with self._db.engine.connect() as conn:
            result = conn.execute(stmt)
            return result.scalar() or 0
    
    def distinct(self, column: str, **kwargs) -> List[Any]:
        """
        获取某列的不重复值
        
        :param column: 列名
        :param kwargs: 查询条件
        :return: 不重复值列表
        """
        if self._table is None or column not in self._columns:
            return []
        
        from sqlalchemy import distinct as sql_distinct
        
        stmt = select(sql_distinct(self._table.c[column]))
        
        conditions = []
        for key, value in kwargs.items():
            if key in self._columns:
                col = self._table.c[key]
                conditions.append(col == value)
        
        if conditions:
            stmt = stmt.where(and_(*conditions))
        
        with self._db.engine.connect() as conn:
            result = conn.execute(stmt)
            return [row[0] for row in result]
    
    def __iter__(self) -> Iterator[Dict]:
        """迭代表中所有记录"""
        return iter(self.all())
    
    def __len__(self) -> int:
        """返回记录数"""
        return self.count()
    
    def __repr__(self) -> str:
        return f"<DbTable({self._table_name})>"
    
    def get_create_table_sql(self) -> str:
        """
        获取建表语句
        
        :return: CREATE TABLE SQL 语句
        """
        if self._table is None:
            return ""
        
        create_stmt = CreateTable(self._table)
        return str(create_stmt.compile(dialect=self._db.engine.dialect))
    
    def print_create_table_sql(self):
        """打印建表语句"""
        sql = self.get_create_table_sql()
        if sql:
            print(f"\n{'='*50}")
            print(f"表 {self._table_name} 的建表语句:")
            print('='*50)
            print(sql)
            print('='*50)
        else:
            print(f"表 {self._table_name} 尚未创建")


# 便捷函数
def get_db(connect_url: str, **kwargs) -> Database:
    """获取数据库连接，是 connect 的别名"""
    return connect(connect_url, **kwargs)


def get_table(connect_url: str, table_name: str, **kwargs) -> DbTable:
    """获取表对象"""
    db = connect(connect_url, **kwargs)
    return db[table_name]


if __name__ == '__main__':
    # 示例用法
    # SQLite 示例
    db = connect('sqlite:///test.db')
    
    # 获取表（如果不存在，插入数据时会自动创建）
    users = db['users']
    
    # 插入数据 - 自动创建表和列
    user_id = users.insert({
        'name': '张三',
        'age': 25,
        'email': 'zhangsan@example.com',
        'created_at': datetime.now()
    })
    print(f'插入用户 ID: {user_id}')
    
    # 批量插入
    users.insert_many([
        {'name': '李四', 'age': 30, 'email': 'lisi@example.com'},
        {'name': '王五', 'age': 35, 'email': 'wangwu@example.com'},
    ])
    
    # 查询
    all_users = users.find()
    print(f'所有用户: {all_users}')
    
    # 条件查询
    young_users = users.find(age=25)
    print(f'25岁的用户: {young_users}')
    
    # 查询单条
    user = users.find_one(name='张三')
    print(f'张三: {user}')
    
    # 更新
    users.update({'name': '张三', 'age': 26}, keys=['name'])
    
    # upsert（存在则更新，不存在则插入）
    users.upsert({'name': '赵六', 'age': 40, 'email': 'zhaoliu@example.com'}, keys=['name'])
    
    # 统计
    count = users.count()
    print(f'用户总数: {count}')
    
    # 获取不重复值
    ages = users.distinct('age')
    print(f'所有年龄: {ages}')
    
    # 分页查询
    page1 = users.find(_limit=2, _offset=0, _order_by='-age')
    print(f'第一页（按年龄降序）: {page1}')
    
    # 删除
    deleted = users.delete(name='赵六')
    print(f'删除记录数: {deleted}')
    
    # 执行原生 SQL
    results = db.query('SELECT * FROM users WHERE age > :min_age', min_age=20)
    print(f'年龄大于20的用户: {results}')
