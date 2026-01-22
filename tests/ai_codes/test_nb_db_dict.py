# coding=utf8
"""
测试 nb_db_dict 模块
这是一个类似 dataset 的功能，支持 SQLAlchemy 2.0
"""
import os
import sys
import tempfile
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db_libs.nb_db_dict import connect, get_db, get_table, Database, DbTable


def test_basic_operations():
    """测试基本的 CRUD 操作"""
    print("\n" + "=" * 50)
    print("测试基本 CRUD 操作")
    print("=" * 50)
    
    # 使用临时 SQLite 数据库
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = None
    try:
        db_url = f'sqlite:///{db_path}'
        db = connect(db_url)
        
        # 1. 测试获取表
        print("\n1. 测试获取表...")
        users = db['users']
        print(f"   表对象: {users}")
        
        # 2. 测试插入 - 自动创建表和列
        print("\n2. 测试插入数据（自动创建表和列）...")
        user_id = users.insert({
            'name': '张三',
            'age': 25,
            'email': 'zhangsan@example.com',
            'created_at': datetime.now(),
            'long_str':'很长的字符串'*60
        })
        print(f"   插入用户 ID: {user_id}")
        
        # 验证表和列已创建
        print(f"   表中的列: {users.columns}")
        
        # 打印建表语句
        users.print_create_table_sql()
        
        # 3. 测试批量插入
        print("\n3. 测试批量插入...")
        count = users.insert_many([
            {'name': '李四', 'age': 30, 'email': 'lisi@example.com', 'city': '北京'},
            {'name': '王五', 'age': 35, 'email': 'wangwu@example.com', 'city': '上海'},
            {'name': '赵六', 'age': 28, 'email': 'zhaoliu@example.com', 'city': '广州'},
        ])
        print(f"   批量插入 {count} 条记录")
        
        # 4. 测试查询所有
        print("\n4. 测试查询所有记录...")
        all_users = users.find()
        print(f"   所有用户 ({len(all_users)} 条):")
        for u in all_users:
            print(f"      {u}")
        
        # 5. 测试条件查询
        print("\n5. 测试条件查询...")
        young_users = users.find(age=25)
        print(f"   25岁的用户: {young_users}")
        
        # 6. 测试查询单条
        print("\n6. 测试查询单条记录...")
        user = users.find_one(name='张三')
        print(f"   张三: {user}")
        
        # 7. 测试更新
        print("\n7. 测试更新...")
        updated = users.update({'name': '张三', 'age': 26, 'city': '深圳'}, keys=['name'])
        print(f"   更新 {updated} 条记录")
        user = users.find_one(name='张三')
        print(f"   更新后的张三: {user}")
        
        # 8. 测试 upsert
        print("\n8. 测试 upsert（存在则更新，不存在则插入）...")
        # 更新已存在的
        users.upsert({'name': '李四', 'age': 31, 'city': '杭州'}, keys=['name'])
        user = users.find_one(name='李四')
        print(f"   更新后的李四: {user}")
        
        # 插入不存在的
        users.upsert({'name': '钱七', 'age': 45, 'email': 'qianqi@example.com'}, keys=['name'])
        user = users.find_one(name='钱七')
        print(f"   新插入的钱七: {user}")
        
        # 9. 测试统计
        print("\n9. 测试统计...")
        total = users.count()
        print(f"   用户总数: {total}")
        age_30_count = users.count(age=31)
        print(f"   31岁的用户数: {age_30_count}")
        
        # 10. 测试获取不重复值
        print("\n10. 测试获取不重复值...")
        cities = users.distinct('city')
        print(f"   所有城市: {cities}")
        
        # 11. 测试分页和排序
        print("\n11. 测试分页和排序...")
        page1 = users.find(_limit=2, _offset=0, _order_by='-age')
        print(f"   第一页（按年龄降序，每页2条）: ")
        for u in page1:
            print(f"      {u.get('name')}: {u.get('age')}岁")
        
        page2 = users.find(_limit=2, _offset=2, _order_by='-age')
        print(f"   第二页: ")
        for u in page2:
            print(f"      {u.get('name')}: {u.get('age')}岁")
        
        # 12. 测试删除
        print("\n12. 测试删除...")
        deleted = users.delete(name='钱七')
        print(f"   删除 {deleted} 条记录")
        total = users.count()
        print(f"   删除后用户总数: {total}")
        
        # 13. 测试原生 SQL
        print("\n13. 测试原生 SQL 查询...")
        results = db.query('SELECT name, age FROM users WHERE age > :min_age', min_age=26)
        print(f"   年龄大于26的用户: {results}")
        
        # 14. 测试迭代
        print("\n14. 测试迭代...")
        print(f"   迭代所有用户:")
        for u in users:
            print(f"      - {u.get('name')}")
        
        print(f"   表长度: {len(users)}")
        
        print("\n✅ 所有基本操作测试通过！")
        
    finally:
        # 关闭数据库连接
        if db is not None:
            db.close()
        # 清理临时文件
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except PermissionError:
                pass  # Windows 上可能仍有锁，忽略


def test_auto_column_type():
    """测试自动推断列类型"""
    print("\n" + "=" * 50)
    print("测试自动推断列类型")
    print("=" * 50)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = None
    try:
        db = connect(f'sqlite:///{db_path}')
        
        # 测试各种数据类型
        data_types = db['data_types']
        
        test_data = {
            'int_val': 42,
            'big_int_val': 9999999999999,
            'float_val': 3.14159,
            'bool_val': True,
            'str_short': 'hello',
            'str_long': 'x' * 300,  # 超过255字符
            'datetime_val': datetime.now(),
            'dict_val': {'key': 'value', 'nested': {'a': 1}},
            'list_val': [1, 2, 3, 'four'],
            'none_val': None,
        }
        
        print("\n插入测试数据...")
        data_types.insert(test_data)
        
        print(f"表中的列: {data_types.columns}")
        
        # 查询并验证
        result = data_types.find_one()
        print("\n查询结果:")
        for key, value in result.items():
            print(f"   {key}: {value} (类型: {type(value).__name__})")
        
        print("\n✅ 自动列类型推断测试通过！")
        
    finally:
        if db is not None:
            db.close()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except PermissionError:
                pass


def test_flyweight_pattern():
    """测试享元模式（相同连接URL返回同一实例）"""
    print("\n" + "=" * 50)
    print("测试享元模式")
    print("=" * 50)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db1 = None
    try:
        db_url = f'sqlite:///{db_path}'
        
        # 多次连接应返回同一实例
        db1 = connect(db_url)
        db2 = connect(db_url)
        db3 = get_db(db_url)
        
        print(f"db1 id: {id(db1)}")
        print(f"db2 id: {id(db2)}")
        print(f"db3 id: {id(db3)}")
        
        assert db1 is db2, "db1 和 db2 应该是同一实例"
        assert db2 is db3, "db2 和 db3 应该是同一实例"
        
        print("\n✅ 享元模式测试通过！同一连接URL返回同一实例")
        
    finally:
        if db1 is not None:
            db1.close()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except PermissionError:
                pass


def test_get_table_helper():
    """测试 get_table 辅助函数"""
    print("\n" + "=" * 50)
    print("测试 get_table 辅助函数")
    print("=" * 50)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = None
    try:
        db_url = f'sqlite:///{db_path}'
        
        # 使用 get_table 直接获取表
        products = get_table(db_url, 'products')
        db = products._db  # 获取 db 实例用于清理
        
        # 插入数据
        products.insert({
            'name': '苹果',
            'price': 5.5,
            'stock': 100
        })
        
        products.insert({
            'name': '香蕉',
            'price': 3.0,
            'stock': 200
        })
        
        # 查询
        all_products = products.find()
        print(f"所有产品: {all_products}")
        
        print("\n✅ get_table 辅助函数测试通过！")
        
    finally:
        if db is not None:
            db.close()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except PermissionError:
                pass


def test_table_operations():
    """测试表级别操作"""
    print("\n" + "=" * 50)
    print("测试表级别操作")
    print("=" * 50)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    db = None
    try:
        db = connect(f'sqlite:///{db_path}')
        
        # 创建表
        print("\n1. 创建表...")
        orders = db.create_table('orders', primary_id='order_id', primary_type='BigInteger')
        print(f"   创建的表: {orders.name}, 列: {orders.columns}")
        
        # 检查表是否存在
        print("\n2. 检查表是否存在...")
        print(f"   'orders' in db: {'orders' in db}")
        print(f"   'nonexistent' in db: {'nonexistent' in db}")
        
        # 列出所有表
        print("\n3. 列出所有表...")
        print(f"   db.tables: {db.tables}")
        
        # 插入数据
        orders.insert({'customer': '客户A', 'amount': 100.5})
        orders.insert({'customer': '客户B', 'amount': 200.0})
        
        # 删除表
        print("\n4. 删除表...")
        db.drop_table('orders')
        print(f"   删除后 db.tables: {db.tables}")
        
        print("\n✅ 表级别操作测试通过！")
        
    finally:
        if db is not None:
            db.close()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except PermissionError:
                pass


if __name__ == '__main__':
    print("=" * 60)
    print("nb_db_dict 模块测试")
    print("类似 dataset 的功能，支持 SQLAlchemy 2.0")
    print("=" * 60)
    
    test_basic_operations()
    test_auto_column_type()
    test_flyweight_pattern()
    test_get_table_helper()
    test_table_operations()
    
    print("\n" + "=" * 60)
    print("🎉 所有测试通过！")
    print("=" * 60)
