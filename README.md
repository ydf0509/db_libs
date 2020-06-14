# db_libs

各种数据库的封装。只封装最难的部分，十分克制。


在redis和pymongo的基础上进行封装。

由于原生的包已经足够好用了，并不需要重新写几百个方法进行过度封装。

仅仅是加了享元模式，使得在相同入参情况下无限实例化相关客户端类的时候不会反复重新连接。

封装方式采用的非常规方式，使用的是继承方式，而非通常情况下的使用组合来进行封装。

继承方式封装的比组合模式封装的好处更多。

```python
# 组合模式封装的代码一般是如下这种例子。
"""
这种方式封装是组合，精确点是23种设计模式的代理模式。
代理模式说的是定义很多方法，来调用self.r所具有的方法。


另外对于无限实例化，还使用了享元模式。
封装数据库切记不要使用单例模式，如果入参传了不同的主机ip或者不同的db，而仍然返回之前的连接对象，那就大错特错了。

"""
# 使用组合模式封装的redis，没有继承模式的好用。
import redis

class RedisClient:

    params__reids_map = {}

    def __init__(self,host,db,port,password):
        if (host,db,port,password) not in self.__class__.params__reids_map:
            self.r = redis.Redis(host,db,port,password)
            self.__class__.params__reids_map[(host,db,port,password)] = self.r
        else:
            self.r = self.__class__.params__reids_map[(host,db,port,password)]
    
    def my_set(self,key,value):
        print('额外的扩展')
        self.r.set(name=key,value=value)
    
    def my_get(self,name):
        # 这个封装简直是多次一举，在redis实例化时候，
        # 将Redis类的构造方法的入参 decode_responses设置为True，就可以避免几百个方法需要反复decode了。
        return self.r.get(name).decode()

```

