# coding=utf8
"""
@author:Administrator
@file: redis_lib.py
@time: 2020/06
"""
import redis2  # pip install redis2
import redis3  # pip install redis3
import decorator_libs  # pip install decorator_libs

"""
将Redis类的构造方法的入参 decode_responses设置为True，将会减少很多手动decode的麻烦。

"""

@decorator_libs.flyweight
class RedisV2(redis2.Redis):
    """
    通常封装的redis工具类，一般是控制单例、享元模式等，使其在实例化时候不会无限重新校验账号密码进行连接。
    通常如果要封装redis，一般使用的是组合的方式，并不会使用到继承官方Redis类，此封装将打破这一常规使用继承，同时使用了享元模式装饰器，确保不会无限重新连接。

    此类由于是继承关联方Reids类，所以可以直接使用一切Redis类的方法，同时在此类可以添加自己的一些方法。
    我不建议直接覆盖父类的方法名，我希望确保原生的Redis方法名是原汁原味的，如果要扩展，我希望是添加一些不同的方法名。
    """

    def my_set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        此方法是举个例子
        这样此类技能照常使用原生Redis类的方法，又能添加自己的一些方法。
        :param name:
        :param value:
        :param ex:
        :param px:
        :param nx:
        :param xx:
        :return:
        """
        print('添加自己的一些逻辑')
        self.set(name, value, ex, px, nx, xx)


@decorator_libs.flyweight
class RedisV3(redis3.Redis):
    """
    redis2 和 redis3这两个包有很多不同之处，redis2是redis包的2.10.6的不同命名空间的备份版本。
    redis3 是redis包的3.xx 的不同命名空间的备份版本。
    之所以这样做，是为了可以确保在同一个项目中，同时使用redis的2.xx和3.xx。
    通常，同一个项目要使用一个包的两个版本是不可能的，因为是在同一个解释器下运行，无法使用所谓的python虚拟环境来隔离使用不同版本。

    redis2和redsi3有许多方法，虽然方法名一样，但入参位置，入参名称，入参类型不一样。
    例如你老项目想使用celery4.4版本，会被安装上redis 3.xx版本，如果你的项目已经多处使用了redis 2.xx的方法，运行起来会产生很多错误。
    如果坚持使用celery4.4版本，同时又不想去修改项目中大量的redis 2.xx的使用地方，那么你的redis工具类可以改成依赖redis2这个包，而不是去依赖redis包的2.xx版本,这样第三方包依赖何种redis版本都不会影响到你。

    此类是为了redsi3.

    """


if __name__ == '__main__':
    """
    测试10000次写入，都采用无限实例化的方式。
    使用没有加入享元模式的Redis类操作10000次时间远远大于RedisV2操作时间。
    """
    with decorator_libs.TimerContextManager():
        for _ in range(100):
            redis2.Redis(password='123456').set('test1', 1)

    with decorator_libs.TimerContextManager():
        for _ in range(100):
            RedisV2(password='123456').set('test1', 1)
