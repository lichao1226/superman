import json
import time
from rediscluster import RedisCluster

class CollectRedis(object):
    """对Redis常用函数进行封装"""
    _timecount = 2
    _TIME_OUT = 20
    def __init__(self,startup_nodes):
        try:
            self.redis = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
        except Exception as e:
            if self._timecount < self._TIME_OUT:
                interval = 5
                self._timecount += interval
                time.sleep(interval)
                self._init(startup_nodes)
            else:
                raise e
    def get_dic(self,table):
        """
        通过pipe获取table中的所有hash记录数据
        :param table: redis表名
        :return: domain_valid_ns，list, 形式：[{},{},{}]
        """
        try:
            t1 = time.time()
            with self.redis.pipeline(transaction=False) as p:
                p.hgetall(table)
                data = p.execute()
            t2 = time.time()
        except Exception as e:
            raise e
        domain_valid_ns = []
        values = data[0].values()
        for i in values:
            domain_valid_ns.append(json.loads(i))
        return domain_valid_ns

    def get_list(self,table):
        """
        通过pipeline获取table中的所有list记录数据
        :param table: redis表名
        :return: task_data，list, 形式：[{},{},{}]
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                p.lrange(table,0,-1)
                data = p.execute()
                task_data = []
                for d in data[0]:
                    task_data.append(json.loads(d))
                return task_data
        except Exception as e:
            raise e

    def update_dic(self,table,data):
        """
        使用管道更新dic数据
        :param table: redis表名
        :param data: list,需要保存的数据，数据结构:{domain:{},domain:{},...}
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                for domain in data:
                    p.hset(table, domain, json.dumps(data[domain]))
                p.execute()
        except Exception as e:
            raise e

    def update_dic_timer(self,table,data,timer=3600):
        """
        带超时器的更新dic数据函数
        :param table: redis表名
        :param data: list,需要保存的数据，数据结构:{domain:{},domain:{},...}
        :param timer: str,默认3600秒，当超时，对应key及数据自动删除
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                for domain in data:
                    p.hset(table, domain, json.dumps(data[domain]))
                    p.expire(table,timer)
                p.execute()
        except Exception as e:
            raise e

    def update_list(self,table,data):
        """
        使用管道向redis中更新list数据，原来存在更新最新数据，没有则插入数据
        :param table: redis表名
        :param data: list,需要保存的数据，数据结构：[{},{},{},...{}]
        :return:
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                for domain in data:
                    p.lpush(table, json.dumps(domain))
                p.execute()
        except Exception as e:
            raise e
        
    def update_list_timer(self,table,data,timer=3600):
        """
        使用管道向redis中更新list数据，原来存在更新最新数据，没有则插入数据
        :param table: redis表名
        :param data: list,需要保存的数据，数据结构：[{},{},{},...{}]
        :param timer: str,默认3600秒，当超时，对应key及数据自动删除
        :return:
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                for domain in data:
                    p.lpush(table, json.dumps(domain))
                    p.expire(table, timer)
                p.execute()
        except Exception as e:
            raise e

    def del_table_dic(self,table,key):
        """
        删除hash,table表中指定单个key及对应值；当表中只有一条k-v记录时，删除时，表也默认跟着被删除。
        """
        try:
            self.redis.hdel(table,key)
        except Exception as e:
            raise e
    def del_dic_keys(self,table,keys):
        """
        删掉hash,dic数据结构中，多个key-value值
        :param table: 表名
        :param keys: list,要删除的多个key
        """
        try:
            with self.redis.pipeline(transaction=False) as p:
                for k in keys:
                    p.hdel(table,k)
                p.execute()
        except Exception as e:
            raise e

    def del_table_list(self,table,flag=True):
        """
        删掉list数据结构中，一个元素，分为左删除、右删除。
        :param table: 表名
        :param: flag: bool,插入数据是左插入，删除时，也默认左删除；flag设置为False时，右删除
        """
        try:
            if flag:
                return self.redis.lpop(table) #删除并返回第一个元素，即第一行数据
            else:
                return self.redis.rpop(table) #删除并返回最后一个元素
        except Exception as e:
            raise e

    def del_table(self,table):
        """删除table表"""
        try:
            self.redis.delete(table)
        except Exception as e:
            raise e

if __name__  == '__main__':
    pass
    # with open('redis_db_config.json','r') as fp:
    #     db_config = json.loads(fp.read())['single']
    # single_redis = singRedis(db_config)
    # data = single_redis.get_dic('domain_valid_ns')














