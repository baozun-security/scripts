import logging
import pymysql
import collections
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, helpers

BRAND = "shopdog"
SYSTERM = "shopdog"
ENV = 'prod'

logging.basicConfig(
    filename=f'/var/log/{BRAND}-{SYSTERM}-{ENV}.log',
    filemode='a',
    format='%(levelname)s %(asctime)s %(pathname)s:%(lineno)s %(process)d %(thread)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

db_settings = {
    "host": "r-3306-ecs-shopdog-prod.service.consul",
    "port": 3306,
    "user": "u_sync_oplog_ro",
    "password": "5Drg#TKO9t8B!E^%",
    "db": "db_shopdog",
    "table": "sys_op_log"
}

es_settings = {
    "host": "elk-jiyun-coordinating.cloud.bz",
    "port": 80,
    "user": "security",
    "password": "nuR1GGNL",
    "index": f"security-{BRAND}-{SYSTERM}-{ENV}",  # 索引规范：security-{品牌}-{系统}
}


class OperationES(object):
    def __init__(self):
        self.index = es_settings['index']
        self.request_timeout = 120
        self.es = Elasticsearch([{'host': es_settings['host'], 'port': es_settings['port']}],
                                http_auth=(es_settings['user'], es_settings['password']))

    def query_data(self, dsl):
        return self.es.search(body=dsl, index=self.index, request_timeout=self.request_timeout)

    def insert(self, body, index=None):
        try:
            index = index or self.index
            body["@timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800")
            insert = self.es.index(index=index, body=body, request_timeout=self.request_timeout)
            return insert
        except Exception as e:
            logging.exception(e)

    def bulk_insert(self, data, index=None):
        new_data = []
        for item in data:
            temp = {
                "_index": index or self.index,
                "_source": item
            }
            new_data.append(temp)
        try:
            helpers.streaming_bulk(self.es, new_data)
        except Exception as err:
            logging.error(f"Failed bulk insert, err: {err}")

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])


class OperatorMysql(object):
    def __init__(self):
        self.duration = timedelta(minutes=10)  # 滚动查询窗口大小, 默认为10min
        self.yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        self.start_time = f"{self.yesterday} 00:00:00"
        self.query_time = datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")  # 查询左边界
        self.end_time = datetime.strptime(f"{self.yesterday} 23:59:59", "%Y-%m-%d %H:%M:%S")
        self.es_cli = OperationES()

    def __enter__(self):
        self.conn = pymysql.connect(host=db_settings['host'],
                                    port=db_settings['port'],
                                    user=db_settings['user'],
                                    password=db_settings['password'],
                                    db=db_settings['db'],
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def reset_query_time(self):
        self.query_time += self.duration

    def query_and_export(self):
        with self.conn.cursor() as cursor:
            while True:
                sql = self.build_sql()
                if not sql:
                    break
                cursor.execute(sql)
                rows = cursor.fetchall()
                if rows:
                    self.es_cli.bulk_insert(self.__format_data(rows))
                self.reset_query_time()

    def build_sql(self):
        left_time = self.query_time
        right_time = left_time + self.duration

        if right_time >= self.end_time:  # 处理越界
            right_time = self.end_time
        if left_time < right_time:
            return f'SELECT * FROM {db_settings["table"]} ' + \
                   f'WHERE create_time >= \'{left_time}\' AND create_time < \'{right_time}\';'

    @staticmethod
    def __format_data(rows):
        resp = []
        for row in rows:
            data = collections.OrderedDict()
            data['@timestamp'] = datetime.strptime(row["create_time"], '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%dT%H:%M:%S+0800')
            data['user_name'] = row['user_login']
            data['user_id'] = row['user_id']
            data['op_module'] = row['op_module']
            data['op_type'] = row['op_type']
            data['op_url'] = row['op_url']
            resp.append(dict(data))
        return resp


if __name__ == '__main__':
    with OperatorMysql() as operator:
        operator.query_and_export()
