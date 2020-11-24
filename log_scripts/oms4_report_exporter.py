import logging
import psycopg2
import pymysql
import collections
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, helpers

BRAND = "oms4"
SYSTERM = "report"
ENV = 'prod'

logging.basicConfig(
    filename=f'/var/log/{BRAND}-{SYSTERM}-{ENV}.log',
    filemode='a',
    format='%(levelname)s %(asctime)s %(pathname)s:%(lineno)s %(process)d %(thread)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

psql_settings = {
    "host": "10.11.110.80",
    "port": 3306,
    "user": "u_sync_oplog_ro",
    "password": "5Drg#TKO9t8B!E^%",
    "db": "db_dms",
    "table": "report_task_info"
}

msql_settings = {
    "host": "r-3306-ecs-oms4-som-01-prod.service.consul",
    "port": 3306,
    "user": "u_sync_oplog_ro",
    "password": "5Drg#TKO9t8B!E^%",
    "db": "db_oms4_auth",
    "table": "user",
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
    def __enter__(self):
        self.db = pymysql.connect(host=msql_settings['host'],
                                  port=msql_settings['port'],
                                  user=msql_settings['user'],
                                  password=msql_settings['password'],
                                  db=msql_settings['db'],
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def query_data(self, sql):
        with self.db.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


class OperatorPsql(object):
    def __init__(self):
        self.duration = timedelta(minutes=10)  # 滚动查询窗口大小, 默认为10min
        self.yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        self.start_time = f"{self.yesterday} 00:00:00"
        self.query_time = datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")  # 查询左边界
        self.end_time = datetime.strptime(f"{self.yesterday} 23:59:59", "%Y-%m-%d %H:%M:%S")
        self.es_cli = OperationES()

    def __enter__(self):
        self.conn = psycopg2.connect(host=psql_settings['host'],
                                     port=psql_settings['port'],
                                     user=psql_settings['user'],
                                     password=psql_settings['password'],
                                     database=psql_settings['db'])
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
            return f'SELECT * FROM {psql_settings["table"]} ' + \
                   f'WHERE create_time >= \'{left_time}\' AND create_time < \'{right_time}\';'

    def __format_data(self, rows):
        resp = []
        for row in rows:
            data = collections.OrderedDict()
            data['@timestamp'] = datetime.strptime(row["create_time"].split('.')[0], '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%dT%H:%M:%S+0800')
            data['user_id'] = row['operator_id']
            data['user_name'] = self.__get_user(data['user_id'])
            data['task_no'] = row['task_no']
            data['report_code'] = row['report_code']
            data['status'] = row['status']
            data['params'] = row['params']
            data['remark'] = row['remark']
            data['current_org_code'] = row['current_org_code']
            data['current_org_type'] = row['current_org_type']
            data['opt_id'] = row['opt_id']
            data['last_modify_time'] = row['last_modify_time']
            resp.append(dict(data))
        return resp

    @staticmethod
    def __get_user(operator_id):
        with OperatorMysql() as db:
            resp = db.query_data(f"select * from {msql_settings['table']} where id={int(operator_id)}")
            if not resp:
                return "unknown"
            return resp[0]["user_name"]


if __name__ == '__main__':
    with OperatorPsql() as operator:
        operator.query_and_export()
