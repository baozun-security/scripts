import logging
import collections
import cx_Oracle as oracle
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, helpers

BRAND = "pacs"
SYSTERM = "pacs"
ENV = 'prod'

logging.basicConfig(
    filename=f'/var/log/{BRAND}-{SYSTERM}-{ENV}.log',
    filemode='a',
    format='%(levelname)s %(asctime)s %(pathname)s:%(lineno)s %(process)d %(thread)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

db_settings = {
    "host": "pacsstb.cloud.bz:1521",
    "user": "u_sync_oplog_ro",
    "password": "5Drg#TKO9t8B!E^%",
    "db": "pacs_stb",
    "table": "pacs.t_sys_export_log"
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
            helpers.bulk(self.es, new_data)
        except Exception as err:
            logging.error(f"Failed bulk insert, err: {err}")

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])


class OperatorOracle(object):
    def __init__(self):
        self.duration = timedelta(minutes=10)  # 滚动查询窗口大小, 默认为10min
        self.yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")
        self.start_time = f"{self.yesterday} 00:00:00"
        self.query_time = datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S")  # 查询左边界
        self.end_time = datetime.strptime(f"{self.yesterday} 23:59:59", "%Y-%m-%d %H:%M:%S")
        self.es_cli = OperationES()

    def __enter__(self):
        self.conn = oracle.connect("%s/%s@%s/%s" % (db_settings['user'], db_settings['password'], db_settings['host'],
                                                    db_settings['db']))
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
                    desc = [d[0].lower() for d in cursor.description]
                    result = [dict(zip(desc, row)) for row in rows]
                    self.es_cli.bulk_insert(self.__format_data(result))
                self.reset_query_time()

    def build_sql(self):
        left_time = self.query_time
        right_time = left_time + self.duration

        if right_time >= self.end_time:  # 处理越界
            right_time = self.end_time
        if left_time < right_time:
            time_layout = "yyyy-mm-dd HH24:MI:SS"
            fields = ["login_ip", "login_id", "login_name", "op_module", "op_url", "op_type", "rows_number", "status",
                      "remark", f'to_char(create_time,\'{time_layout}\') as create_time']
            return f'select {",".join(fields)} from {db_settings["table"]} WHERE ' + \
                   f'to_char(create_time, \'{time_layout}\') >= \'{left_time}\' AND ' + \
                   f'to_char(create_time, \'{time_layout}\') < \'{right_time}\''

    @staticmethod
    def __format_data(rows):
        resp = []
        for row in rows:
            data = collections.OrderedDict()
            data['@timestamp'] = datetime.strptime(row["create_time"], '%Y-%m-%d %H:%M:%S').strftime(
                '%Y-%m-%dT%H:%M:%S+0800')
            data['user_name'] = row['login_name']
            data['user_id'] = row['login_id']
            data['login_ip'] = row['login_ip']
            data['op_module'] = row['op_module']
            data['op_type'] = row['op_type']
            data['op_url'] = row['op_url']
            data['rows_number'] = row['rows_number']
            data['status'] = row['status']
            data['remark'] = row['remark']
            resp.append(dict(data))
        return resp


if __name__ == '__main__':
    with OperatorOracle() as operator:
        operator.query_and_export()
