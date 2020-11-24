import time
import logging
import pymysql
import collections
from datetime import datetime,timedelta
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch([{'host': 'elk-jiyun-coordinating.cloud.bz', 'port': 80}],
                   http_auth=('security', 'nuR1GGNL'))
index = 'security-bi-bi-prod'

logging.basicConfig(filename='/var/log/%s.log'%index,
                    filemode='a',
                    format='%(levelname)s %(asctime)s %(pathname)s:%(lineno)s %(process)d %(thread)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def security_bi():
    try:
        conn = pymysql.connect(host='10.11.12.32',
                               port=3306,
                               user='u_sync_oplog_ro',
                               password='5Drg#TKO9t8B!E^%',
                               db='finebi5_repo',
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)

    except Exception as err:
        logging.error(err)
        return

    resp = []
    cur = conn.cursor()
    init_time = datetime.today() - timedelta(days=1)
    while init_time < datetime.today():
        init_time_stamp = int(time.mktime(init_time.timetuple()))*1000
        init_time_after_ten_minutes = init_time + timedelta(minutes=10)
        init_time_after_ten_minutes_stamp = int(time.mktime(init_time_after_ten_minutes.timetuple()))*1000
        # print(init_time_stamp,init_time_after_ten_minutes_stamp)
        sql = '''SELECT * FROM fine_record_execute WHERE time >= '%s' AND time < '%s';''' % (
            init_time_stamp, init_time_after_ten_minutes_stamp)
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            for row in rows:
                data = collections.OrderedDict()
                data['syncid'] = row['syncID']
                data['storagetime'] = row['storageTime']
                data['browser'] = row['browser']
                data['complete'] = row['complete']
                data['consume'] = row['consume']
                data['displayname'] = row['displayName']
                data['id'] = row['id']
                data['login_ip'] = row['ip']
                data['memory'] = row['memory']
                data['param'] = row['param']
                data['reportid'] = row['reportId']
                data['sessionid'] = row['sessionID']
                data['source'] = row['source']
                data['query'] = row['query']
                data['querytime'] = row['queryTime']
                data['tname'] = row['tname']
                data['executetype'] = row['executeType']
                data['user_id'] = row['userId']
                data['user_name'] = row['username']
                data['user_role'] = row['userrole']
                _timestamp = int(row[time] / 1000)
                timeArray = time.localtime(_timestamp)
                styleTime = time.strftime('%Y-%m-%d %H:%M:%S', timeArray)
                data['@timestamp'] = datetime.strptime(styleTime, '%Y-%m-%d %H:%M:%S').strftime(
                    '%Y-%m-%dT%H:%M:%S+0800')
                resp.append(dict(data))
        init_time += timedelta(minutes=10)
    cur.close()
    conn.close()
    actions = [
            {
                '_index': index,
                '_source': data,
            }
            for data in resp
        ]
    try:
        helpers.bulk(es, actions)
    except Exception as err:
        logging.error(err)
        return
    logging.info('%s成功写入数据%s条' % (index, len(actions)))

security_bi()