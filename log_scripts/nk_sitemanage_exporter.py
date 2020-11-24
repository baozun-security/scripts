import psycopg2,logging,json,collections
from datetime import datetime,timedelta
from elasticsearch import Elasticsearch,helpers

logging.basicConfig(filename='/var/log/nikesitemanage.log',
                    filemode='a',
                    format='%(levelname)s %(asctime)s %(pathname)s:%(lineno)s %(process)d %(thread)d %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


def nikesitemanage():
    es = Elasticsearch([{'host': 'elk-jiyun-coordinating.cloud.bz', 'port': 80}],
                       http_auth=('security', 'nuR1GGNL'))
    index = 'security-nike-sitemanage-user-prod'
    resp = []
    try:
        pg = psycopg2.connect(database='db_nike_hk',
                              user='u_devops_soc_ro',
                              password='a9MWY6HzN5G',
                              host='10.18.4.10',
                              port=5433)
    except Exception as err:
        logging.error(err)
        return
    cur = pg.cursor()
    init_time = datetime.today() - timedelta(days=1)
    while init_time < datetime.today():
        init_time_after_ten_minutes = init_time + timedelta(minutes=10)
        sql = '''SELECT * FROM t_au_user_op_log WHERE op_time >= '%s' AND op_time < '%s';''' % (
        init_time, init_time_after_ten_minutes)
        cur.execute(sql)
        rows = cur.fetchall()
        if rows:
            for i in rows:
                data = collections.OrderedDict()
                data['ip'] = i[1]
                try:
                    data['@timestamp'] = datetime.strptime(str(i[2]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                        '%Y-%m-%dT%H:%M:%S.%f+0800')
                except ValueError:
                    data['@timestamp'] = datetime.strptime(str(i[2]), '%Y-%m-%d %H:%M:%S').strftime(
                        '%Y-%m-%dT%H:%M:%S+0800')
                except Exception as error:
                    logging.error(error)

                data['req_method'] = i[3]
                data['req_param'] = i[4]
                data['res_name'] = i[5]
                data['res_uri'] = i[6]
                data['resp_result'] = i[7]
                data['session_id'] = i[8]
                data['user_id'] = i[9]
                data['user_name'] = i[10]
                try:
                    data['version'] = datetime.strptime(str(i[11]), '%Y-%m-%d %H:%M:%S.%f').strftime(
                        '%Y-%m-%dT%H:%M:%S.%f+0800')
                except ValueError:
                    data['version'] = datetime.strptime(str(i[11]), '%Y-%m-%d %H:%M:%S').strftime(
                        '%Y-%m-%dT%H:%M:%S+0800')
                except Exception as error:
                    logging.error(error)
                resp.append(json.loads(json.dumps(data)))
        init_time += timedelta(minutes=10)
    cur.close()
    pg.close()

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
    logging.info('%s成功写入数据%s条' %(index,len(actions)))

nikesitemanage()