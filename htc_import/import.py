import xmlrpc.client as xc
from read_config import ReadConfig
from read_xml import ReadXML
import datetime
import os
from os.path import join
import ipaddress
import sys
import logging
import shutil
from os import getcwd, path


logging.basicConfig(filename="log.txt",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)

if __name__ == '__main__':
    try:
        cwd = getcwd()
        if not path.isfile(cwd+'log.txt'):
            open(cwd+'log.txt', 'w')
        cfg = ReadConfig()
        switcher = {
            "#S": "site_name",
            "#I": "site_code",
            "#D": str(datetime.datetime.today().strftime('%y%m%d')),
            "#T": "000000",
            "#M": "xx:xx:xx:xx:xx:xx"
        }
        common = xc.ServerProxy('{}/xmlrpc/2/common'.format(cfg.url))
        common.version()
        uid = common.authenticate(cfg.db, cfg.username, cfg.password, {})
        models = xc.ServerProxy('{}/xmlrpc/2/object'.format(cfg.url))
        can_access_sensor = models.execute_kw(cfg.db, uid, cfg.password,
                                              'htc.sensor', 'check_access_rights',
                                              ['read'], {'raise_exception': False})

        can_access_transaction = models.execute_kw(cfg.db, uid, cfg.password,
                                                   'htc.sensor_transaction', 'check_access_rights',
                                                   ['write'], {'raise_exception': False})
        current_file_name = None

        if can_access_sensor and can_access_transaction:
            xml = ReadXML(cfg)
            dic_trans = dict()
            try:
                dic_trans = xml.read()
            except Exception as e:
                id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                    'create_uid': uid,
                    'create_date': datetime.datetime.today(),
                    'name': "Call from RPC",
                    'type': "client",
                    'dbname': cfg.db,
                    'path': "",
                    'func': "xml.read()",
                    'line': "",
                    'level': "ERROR",
                    'message': str(e)
                }])

            try:

                for key, value in dic_trans.items():
                    obj = value[0]
                    current_file_name = obj.file_name
                    today = datetime.datetime.today().date()
                    trn_date = obj.transaction_date.date()
                    mac_address = value[0].mac_address
                    site_code = value[0].site_code
                    sensor = models.execute_kw(cfg.db, uid, cfg.password,
                                               'htc.sensor', 'search_read',
                                               [[['mac_address', '=', mac_address]]],
                                               {'fields': ['group_sensor_ids'],'limit': 1})
                    site = models.execute_kw(cfg.db, uid, cfg.password,
                                             'htc.site', 'search_read',
                                             [[['site_code', '=', site_code]]],
                                             {'fields': ['ip_range', 'site_file_name_format','file_name','site_name'], 'limit': 1})
                    ipList = []
                    valid_ip = []
                    if site:
                        file_name_vlaues = []
                        format_symbol=[]
                        if len(site[0]['site_file_name_format']) == len(obj.file_name):
                            orginal_name_formats = obj.file_name.split(".")
                            file_name_ids = site[0]['file_name']
                            file_names = models.execute_kw(cfg.db, uid, cfg.password,
                                             'file.format', 'search_read',
                                             [[['id', 'in', file_name_ids]]],
                                             {'fields': ['name', 'value']})
                            for i in range(len(file_names)):
                                file_name = file_names[i]['name']
                                format_symbol.append(file_name)
                                if file_name == "#S" or file_name == "#I":
                                    field_name = switcher.get(file_name)
                                    field_value = site[0][field_name]
                                    file_name_vlaues.append(field_value)
                                elif file_name == "#M":
                                    file_name_vlaues.append(mac_address)
                                elif file_name == "#D" or file_name == "#T":
                                    file_name_vlaues.append(i)
                            file_name_vlaues.append("xml")
                            for file_value in file_name_vlaues:
                                if type(file_value) is int:
                                    file_name_vlaues[file_value] = orginal_name_formats[file_value]
                            file_name_result = ".".join(str(x) for x in file_name_vlaues)
                            file_name_result = file_name_result.replace(":", "-")
                            if file_name_result != obj.file_name:
                                if os.path.exists(cfg.get_root_folder()  + '/Error'):
                                    shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                      join(cfg.get_root_folder()  + '/Error', obj.file_name))
                                else:
                                    try:
                                        os.mkdir(cfg.get_source_folder() + '/Error')
                                        shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                                join(cfg.get_source_folder() + '/Error', obj.file_name))
                                    except OSError:
                                        print("Creation of the directory %s failed" % cfg.get_root_folder() + '/Error')
                                        logging.info(str(e))
                                format_symbol_text = ".".join(str(x) for x in format_symbol)
                                format_symbol_text += ".xml"

                                id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                                    'create_uid': uid,
                                    'create_date': datetime.datetime.today(),
                                    'name': "Call from RPC",
                                    'type': "client",
                                    'dbname': cfg.db,
                                    'path': "",
                                    'func': "not valid file name formate",
                                    'line': "",
                                    'level': "ERROR",
                                    'message': "file name must be " + format_symbol_text + " " + file_name_result + " insead of" + obj.file_name
                                }])
                                continue
                            try:
                                ipList = list(ipaddress.ip_network(site[0]['ip_range'], False).hosts())
                                extract_ip_list = list(map(lambda x: x.compressed, ipList))
                                valid_ip = list(filter(lambda x: x == obj.ip_address, extract_ip_list))
                            except Exception as e:
                                logging.info(str(e))
                                id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                                    'create_uid': uid,
                                    'create_date': datetime.datetime.today(),
                                    'name': "Call from RPC",
                                    'type': "client",
                                    'dbname': cfg.db,
                                    'path': "",
                                    'func': "not valid ip",
                                    'line': "",
                                    'level': "ERROR",
                                    'message': str(e) + " " +obj.file_name
                                }])
                                continue
                        else:
                            if os.path.exists(cfg.get_root_folder() + '/Error'):
                                shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                      join(cfg.get_root_folder() + '/Error', obj.file_name))
                            else:
                                try:
                                    os.mkdir(cfg.get_root_folder() + '/Error')
                                    shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                            join(cfg.get_root_folder() + '/Error', obj.file_name))
                                except OSError:
                                    print("Creation of the directory %s failed" % cfg.get_root_folder() + '/Error')
                                    logging.info(str(e))
                            id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                                        'create_uid': uid,
                                        'create_date': datetime.datetime.today(),
                                        'name': "Call from RPC",
                                        'type': "client",
                                        'dbname': cfg.db,
                                        'path': "",
                                        'func': "not valid file name format",
                                        'line': "",
                                        'level': "ERROR",
                                        'message': "not valid file name format" + " " +obj.file_name
                                    }])
                            continue
                    if len(valid_ip) == 0:
                        id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                            'create_uid': uid,
                            'create_date': datetime.datetime.today(),
                            'name': "Call from RPC",
                            'type': "client",
                            'dbname': cfg.db,
                            'path': "",
                            'func': "not valid ip",
                            'line': "",
                            'level': "ERROR",
                            'message': str(e) + " " +obj.file_name
                        }])
                        if os.path.exists(cfg.get_root_folder() + '/Error'):
                                shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                      join(cfg.get_root_folder() + '/Error', obj.file_name))
                        else:
                            try:
                                os.mkdir(cfgget_root_folder()  + '/Error')
                                shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                        join(cfg.get_root_folder()  + '/Error', obj.file_name))
                            except OSError:
                                print("Creation of the directory %s failed" % cfg.get_root_folder() + '/Error')
                                logging.info(str(e))
                        continue

                    if sensor and site:
                        model_list = list()
                        for v in value:
                            model_list.append({
                                'site_id':
                                    site[0]['id'],
                                'sensor_id':
                                    sensor[0]['id'],
                                'transaction_date':
                                    v.transaction_date,
                                'in_count':
                                    v.in_count,
                                'out_count':
                                    v.out_count,
                                'status':
                                    v.status,
                                'process_count':
                                    0,
                                'week':
                                    v.week,
                                'day':
                                    v.day,
                                'method':
                                    "System",
                                'start_time':
                                    v.start_time,
                                'end_time':
                                    v.end_time
                            })
                        total_in = sum(map(lambda x: int(x.get("in_count")), model_list))
                        total_out = sum(map(lambda x: int(x.get("out_count")), model_list))
                        if today == trn_date:
                            daily_counter_model = models.execute_kw(cfg.db, uid, cfg.password,
                                                                    'htc.daily_counter', 'search_read',
                                                                    [[['sensor_id', '=', sensor[0]['id']]]],
                                                                    {'fields': ['transaction_date', 'daily_total_in',
                                                                                'daily_total_out', 'alert_count'], 'limit': 1})
                            if daily_counter_model:
                                if daily_counter_model[0]['transaction_date'] == str(trn_date):
                                    model_total_in = daily_counter_model[0]['daily_total_in'] + total_in
                                    model_total_out = daily_counter_model[0]['daily_total_out'] + total_out
                                    model_alert_count = daily_counter_model[0]['alert_count']
                                    if sensor[0]['group_sensor_ids']:
                                        group_sensor = models.execute_kw(cfg.db, uid, cfg.password,
                                                                         'htc.group_sensors', 'search_read',
                                                                         [[['id', '=', sensor[0]['group_sensor_ids']]]],
                                                                         {'fields': ['enable_alert',
                                                                                     'in_status',
                                                                                     'alert_count',
                                                                                     'inform_limit_count']})
                                        for gs in group_sensor:
                                            if gs['enable_alert']:
                                                if gs['in_status'] == 5 and \
                                                        model_total_in > gs['inform_limit_count'] * model_alert_count:
                                                    model_alert_count += 1
                                                    models.execute_kw(cfg.db, uid, cfg.password,
                                                                      'htc.notification_email', 'email_notify',
                                                                      [['record', 'total', 'types', 'limit_count',
                                                                        'device_name']],
                                                                      {'record': gs, 'total': model_total_in,
                                                                       'types': 'In',
                                                                       'limit_count': gs['inform_limit_count'],
                                                                       'device_name': obj.device_name})
                                                elif gs['in_status'] == 10 and \
                                                        model_total_out > gs['inform_limit_count'] * model_alert_count:
                                                    model_alert_count += 1
                                                    param = dict()
                                                    param['user_id'] = uid

                                                    models.execute_kw(cfg.db, uid, cfg.password,
                                                                      'htc.notification_email', 'email_notify',
                                                                      [['record', 'total', 'types', 'limit_count',
                                                                        'device_name']],
                                                                      {'record': gs, 'total': model_total_out,
                                                                       'types': 'Out',
                                                                       'limit_count': gs['inform_limit_count'],
                                                                       'device_name': obj.device_name})
                                    models.execute_kw(cfg.db, uid, cfg.password, 'htc.daily_counter', 'write',
                                                    [daily_counter_model[0]['id'], {
                                                        'daily_total_in': model_total_in,
                                                        'daily_total_out': model_total_out,
                                                        'alert_count': model_alert_count,
                                                    }])
                                else:
                                    models.execute_kw(cfg.db, uid, cfg.password, 'htc.daily_counter', 'write',
                                                    [daily_counter_model[0]['id'], {
                                                        'daily_total_in': total_in,
                                                        'daily_total_out': total_out,
                                                        'alert_count': 1,
                                                        'transaction_date' :today,
                                                    }])
                            else:
                                if sensor[0]['group_sensor_ids']:
                                    group_sensor = models.execute_kw(cfg.db, uid, cfg.password,
                                                                     'htc.group_sensors', 'search_read',
                                                                     [[['id', '=', sensor[0]['group_sensor_ids']]]],
                                                                     {'fields': ['enable_alert',
                                                                                 'in_status',
                                                                                 'alert_count']})
                                    for gs in group_sensor:
                                        if gs['enable_alert']:
                                            if gs['in_status'] == 5 and \
                                                    total_in > gs['inform_limit_count'] * model_alert_count:
                                                model_alert_count += 1
                                                models.execute_kw(cfg.db, uid, cfg.password,
                                                                  'htc.notification_email', 'email_notify',
                                                                  [['record', 'total', 'types', 'limit_count',
                                                                    'device_name']],
                                                                  {'record': gs, 'total': total_in,
                                                                   'types': 'In',
                                                                   'limit_count': gs['inform_limit_count'],
                                                                   'device_name': obj.device_name})
                                            elif gs['in_status'] == 10 and \
                                                    total_out > gs['inform_limit_count'] * model_alert_count:
                                                model_alert_count += 1
                                                models.execute_kw(cfg.db, uid, cfg.password,
                                                                  'htc.notification_email', 'email_notify',
                                                                  [['record', 'total', 'types', 'limit_count',
                                                                    'device_name']],
                                                                  {'record': gs, 'total': total_out,
                                                                   'types': 'Out',
                                                                   'limit_count': gs['inform_limit_count'],
                                                                   'device_name': obj.device_name})
                                models.execute_kw(cfg.db, uid, cfg.password, 'htc.daily_counter', 'write', [
                                    {
                                        'site_id':
                                            site[0]['id'],
                                        'sensor_id':
                                            sensor[0]['id'],
                                        'transaction_date':
                                            obj.transaction_date,
                                        'daily_total_in':
                                            total_in,
                                        'daily_total_out':
                                            total_out,
                                        'alert_count':
                                            1
                                    }
                                ])
                        else:
                            daily_counter_model = models.execute_kw(cfg.db, uid, cfg.password,
                                                                    'htc.daily_counter', 'search_read',
                                                                    [[['sensor_id', '=', sensor[0]['id']]]],
                                                                    {'fields': ['transaction_date', 'daily_total_in',
                                                                                'daily_total_out', 'alert_count'], 'limit': 1})
                            if daily_counter_model:
                                models.execute_kw(cfg.db, uid, cfg.password, 'htc.daily_counter', 'write',
                                                        [daily_counter_model[0]['id'], {
                                                            'daily_total_in': 0,
                                                            'daily_total_out': 0,
                                                            'alert_count': 1,
                                                            'transaction_date' :datetime.datetime.today(),
                                                        }])
                            else:
                                models.execute_kw(cfg.db, uid, cfg.password, 'htc.daily_counter', 'create',
                                                        [{
                                                            'daily_total_in': 0,
                                                            'daily_total_out': 0,
                                                            'alert_count': 1,
                                                            'transaction_date' :datetime.datetime.today(),
                                                        }])
                        models.execute_kw(cfg.db, uid, cfg.password, 'htc.sensor_transaction', 'create', model_list)
                        if os.path.exists(cfg.get_root_folder()  + '/Processed'):
                            shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                      join(cfg.get_root_folder()  + '/Processed', obj.file_name))
                        else:
                            try:
                                os.mkdir(cfg.get_root_folder()  + '/Processed')
                                shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                          join(cfg.get_root_folder()  + '/Processed', obj.file_name))
                            except OSError:
                                print("Creation of the directory %s failed" % cfg.get_root_folder()  + '/Processed')

                        models.execute_kw(cfg.db, uid, cfg.password, 'htc.sensor', 'write', [sensor[0]['id'], {
                            'ip_address': obj.ip_address,
                            'device_name': obj.device_name,
                            'device_id': obj.device_id,
                            'host_name': obj.host_name,
                            'timezone_name': obj.timezone_name,
                            'software_version': obj.software_version,
                            'serial_number': obj.serial_number,
                            'sensor_name': obj.name,
                            'status':True
                        }])
            except Exception as e:
                id = models.execute_kw(cfg.db, uid, cfg.password, 'ir.logging', 'create', [{
                    'create_uid': uid,
                    'create_date': datetime.datetime.today(),
                    'name': "Call from RPC",
                    'type': "client",
                    'dbname': cfg.db,
                    'path': "",
                    'func': "populate data",
                    'line': "",
                    'level': "ERROR",
                    'message': str(e)
                }])
                if os.path.exists(cfg.get_root_folder()  + '/Error'):
                                shutil.move(join(cfg.get_source_folder(), obj.file_name),
                                      join(cfg.get_root_folder()  + '/Error', obj.file_name))
                else:
                    try:
                        os.mkdir(cfg.get_root_folder()  + '/Error')
                        shutil.move(join(cfg.get_source_folder(), current_file_name),
                                join(cfg.get_root_folder()  + '/Error', current_file_name))
                    except OSError:
                        print("Creation of the directory %s failed" % cfg.get_root_folder()  + '/Error')
                        logging.info(str(e))
                logging.info(str(e))


    except Exception as e:
        print(e)
        logging.info(str(e))
        sys.exit(-1)
