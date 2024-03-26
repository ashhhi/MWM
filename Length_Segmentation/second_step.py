from dao import MysqlDb

# raw_table_name_list = ['Raw_20240205', 'Raw_20240110', 'Raw_20231202', 'Raw_20231120', 'Raw_20230912']
raw_table_name_list = ['Raw_20230912']

gravity = 9.81
mysqlconn = MysqlDb()

session_counts = {'20230713152509_562': 504, '20230722144029_527': 3624, '20230803142740_387': 201, '20230815103732_641': 12, '20230830151547_470': 1020, '20230831143519_650': 3888, '20230906100924_573': 4344, '20230911151105_664': 1680}
for sessionId in session_counts.keys():
    print('sessionId:', sessionId)
    data = []
    for raw_table_name in raw_table_name_list:
        data = mysqlconn.select(f'select * from {raw_table_name} where sessionId = \'{sessionId}\'')
        if data:
            break
    if not data:
        print('less than 5000')
        continue
    print(data)
    AcceX = []
    AcceY = []
    AcceZ = []
    Acce = []
    for item in data:
        AcceX.append(float(item['AcceX']) * gravity)
        AcceY.append(float(item['AcceY']) * gravity)
        AcceZ.append(float(item['AcceZ']) * gravity)
        Acce.append((float(item['AcceX']) + float(item['AcceY']) + float(item['AcceZ'])) * gravity)
    peak, valley = max(Acce), min(Acce)
    record = mysqlconn.select(f'select peak, valley, level from amplitude_analyse where sessionid = \'{sessionId}\'')
    print(record)
    if record:
        new_peak = max(peak, record['peak'])
        new_valley = min(valley, record['valley'])
        if new_peak > 100 or new_valley < -100:
            new_level = 'large'
        elif new_peak > 50 or new_valley < -50:
            new_level = 'medium'
        else:
            new_level = 'small'
        if new_level == 'large' and (record['level'] == 'medium' or record['level'] == 'small'):
            mysqlconn.execute_db(f'update amplitude_analyse set peak={new_peak}, valley={new_valley}, level = \'{new_level}\' where sessionid =  \'{sessionId}\'')
        elif new_level == 'medium' and record['level'] == 'small':
            mysqlconn.execute_db(f'update amplitude_analyse set peak={new_peak}, valley={new_valley}, level = \'{new_level}\' where sessionid =  \'{sessionId}\'')