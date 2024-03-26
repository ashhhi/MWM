from dao import MysqlDb

# raw_table_name_list = ['Raw_20240205', 'Raw_20240110', 'Raw_20231202', 'Raw_20231120', 'Raw_20230912']
raw_table_name_list = ['Raw_20230912']

gravity = 9.81
mysqlconn = MysqlDb()

session_counts = {'20211220144525_30': 1, '20220901142743_80': 4608, '20220902105709_90': 696, '20221020170011_31': 4356, '20221021034557_31': 3420, '20220901155848_123': 2844, '20221027141210_116': 336, '20221125152422_167': 120, '20221027105458_129': 228, '20221201144917_144': 1476, '20221109103208_156': 1128, '20221215145441_144': 4872, '20230105110344_127': 4128, '20221115104921_162': 2520, '20221220111957_162': 744, '20230118152234_189': 660, '20230215102255_243': 180, '20230202145312_80': 156, '20230307141952_289': 3768, '20230309113708_206': 732, '20230218101823_265': 72, '20230404150601_370': 2700, '20230411103125_404': 4392, '20230411120508_304': 336, '20230329145314_341': 3828, '20230411154839_374': 3168, '20230413121358_333': 792, '20230417144749_357': 12, '20230419120928_310': 60, '20230421144626_345': 1380, '20230427141502_394': 84, '20230504104924_248': 3948, '20230518103312_445': 1560, '20230519150936_342': 2244, '20230519150936_347': 1596, '20230424144401_358': 2160, '20230601095804_463': 372, '20230601103031_443': 1104, '20230601131508_412': 4728, '20230608111840_448': 2388, '20230613102550_392': 2820, '20230617100535_465': 528, '20230621104454_382': 2376, '20230623100237_388': 780, '20230624100733_465': 3972, '20230627154911_459': 1128, '20230630160734_461': 3732, '20230706100939_465': 1752, '20230713143335_492': 1860, '20230713143746_490': 3372, '20230713152509_561': 1584, '20230713152509_562': 504, '20230722144029_527': 3624, '20230803142740_387': 201, '20230815103732_641': 12, '20230830151547_470': 1020, '20230831143519_650': 3888, '20230906100924_573': 4344, '20230911151105_664': 1680}
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
            print('update')
        elif new_level == 'medium' and record['level'] == 'small':
            print('update')