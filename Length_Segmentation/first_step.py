import numpy as np
from decimal import Decimal
from dao import MysqlDb
import matplotlib.pyplot as plt
from scipy.signal import find_peaks
from tqdm import tqdm

# raw_table_name_list = ['Raw_20240205', 'Raw_20240110', 'Raw_20231202', 'Raw_20231120', 'Raw_20230912']
raw_table_name_list = ['Raw_20230912']
gravity = 9.81


# 按时间升序排列
def sorted_id(time_ms, time_s, data_arr, id):
    # id是数据库自增key，可以通过这个来排序使得时间轴正向排列
    zipped = zip(time_ms, time_s, data_arr, id)
    sorted_zipped = sorted(zipped, key=lambda x: x[3])
    time_ms, time_s, data_arr, id = zip(*sorted_zipped)
    return time_ms, time_s, data_arr, id


def Visual(Time, Data, label_y, save_path, label_x='Time (ms)'):
    # 绘制图表
    plt.plot(Time, Data)

    # 设置横轴和纵轴标签
    plt.xlabel(label_x)
    plt.ylabel(label_y)

    # 可选：设置横轴和纵轴的范围
    plt.xlim(0, max(Time))
    plt.ylim(min(Acce), max(Acce))

    # 可选：设置图表标题
    plt.title(f'{label_x} vs. {label_y}')

    if save_path:
        # 存储
        plt.savefig(save_path)
        plt.close('all')
    else:
        plt.show()


if __name__ == '__main__':
    mysqlconn = MysqlDb()
    large_cnt = 0
    medium_cnt = 0
    small_cnt = 0

    for raw_table_name in raw_table_name_list:
        print("Now running table:", raw_table_name)
        number_of_session = mysqlconn.select(f"select COUNT(DISTINCT sessionid) AS session_count from {raw_table_name}")
        print('Session number:', number_of_session)
        # session_list = mysqlconn.select(f"select DISTINCT sessionid from {raw_table_name}")
        session_list = mysqlconn.select(f'select * from (SELECT sessionid, MIN(id) AS id FROM {raw_table_name} GROUP BY sessionid) as temp_table order by id ASC')
        # print(session_list)
        large_id = []
        medium_id = []
        small_id = []
        shorter_than_5000 = []
        offset = 0
        page_size = 2000000
        cnt = int(number_of_session[0]['session_count'])
        to_remove = []
        # test = []

        while True:
            print(f'Number of unprocessed sessionId in current table ({raw_table_name}):', cnt)
            print(f'Current offset: {offset}, Current page: {offset // page_size}')
            number_of_empty = 0
            # 在下一次while循环中去除掉已经遍历过的sessionId
            if len(to_remove):
                session_list_copy = [x for x in session_list if x['sessionid'] not in to_remove]
                session_list = session_list_copy
                to_remove = []
            # 分页查询，避免数据量过大内存爆表
            all_data = mysqlconn.page_select(raw_table_name, page_size=page_size, offset=offset)
            if not all_data:
                break
            if len(shorter_than_5000):
                all_data.extend(shorter_than_5000)
            offset += page_size
            shorter_than_5000 = []
            # 每次都全部运行完的话太费时间了，因为后面的sessionId基本上在靠前页的数据中都没有（因为我给select出来的数据都进行了排序处理）
            # 因此为了加速，这里使用合适的数值作为一个for循环, 10000是一条sessionId的估计长度
            # for i in tqdm(range(len(session_list))):
            for i in tqdm(range(min(len(session_list), page_size // 10000 * 3))):
                sessionid = session_list[i]['sessionid']
                # data = mysqlconn.select(f'select * from {raw_table_name} where sessionId =\'{sessionid}\'')
                data = [row for row in all_data if row.get('sessionId') == sessionid]
                '''1. 去除太短的数据，或把该数据保留下来，添加到下一页的数据之后'''
                if len(data) < 5000:
                    # test.append(len(data))
                    shorter_than_5000.extend(data)
                    continue
                # print('large')
                to_remove.append(sessionid)
                number_of_empty += 1
                # print(sessionid, "length", len(data))
                Time_ms = []
                Time_s = []
                AcceX = []
                AcceY = []
                AcceZ = []
                Acce = []
                GyroX = []
                GyroY = []
                GyroZ = []
                Gyro = []
                id = []
                for item in data:
                    Time_ms.append(item['Time_ms'])
                    Time_s.append(item['Time_s'])
                    AcceX.append(float(item['AcceX']) * gravity)
                    AcceY.append(float(item['AcceY']) * gravity)
                    AcceZ.append(float(item['AcceZ']) * gravity)
                    GyroX.append(float(item['GyroX']) * gravity)
                    GyroY.append(float(item['GyroY']) * gravity)
                    GyroZ.append(float(item['GyroZ']) * gravity)
                    id.append(item['id'])
                    Acce.append((float(item['AcceX']) + float(item['AcceY']) + float(item['AcceZ'])) * gravity)
                    Gyro.append((float(item['GyroX']) + float(item['GyroY']) + float(item['GyroZ'])) * gravity)


                '''2. 按id升序排列，防止数据库里是打乱的数据'''
                Time_ms, Time_s, Acce, id = sorted_id(Time_ms, Time_s, Acce, id)

                cnt_minute = 0
                Time = []
                for i in range(len(Time_s)):
                    if Time_s[i] == 0 and i != 0 and Time_s[i - 1] > 0:
                        cnt_minute += 1
                    Time.append((Time_s[i] + cnt_minute * 60 - Time_s[0]) * 1000 + Time_ms[i])
                Acce = np.array(Acce)

                '''3. 只需要峰值部分，去除非峰值的数据'''
                Acce_pos_peaks, _ = find_peaks(Acce)  # 正峰值
                Acce_neg_peaks, _ = find_peaks(-Acce)  # 负峰值
                Acce_peaks = np.concatenate((Acce_neg_peaks, Acce_pos_peaks))
                clean_data = np.zeros_like(Acce)
                clean_data[Acce_peaks] = Acce[Acce_peaks]
                Acce = clean_data

                if max(Acce) > 100 or min(Acce) < -100:
                    # print("大峰值：", sessionid, max(Acce), min(Acce))
                    large_id.append([sessionid, raw_table_name, max(Acce), min(Acce), 'large'])
                    # Visual(Time, Acce, 'Acce', f'pic/over100/{sessionid}')
                elif max(Acce) > 50 or min(Acce) < -50:
                    # print("中峰值：", sessionid, max(Acce), min(Acce))
                    medium_id.append([sessionid, raw_table_name, max(Acce), min(Acce), 'medium'])
                    # Visual(Time, Acce, 'Acce', f'pic/over50/{sessionid}')
                else:
                    # print("小峰值：", sessionid, max(Acce), min(Acce))
                    small_id.append([sessionid, raw_table_name, max(Acce), min(Acce), 'small'])
                    # Visual(Time, Acce, 'Acce', f'pic/under50/{sessionid}')

            cnt -= number_of_empty

        # 查看长度小于5000的session，对比是不是都小于5000，如果不是的话就单独拿出来后续再做处理
        session_counts = {}
        for item in shorter_than_5000:
            sessionid = item['sessionId']
            if sessionid in session_counts:
                session_counts[sessionid] += 1
            else:
                session_counts[sessionid] = 1
        print('< 5000:')
        # session_counts要保存下来（复制粘贴），进入下一步处理
        print(session_counts)

        large_cnt += len(large_id)
        medium_cnt += len(medium_id)
        small_cnt += len(small_id)
        print('插入数据库中...')
        # 插入数据库
        for item in large_id:
            sql = f'insert into amplitude_analyse (peak, valley, `level`, sessionId, tableName) values {item[2], item[3], item[4], item[0], item[1]}'
            mysqlconn.execute_db(sql)
        for item in medium_id:
            sql = f'insert into amplitude_analyse (peak, valley, `level`, sessionId, tableName) values {item[2], item[3], item[4], item[0], item[1]}'
            mysqlconn.execute_db(sql)
        for item in small_id:
            sql = f'insert into amplitude_analyse (peak, valley, `level`, sessionId, tableName) values {item[2], item[3], item[4], item[0], item[1]}'
            mysqlconn.execute_db(sql)
        print('插入数据库完成!!!')
        print('current_large_cnt:', large_cnt)
        print('current_medium_cnt:', medium_cnt)
        print('current_small_cnt:', small_cnt)

    print('---------------------------')
    print('large_cnt:', large_cnt)
    print('medium_cnt:', medium_cnt)
    print('small_cnt:', small_cnt)
    print('---------------------------')
