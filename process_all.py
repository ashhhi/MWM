import numpy as np
from decimal import Decimal
from dao import MysqlDb
import matplotlib.pyplot as plt
from scipy.signal import find_peaks

raw_table_name = 'Raw_20240205'
gravity = 9.81

# 按时间升序排列
def sorted_id(time_ms, time_s, data_arr, id):
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
    number_of_session = mysqlconn.select(f"select COUNT(DISTINCT sessionid) AS session_count from {raw_table_name}")
    print("number of session", number_of_session)
    session_list = mysqlconn.select(f"select DISTINCT sessionid from {raw_table_name}")
    print(session_list)

    for session in session_list:
        sessionid = session['sessionid']
        data = mysqlconn.select(f'select * from {raw_table_name} where sessionId =\'{sessionid}\'')
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
            GyroZ.append(float(item['GyroZ'])    * gravity)
            id.append(item['id'])
            Acce.append((float(item['AcceX']) + float(item['AcceY']) + float(item['AcceZ'])) * gravity)
            Gyro.append((float(item['GyroX']) + float(item['GyroY']) + float(item['GyroZ'])) * gravity)

        '''1. 去除太短的数据'''
        if len(Acce) < 5000:
            continue

        '''2. 按id升序排列，防止数据库里是打乱的数据'''
        Time_ms, Time_s, Acce, id = sorted_id(Time_ms, Time_s, Acce, id)

        cnt_minute = 0
        Time = []
        for i in range(len(Time_s)):
            if Time_s[i] == 0 and i != 0 and Time_s[i-1] > 0:
                cnt_minute += 1
            Time.append((Time_s[i] + cnt_minute * 60 - Time_s[0]) * 1000 + Time_ms[i])
        Acce = np.array(Acce)

        Visual(Time, Acce, 'Acce', f'pic/origin/{sessionid}')

        '''3. 只需要峰值部分，去除非峰值的数据'''
        Acce_pos_peaks, _ = find_peaks(Acce)    # 正峰值
        Acce_neg_peaks, _ = find_peaks(-Acce)   # 负峰值
        Acce_peaks = np.concatenate((Acce_neg_peaks, Acce_pos_peaks))
        clean_data = np.zeros_like(Acce)
        clean_data[Acce_peaks] = Acce[Acce_peaks]
        Acce = clean_data

        Visual(Time, Acce, 'Acce', f'pic/without_threshold/{sessionid}')

        '''设置threshold'''
        # pos_avarage = Acce[Acce_pos_peaks].mean()
        # neg_average = Acce[Acce_neg_peaks].mean()
        # pos_threshold = pos_avarage
        # neg_threshold = neg_average
        pos_threshold = 50
        neg_threshold = -30
        print('pos_threshold', pos_threshold, 'neg_threshold', neg_threshold)
        Acce[(Acce < pos_threshold) & (Acce > neg_threshold)] = 0

        Visual(Time, Acce, 'Acce', f'pic/with_threshold/{sessionid}')




