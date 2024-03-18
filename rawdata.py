import numpy as np

from dao import MysqlDb
import matplotlib.pyplot as plt
from scipy.signal import find_peaks

raw_table_name = 'Raw_20231202'

# 按时间升序排列
def sorted_time(time_arr, data_arr):
    # 使用 zip() 函数将时间数组和数据数组打包成元组列表，并按照时间进行排序
    sorted_data = [x for _, x in sorted(zip(time_arr, data_arr))]
    # 按照时间进行排序后的结果
    sorted_time = sorted(time_arr)
    return sorted_time, sorted_data


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
        data = mysqlconn.select(f'select * from Raw_20231202 where sessionId =\'{sessionid}\'')
        print(len(data))
        Time = []
        AcceX = []
        AcceY = []
        AcceZ = []
        Acce = []
        GyroX = []
        GyroY = []
        GyroZ = []
        Gyro = []
        for item in data:
            Time.append(item['Time_ms'] + item['Time_s'] * 1000)
            AcceX.append(item['AcceX'])
            AcceY.append(item['AcceY'])
            AcceZ.append(item['AcceZ'])
            GyroX.append(item['GyroX'])
            GyroY.append(item['GyroY'])
            GyroZ.append(item['GyroZ'])
            Acce.append(float(item['AcceX']) + float(item['AcceY']) + float(item['AcceZ']))
            Gyro.append(float(item['GyroX']) + float(item['GyroY']) + float(item['GyroZ']))

        '''1. 去除太短的数据'''
        if len(Acce) < 5000:
            continue

        '''2. 按时间升序排列，数据库里时间是打乱的'''
        Time_ms, Acce = sorted_time(Time, Acce)

        Acce = np.array(Acce)
        Gyro = np.array(Gyro)

        Visual(Time, Acce, 'Acce', f'pic/origin/{sessionid}')

        '''3. 只需要峰值部分，去除非峰值的数据'''
        Acce_pos_peaks, _ = find_peaks(Acce)    # 正峰值
        Acce_neg_peaks, _ = find_peaks(-Acce)   # 负峰值
        Acce_peaks = np.concatenate((Acce_neg_peaks, Acce_pos_peaks))
        clean_data = np.zeros_like(Acce)
        clean_data[Acce_peaks] = Acce[Acce_peaks]
        Acce = clean_data

        Visual(Time, Acce, 'Acce', f'pic/without_threshold/{sessionid}')

        '''求正负峰值的平均值，拿到噪声大概的threshold'''

        # pos_avarage = Acce[Acce_pos_peaks].mean()
        # neg_average = Acce[Acce_neg_peaks].mean()
        # pos_threshold = pos_avarage
        # neg_threshold = neg_average
        pos_threshold = 100
        neg_threshold = -100
        print('pos_threshold', pos_threshold, 'neg_threshold', neg_threshold)
        Acce[(Acce < pos_threshold) & (Acce > neg_threshold)] = 0

        Visual(Time, Acce, 'Acce', f'pic/with_threshold/{sessionid}')




