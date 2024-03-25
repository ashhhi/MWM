import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from dao import MysqlDb


# def getSessionData(mysql_user, mysql_password, mysql_host, mysql_database):
#     # MySQL connection configuration
#     conn = mysql.connector.connect(
#         host=mysql_host,
#         user=mysql_user,
#         password=mysql_password,
#         database=mysql_database,
#     )
#
#     # Retrieve the accelerometer data and store it in a variable
#     cursor = conn.cursor()
#     print('fetch data')
#     cursor.execute(
#         f"SELECT sessionId FROM {summary} WHERE gameStartTime BETWEEN '{startData}' AND '{endData}' AND (game = 2 OR game = 3) group by sessionId, userId ORDER BY gameStartTime asc")
#     data = cursor.fetchall()
#     return data


# def getRawData(mysql_user, mysql_password, mysql_host, mysql_database, sessionId):
#     # MySQL connection configuration
#     conn = mysql.connector.connect(
#         host=mysql_host,
#         user=mysql_user,
#         password=mysql_password,
#         database=mysql_database,
#     )
#     # Retrieve the accelerometer data and store it in a variable
#     sql_query = f"""SELECT Time_s, Time_ms, AcceX, AcceY, AcceZ, GyroX, GyroY, GyroZ, Temp_C
#                    FROM raw WHERE sessionId = '{sessionId}'"""
#     cursor = conn.cursor()
#     cursor.execute(sql_query)
#     data = cursor.fetchall()
#     return data


# def uploadAnalysis(mysql_user, mysql_password, mysql_host, mysql_database, summaryList):
#     # MySQL connection configuration
#     conn = mysql.connector.connect(
#         host=mysql_host,
#         user=mysql_user,
#         password=mysql_password,
#         database=mysql_database,
#     )
#
#     for data in summaryList:
#         # Retrieve the accelerometer data and store it in a variable
#         sql_query = f"""UPDATE snmmpdb.analyse_table SET checkDuplicate = 1
#                     WHERE sessionId = '{data}' AND checkDuplicate = 0;"""
#
#         cursor = conn.cursor()
#         cursor.execute(sql_query)
#         conn.commit()


# database setting
summary_name = 'Summary_20230912'
raw_name = 'Raw_20230912'

startData = '2021-12-17 00:00:00'
endData = '2023-09-21 23:59:59'


mysqlconn = MysqlDb()
sessionIdList = mysqlconn.select(f"SELECT sessionId FROM {summary_name} WHERE gameStartTime BETWEEN \'{startData}\' AND \'{endData}\' AND (game = 2 OR game = 3) group by sessionId, userId ORDER BY gameStartTime asc")
sessionIdLength = len(sessionIdList)
cur = 0
duplicateList = []

if sessionIdLength != 0:
    print("0 /", sessionIdLength)
    for sessionId in sessionIdList:
        cur += 1
        if cur % 200 == 0:
            print(cur, "/", sessionIdLength)
        # Retrieve the signal from the database
        rawData = mysqlconn.select(f'SELECT * FROM {raw_name} WHERE sessionId = \'{sessionId}\'')
        dataLength = len(rawData)
        # Should get in database
        setData = set(rawData)
        setLength = len(setData)
        if setLength != 0:
            if setLength / dataLength <= 0.5:
                # print("data length:" , dataLength, ". set length:", setLength)
                duplicateList.append(sessionId[0])

    print(sessionIdLength, "/", sessionIdLength)
    print("upload to database")
    for data in duplicateList:
        mysqlconn.execute_db(
            f'UPDATE snmmpdb.analyse_table SET checkDuplicate = 1 WHERE sessionId = \'{data}\' AND checkDuplicate = 0;')
    print("upload finish")
    duplicateGames = len(duplicateList)
    print("Duplicate percent:", duplicateGames, "/", sessionIdLength, "= ", duplicateGames / sessionIdLength * 100, "%")

else:
    print("Not game record")
