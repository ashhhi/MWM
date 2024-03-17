import yaml
from dao import MysqlDb

MysqlConnection = MysqlDb()
print(MysqlConnection.select('select id from analyse_table where sessionId=\'20231208142916_349\''))