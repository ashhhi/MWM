import pymysql, yaml
from DBUtils.PooledDB import PooledDB
# 读取 YAML 文件
with open('config.yaml', 'r') as file:
    data = yaml.load(file, Loader=yaml.FullLoader)
    dbhost = data['dbhost']
    dbport = data['dbport']
    dbname = data['dbname']
    dbuser = data['dbuser']
    dbpwd = data['dbpwd']


POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=0,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=0,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    host=dbhost,
    port=dbport,
    user=dbuser,
    password=dbpwd,
    database=dbname,
    charset='utf8'
)

class MysqlDb:
    def __init__(self):
        # get a link from the pool
        # self.conn = pymysql.connect(
        #     host=dbhost,
        #     port=dbport,
        #     user=dbuser,
        #     passwd=dbpwd,
        #     db=dbname,
        #     autocommit=True
        # )
        self.conn = POOL.connection()
        self.cur = self.conn.cursor(pymysql.cursors.DictCursor)

    def close(self):  # 对象资源被释放时触发，在对象即将被删除时的最后操作
        # 关闭游标
        self.cur.close()
        # 关闭数据库连接
        self.conn.close()

    def select(self, sql):
        try:
            # 查询
            # 检查连接是否断开，如果断开就进行重连
            self.conn.ping(reconnect=True)
            # sql = "select * from testbeds_info"
            # 使用 execute() 执行sql
            self.cur.execute(sql)
            # 使用 fetchall() 获取查询结果
            data = self.cur.fetchall()
            return data
        except Exception as e:
            print("mysql查找操作出现错误：{}".format(e))
            # 回滚所有更改
            self.conn.rollback()

    def execute_db(self, sql):
        """更新/新增/删除"""
        try:
            # 检查连接是否断开，如果断开就进行重连
            self.conn.ping(reconnect=True)
            # 使用 execute() 执行sql
            self.cur.execute(sql)
            # 提交事务
            self.conn.commit()
        except Exception as e:
            print("mysql更新/新增/删除操作出现错误：{}".format(e))
            # 回滚所有更改
            self.conn.rollback()