"""
数据同步类
1. 提供配置信息加载函数
1. 提供数据库 Engine or Connection 对象创建函数
2. 提供 tushare DataApi 对象函数
"""

import configparser
import datetime
import logging
import os
import time

import pandas as pd
import pymysql
import tushare as ts
import sqlalchemy

class TushareSync:
    _BEGIN_DATE = '20100101' # 同步数据最早开始日期
    _LIMIT = 10000 # 每次从tushare同步数据量
    _INTERVAL = 2 # 每次同步数据间隔时间

    def __init__(self, table_name, api_name, date_column, end_date=""):
        self.limit = TushareSync._LIMIT
        self.interval = TushareSync._INTERVAL

        self._init_env()
        self.set_sync_setting(table_name, api_name, fields, date_column, end_date)

    # 初始化环境变量
    def _init_env(self):
        self._cfg = None
        self._tushare_api = None
        self._logger = None
        self._mysql_conn = None
        self._sqlalchemy_conn = None

    # 设置同步相关参数
    def set_sync_setting(self, table_name, api_name, fields, date_column, end_date=""):
        self.table_name = table_name
        self.api_name = api_name
        self.date_column = date_column
        self.fields = self.get_fields()
        if not end_date:
            end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
        self.end_date = end_date

    def __del__(self):
        if self._mysql_conn:
            self._mysql_conn.close()

        if self._sqlalchemy_conn:
            self._sqlalchemy_conn.dispose()


    @property
    def BEGIN_DATE(self):
        return self._BEGIN_DATE 
    
    # 加载配置信息函数
    def get_cfg(self):
        if self._cfg is None:
            cfg = configparser.ConfigParser()
            file_name = os.path.join(os.path.dirname(__file__), '../application.ini')
            file_name = os.path.abspath(file_name)
            cfg.read(file_name)
            self._cfg = cfg
        
        return self._cfg

    # 获取 SQLAlchemy Connection 对象
    def get_db_conn(self):
        if self._sqlalchemy_conn is None:
            cfg = self.get_cfg()
            db_host = cfg['mysql']['host']
            db_user = cfg['mysql']['user']
            db_password = cfg['mysql']['password']
            db_port = cfg['mysql']['port']
            db_database = cfg['mysql']['database']
            db_url = f'mysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8&use_unicode=1'
            self._sqlalchemy_conn = sqlalchemy.create_engine(db_url)
        
        return self._sqlalchemy_conn

    # 获取 pymysql Connection 对象
    def get_pymysql_conn(self):
        if self._mysql_conn is None:
            cfg = self.get_cfg()
            self._mysql_conn = pymysql.connect(host=cfg['mysql']['host'],
                            port=int(cfg['mysql']['port']),
                            user=cfg['mysql']['user'],
                            passwd=cfg['mysql']['password'],
                            db=cfg['mysql']['database'],
                            charset='utf8')
            
        return self._mysql_conn
    

    def get_fields(self):
        # 从建表sql脚本中读取建表SQL语句

        # 按行分割SQL语句
        lines = self.get_table_sql().split('\n')
        columns = []
        
        # 遍历每一行
        for line in lines:
            line = line.strip()
            # 跳过空行和非字段定义行
            if not line or line.startswith(('CREATE', 'DROP', ')', 'ENGINE', 'UNIQUE', '/*!', 'PARTITION')):
                continue
                
            # 提取字段名
            if '`' in line:
                column_name = line.split('`')[1]  # 获取第一对反引号中的内容
                columns.append(column_name)
        
        return columns

    def get_table_sql(self):
        """
        获取建表SQL语句
        """
        table_sql = ""
        with open(self.get_table_filepath(), 'r', encoding='utf-8') as f:
            table_sql = f.read()
        return table_sql

    def is_table_exist(self):
        return self.get_one_from_sql(f"SELECT COUNT(1) FROM information_schema.TABLES WHERE TABLE_NAME='{self.table_name}'") > 0
    
    
    def _clean_sql(self, s: str) -> str:
        """
        删除SQL字符串中每行的注释内容（# 后面的部分）
        
        Args:
            s: 包含SQL语句的多行字符串
        
        Returns:
            清理后的SQL字符串
        """
        # 按行分割字符串
        lines = s.split('\n')
        # 处理每一行，删除#后面的内容
        cleaned_lines = [line.split('#')[0].rstrip() for line in lines]
        # 重新组合成字符串，去除空行
        return '\n'.join(line for line in cleaned_lines if line.strip())


    # 执行一条 SQL 语句，返回 results
    def exec_sql(self, sql):
        clean_sql = self._clean_sql(sql)

        result = None

        # 执行多条语句
        for row in clean_sql.split(';'):
            if row.strip() != '':
                result = self.get_db_conn().execute(sqlalchemy.text(row))

        return result
    
    # 执行一条 SQL 语句，返回结果中的第一个值
    def get_one_from_sql(self, sql):
        return self.exec_sql(sql).fetchone()[0]

    # 将tushare dataframe 数据写入数据库, 使用 SQLAlchemy
    def save_datafame_to_db(self, data, if_exists="append"):
        sqlalchemy_conn = self.get_db_conn()
        if sqlalchemy_conn:
            data.to_sql(self.table_name, sqlalchemy_conn, index=False, if_exists=if_exists, chunksize=self.limit)
        else:
            raise Exception("创建 sqlalchemy conn 失败.")

    # 构建 Tushare 查询 API 接口对象
    def get_tushare_api(self):
        if self.tushare_api is None:
            cfg = self.get_cfg()
            token = cfg['tushare']['token']
            self.tushare_api = ts.pro_api(token=token, timeout=300)
        
        return self.tushare_api

    
    def log_info(self, msg):
        self.get_logger().info(msg)

    def log_warn(self, msg):
        self.get_logger().warn(msg)

    def log_error(self, msg):
        self.get_logger().error(msg)

    # 获取日志文件打印输出对象
    def get_logger(self):
        if self._logger:
            return self._logger
        
        log_name = self.table_name
        file_name = cfg['logging']['filename']
        
        cfg = self.get_cfg()    
        log_level = cfg['logging']['level']
        backup_days = int(cfg['logging']['backupDays'])
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)
        log_dir = os.path.join(os.getcwd(), 'logs')
        log_file = os.path.join(log_dir, f'{file_name}.{str(datetime.datetime.now().strftime("%Y-%m-%d"))}')
        
        # 添加文件日志处理    
        if file_name != '':
            if not os.path.exists(log_dir):
                logger.info(f"创建日志文件夹 [{str(log_dir)}]")
                os.makedirs(log_dir)

            clen_file = os.path.join(log_dir, 'file_name.%s' %
                                    str((datetime.datetime.now() +
                                        datetime.timedelta(days=-backup_days)).strftime('%Y-%m-%d'))
                                    )

            if os.path.exists(clen_file):
                os.remove(clen_file)

            handler = logging.FileHandler(log_file, encoding='utf-8')
            file_fmt = '[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)s - %(name)s ] %(message)s '
            formatter = logging.Formatter(file_fmt)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_fmt = '[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)s - %(name)s ] %(message)s '
        console_formatter = logging.Formatter(console_fmt)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        logger.info(f"日志文件： [{log_file}]")

        self._logger = logger
        return self._logger

    # 获取 SQL 脚本存储文件夹
    def get_sql_folder(self):
        cfg = self.get_cfg()
        return cfg['mysql']['sql-folder']
    
    # 获取表 SQL 脚本文件绝对路径
    def get_table_filepath(self):
        sql_folder = self.get_sql_folder()
        return os.path.join(os.getcwd(), sql_folder, f'{self.table_name}.sql')

    
    def create_table(self, drop_exist=True):
        """
        数据库中建表
        :param drop_exist: 如果表存在是否先 Drop 后再重建
        :return:
        """

        if (drop_exist) or (not self.table_exist(self.table_name)):
            self.exec_sql(f"DROP TABLE IF EXISTS {self.table_name};") 

            table_sql_filepath = self.get_table_filepath()
            table_sql = ""
            with open(table_sql_filepath, "r", encoding="utf-8") as table_file:
                table_sql = table_file.read()

            self.exec_sql(table_sql)
            
            # todo, write log
            # self.log_info(f'Execute result: Total [{count}], Succeed [{suc_cnt}] , Failed [{flt_cnt}] ')


    def table_exist(self, table_name) -> bool:
        try:
            sql = f"desc {table_name};"
            result = self.exec_sql(sql)
            return True 
        except Exception as e:
            return False
        

    def query_last_sync_date(self, sql):
        """
        查询历史同步数据的最大日期
        :param sql: 执行查询的SQL
        :return: 查询结果
        """
        logger = self.get_logger("utils", 'data_syn.log')
        conn = self.get_pymysql_conn()
        cursor = conn.cursor()
        cursor.execute(sql + ';')
        result = cursor.fetchall()
        cursor.close()
        last_date = result[0][0]
        result = "19700101"
        if last_date is not None:
            result = str(last_date)
        logger.info(f"Query last sync date with sql [{sql}], result: [{result}]")
        return result


    # 获取两个日期的最小值
    def min_date(self, date1, date2):
        return date1 if date1 <= date2 else date2

    # 获取两个日期的最大值
    def max_date(self, date1, date2):
        return date1 if date1 >= date2 else date2
    
    


    def sync_from_tushare_to_db(self, start_date, end_date):
        """
        将数据从tushare同步到数据库
        :param start_date: 开始时间
        :param end_date: 结束时间
        :return: None
        流程:
        1. 清理历史数据
        2. 按日期循环从tushare抓取数据，并保存到数据库
        """

        # 创建 API / Connection / Logger 对象
        ts_api = self.get_tushare_api()
        connection = self.get_db_conn()

        try:
            # 清理历史数据
            self.exec_sql(f"DELETE FROM {self.table_name} WHERE {self.date_column}>='{start_date}' AND {self.date_column}<='{end_date}'")
            self.log_info(f'Execute Clean SQL {self.table_name}: {start_date} ~ {end_date}')

            # 数据同步时间开始时间和结束时间, 包含前后边界
            start = datetime.datetime.strptime(start_date, '%Y%m%d')
            end = datetime.datetime.strptime(end_date, '%Y%m%d')

            date = start

            while date <= end:
                step_date = str(date.strftime('%Y%m%d'))
                offset = 0
                while True:
                    self.log_info(f"Tushare API: {self.api_name} {self.date_column}[{step_date}] from offset[{offset}] limit[{self.limit}]")
                    data = ts_api.query(self.api_name,
                                        **{
                                            self.date_column: step_date,
                                            "start_date": step_date,
                                            "end_date": step_date,
                                            "offset": offset,
                                            "limit": self.limit
                                        },
                                        fields=self.fields)
                    time.sleep(self.interval)
                    if data.last_valid_index() is not None:
                        size = data.last_valid_index() + 1
                        self.log_info(f"Write [{size}] records into table [{self.table_name}]")
                        
                        # data.to_sql(self.table_name, connection, index=False, if_exists='append', chunksize=self.limit)
                        self.save_datafame_to_db(data)

                        offset = offset + size
                        if size < self.limit:
                            break
                    else:
                        break
                # 更新下一次微批时间段
                date = date + datetime.timedelta(days=1)
                break
        except Exception as e:
            self.log_error("Get Exception[%s]" % e.__cause__)


    def full_sync(self):
        """
        全量初始化表数据
        """
        self.create_table(True)
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))
        self.sync_from_tushare_to_db(self.BEGIN_DATE, end_date)


    def incremental_sync(self):
        """
        增量同步数据
        """

        # 查询历史最大同步日期
        last_date = self.get_one_from_sql(f"select max({self.date_column}) from {self.table_name}")
        start_date = self.max_date(last_date, self.BEGIN_DATE)
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))

        self.sync_from_tushare_to_db(start_date, end_date)


def test_get_fields():
    basic_sync = TushareSync("stock_basic", "stock_basic", "trade_date")
    print(basic_sync.get_fields())


if __name__ == '__main__':
    test_get_fields()

