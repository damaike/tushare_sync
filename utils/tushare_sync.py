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
from sqlalchemy import create_engine

class TushareSync:
    _BEGIN_DATE = '20100101'
    _LIMIT = 5000
    _INTERVAL = 0.3

    def __init__(self, table_name, api_name, fields, date_column, end_date=""):
        self.limit = TushareSync._LIMIT
        self.interval = TushareSync._INTERVAL

        self._cfg = None
        self._tushare_api = None
        self._logger = None
        self._mysql_connection = None
        self._mock_connection = None

        self.sync_setting(table_name, api_name, fields, date_column, end_date)

    # 设置同步相关参数
    def sync_setting(self, table_name, api_name, fields, date_column, end_date):
        self.table_name = table_name
        self.api_name = api_name
        self.fields = fields
        self.date_column = date_column
        self.end_date = end_date

    def __del__(self):
        if self._mysql_connection:
            self._mysql_connection.close()

        if self._mock_connection:
            self._mock_connection.dispose()


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

    def get_sql_folder(self):
        cfg = self.get_cfg()
        return cfg['mysql']['sql_folder']
    
    def get_table_sql_filepath(self):
        sql_folder = self.get_sql_folder()
        return os.path.join(os.getcwd(), sql_folder, f'{self.table_name}.sql')

    # 获取 MySQL Connection 对象
    def get_mock_connection(self):
        if self._mock_connection is None:
            cfg = self.get_cfg()
            db_host = cfg['mysql']['host']
            db_user = cfg['mysql']['user']
            db_password = cfg['mysql']['password']
            db_port = cfg['mysql']['port']
            db_database = cfg['mysql']['database']
            db_url = f'mysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8&use_unicode=1'
            self._mock_connection = create_engine(db_url)
        
        return self._mock_connection

    def get_mysql_connection(self):
        if self._mysql_connection is None:
            cfg = self.get_cfg()
            self._mysql_connection = pymysql.connect(host=cfg['mysql']['host'],
                            port=int(cfg['mysql']['port']),
                            user=cfg['mysql']['user'],
                            passwd=cfg['mysql']['password'],
                            db=cfg['mysql']['database'],
                            charset='utf8')
            
        return self._mysql_connection
    
    # 执行 MySQL SQL 语句，使用 pymsql
    def exec_mysql_sql(self, sql):
        conn = self.get_mysql_connection()
        cursor = conn.cursor()
        counts = cursor.execute(sql + ';')
        conn.commit()
        cursor.close()
        return counts

    # 将tushare dataframe 数据写入数据库, 使用 SQLAlchemy
    def save_datafame_to_db(self, data, if_exists="append"):
        data.to_sql(self.table_name, self.get_mock_connection(), index=False, if_exists=if_exists, chunksize=self.limit)

    # 构建 Tushare 查询 API 接口对象
    def get_tushare_api(self):
        if self.tushare_api is None:
            cfg = self.get_cfg()
            token = cfg['tushare']['token']
            self.tushare_api = ts.pro_api(token=token, timeout=300)
        
        return self.tushare_api

    # 获取日志文件打印输出对象
    def get_logger(self, log_name, file_name):
        if self._logger:
            return self._logger
        
        cfg = self.get_cfg()    
        log_level = cfg['logging']['level']
        backup_days = int(cfg['logging']['backupDays'])
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
        log_file = os.path.join(log_dir, f'{file_name}.{str(datetime.datetime.now().strftime("%Y-%m-%d"))}')
        
        # 添加文件日志处理    
        if file_name != '':
            if not os.path.exists(log_dir):
                logger.info(f"Make logger dir [{str(log_dir)}]")
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

        logger.info(f"Logger File [{log_file}]")

        self._logger = logger
        return self._logger


    # 数据库中建表
    def exec_create_table_script(self, drop_exist):
        """
        执行 SQL 脚本
        :param drop_exist: 如果表存在是否先 Drop 后再重建
        :return:
        """
        table_sql_filepath = self.get_table_sql_filepath()
        table_exist = self.query_table_is_exist(self.table_name)

        if (drop_exist) or (not table_exist):
            cfg = self.get_cfg()
            logger = self.get_logger(self.table_name, cfg['logging']['filename'])
            db = self.get_mysql_connection()
            cursor = db.cursor()
            count = 0
            flt_cnt = 0
            suc_cnt = 0
            str1 = ''

            cursor.execute(f"DROP TABLE IF EXISTS {self.table_name};") 

            with open(table_sql_filepath, "r", encoding="utf-8") as file_object:
                for line in file_object:
                    if not line.startswith("--") and not line.startswith('/*'):  # 处理注释
                        str1 = str1 + ' ' + ' '.join(line.strip().split())  # pymysql一次只能执行一条sql语句


            for commandSQL in str1.split(';'):
                command = commandSQL.strip()
                if command != '':
                    try:
                        logger.info(f'Execute SQL [{command.strip()}]')
                        cursor.execute(command.strip() + ';')
                        count = count + 1
                        suc_cnt = suc_cnt + 1
                    except db.DatabaseError as e:
                        logger.error(e)
                        logger.error(command)
                        flt_cnt = flt_cnt + 1
                        pass
            logger.info(f'Execute result: Total [{count}], Succeed [{suc_cnt}] , Failed [{flt_cnt}] ')
            cursor.close()
            if flt_cnt > 0:
                raise Exception(f'Execute SQL script [{table_sql_filepath}] failed. ')


    def query_table_is_exist(self, table_name):
        sql = f"SELECT count(1) from information_schema.TABLES t WHERE t.TABLE_NAME ='{table_name}'"
        conn = self.get_mysql_connection()
        cursor = conn.cursor()
        cursor.execute(sql + ';')
        count = cursor.fetchall()[0][0]
        if int(count) > 0:
            return True
        else:
            return False


    def query_last_sync_date(self, sql):
        """
        查询历史同步数据的最大日期
        :param sql: 执行查询的SQL
        :return: 查询结果
        """
        logger = self.get_logger("utils", 'data_syn.log')
        conn = self.get_mysql_connection()
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
        if date1 <= date2:
            return date1
        else:
            return date2


    def max_date(self, date1, date2):
        if date1 >= date2:
            return date1
        else:
            return date2


    def get_ts_code_list(self, ts_code_limit):
        """
        获取 ts_code 列表
        :return:  股票代码列表 Series
        """
        # 创建 API / Connection / Logger 对象
        ts_api = self.get_tushare_api()
        logger = self.get_logger("utils", 'data_syn.log')

        result = pd.Series(data=None, index=None, name=None, dtype=str)
        ts_code_offset = 0  # 读取偏移量
        while True:
            logger.info(f"Query ts_code from tushare with api[stock_basic] from ts_code_offset[{ts_code_offset}] ts_code_limit[{ts_code_limit}]")
            df_ts_code = ts_api.stock_basic(**{
                "limit": 1000,
                "offset": ts_code_offset
            }, fields=[
                "ts_code"
            ])
            time.sleep(self.interval)
            if df_ts_code.last_valid_index() is not None:
                ts_code = df_ts_code['ts_code']
                logger.info(f"Query ts_code from tushare with api[stock_basic] from ts_code_offset[{ts_code_offset}] ts_code_limit[{ts_code_limit}]: Result[{ts_code.str.cat(sep=',')}]")
                result = pd.concat([result, ts_code], axis=0)
            else:
                break
            ts_code_offset = ts_code_offset + df_ts_code.last_valid_index() + 1
        return result


    def exec_sync_with_ts_code(self, start_date, end_date, date_step, ts_code_limit):
        # 创建 API / Connection / Logger 对象
        ts_api = self.get_tushare_api()
        logger = self.get_logger(self.table_name, 'data_syn.log')

        ts_codes = self.get_ts_code_list()
        cfg = self.get_cfg()
        database_name = cfg['mysql']['database']

        max_retry = 3
        cur_retry = 0
        while cur_retry < max_retry:
            try:
                # 清理历史数据
                clean_sql = f"DELETE FROM {database_name}.{self.table_name} WHERE {self.date_column}>='{start_date}' AND {self.date_column}<='{end_date}'"
                logger.info(f'Execute Clean SQL [{clean_sql}]')
                counts = self.exec_mysql_sql(clean_sql)
                logger.info(f"Execute Clean SQL Affect [{counts}] records")

                logger.info(f"Sync table[{self.table_name}] in ts_code mode start_date[{start_date}] end_date[{end_date}]")

                start = datetime.datetime.strptime(start_date, '%Y%m%d')
                end = datetime.datetime.strptime(end_date, '%Y%m%d')
                step_start = start  # 微批开始时间
                step_end = self.min_date(start + datetime.timedelta(date_step - 1), end)  # 微批结束时间

                while step_start <= end:
                    start_date = str(step_start.strftime('%Y%m%d'))
                    end_date = str(step_end.strftime('%Y%m%d'))
                    offset = 0

                    ts_code_start = 0
                    while ts_code_start < ts_codes.size:
                        ts_code_end = min(ts_code_start + ts_code_limit, ts_codes.size)
                        ts_code = ts_codes[ts_code_start:ts_code_end].str.cat(sep=',')
                        while True:
                            logger.info(
                                f"Query [{self.table_name}] from tushare with api[{self.api_name}] start_date[{start_date}] end_date[{end_date}] "
                                f"ts_code_start[{ts_code_start}] ts_code_end[{ts_code_end}] ts_code[{ts_code}]"
                                f" from offset[{offset}] limit[{limit}]")

                            data = ts_api.query(self.api_name,
                                                **{
                                                    "ts_code": ts_code,
                                                    "start_date": start_date,
                                                    "end_date": end_date,
                                                    "offset": offset,
                                                    "limit": self.limit
                                                },
                                                fields=self.fields)
                            time.sleep(self.interval)
                            if data.last_valid_index() is not None:
                                size = data.last_valid_index() + 1
                                logger.info(
                                    f'Write [{size}] records into table [{self.table_name}]')
                                self.save_datafame_to_db(data)
                                offset = offset + size
                                if size < self.limit:
                                    break
                            else:
                                break
                        ts_code_start = ts_code_end

                    # 更新下一次微批时间段
                    step_start = step_start + datetime.timedelta(date_step)
                    step_end = self.min_date(step_end + datetime.timedelta(date_step), end)
                break
            except Exception as e:
                if cur_retry < max_retry:
                    cur_retry += 1
                    logger.error("Get Exception[%s]" % e.__cause__)
                    time.sleep(self.interval)
                    continue
                else:
                    raise e

    # fields 字段列表
    #
    def exec_sync_with_spec_date_column(self, start_date, end_date):
        """
        执行数据同步并存储-基于 trade_date 字段
        :param start_date: 开始时间
        :param end_date: 结束时间
        :return: None
        """

        # 创建 API / Connection / Logger 对象
        ts_api = self.get_tushare_api()
        connection = self.get_mock_connection()
        logger = self.get_logger(self.table_name, 'data_syn.log')

        cfg = self.get_cfg()
        database_name = cfg['mysql']['database']

        max_retry = 3
        cur_retry = 0
        while cur_retry < max_retry:
            try:
                # 清理历史数据
                clean_sql = "DELETE FROM %s.%s WHERE %s>='%s' AND %s<='%s'" % \
                            (database_name, self.table_name, self.date_column, start_date, self.date_column, end_date)
                logger.info('Execute Clean SQL [%s]' % clean_sql)
                counts = self.exec_mysql_sql(clean_sql)
                logger.info("Execute Clean SQL Affect [%d] records" % counts)

                # 数据同步时间开始时间和结束时间, 包含前后边界
                start = datetime.datetime.strptime(start_date, '%Y%m%d')
                end = datetime.datetime.strptime(end_date, '%Y%m%d')

                step = start  # 微批开始时间

                while step <= end:
                    step_date = str(step.strftime('%Y%m%d'))
                    offset = 0
                    while True:
                        logger.info("Query [%s] from tushare with api[%s] %s[%s]"
                                    " from offset[%d] limit[%d]" % (
                                        self.table_name, self.api_name, self.date_column, step_date, offset, self.limit))
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
                            logger.info(f"Write [{size}] records into table [{self.table_name}]")
                            
                            # data.to_sql(self.table_name, connection, index=False, if_exists='append', chunksize=self.limit)
                            self.save_datafame_to_db(data)

                            offset = offset + size
                            if size < self.limit:
                                break
                        else:
                            break
                    # 更新下一次微批时间段
                    step = step + datetime.timedelta(days=1)
                break
            except Exception as e:
                if cur_retry < max_retry:
                    cur_retry += 1
                    logger.error("Get Exception[%s]" % e.__cause__)
                    time.sleep(3)
                else:
                    raise e

    def exec_sync_with_spec_date_column_v2(self, start_date, end_date, date_step=1):
        """
        执行数据同步并存储-基于 trade_date 字段
        :param date_step: Step
        :param start_date: 开始时间
        :param end_date: 结束时间
        :return: None
        """

        # 创建 API / Connection / Logger 对象
        ts_api = self.get_tushare_api()
        connection = self.get_mock_connection()
        logger = self.get_logger(self.table_name, 'data_syn.log')

        cfg = self.get_cfg()
        database_name = cfg['mysql']['database']

        max_retry = 3
        cur_retry = 0
        while True:
            try:
                # 清理历史数据
                clean_sql = f"DELETE FROM {database_name}.{self.table_name} WHERE {self.date_column}>='{start_date}' AND {self.date_column}<='{end_date}'"
                logger.info(f'Execute Clean SQL [{clean_sql}]')
                counts = self.exec_mysql_sql(clean_sql)
                logger.info(f"Execute Clean SQL Affect [{counts}] records")

                # 数据同步时间开始时间和结束时间, 包含前后边界
                start = datetime.datetime.strptime(start_date, '%Y%m%d')
                end = datetime.datetime.strptime(end_date, '%Y%m%d')

                step_start = start  # 微批开始时间

                while step_start <= end:
                    step_end = min(step_start + datetime.timedelta(days=date_step), end)
                    step_start_str = str(step_start.strftime('%Y%m%d'))
                    step_end_str = str(step_end.strftime('%Y%m%d'))
                    offset = 0
                    while True:
                        logger.info(f"Query [{self.table_name}] from tushare with api[{self.api_name}] {self.date_column}[{step_start_str}-{step_end_str}]"
                                    f" from offset[{offset}] limit[{self.limit}]")
                        data = ts_api.query(self.api_name,
                                            **{
                                                "start_date": step_start_str,
                                                "end_date": step_end_str,
                                                "offset": offset,
                                                "limit": self.limit
                                            },
                                            fields=self.fields)
                        time.sleep(self.interval)
                        if data.last_valid_index() is not None:
                            size = data.last_valid_index() + 1
                            logger.info(f'Write [{size}] records into table [{self.table_name}]')

                            # data.to_sql(self.table_name, connection, index=False, if_exists='append', chunksize=self.limit)
                            self.save_datafame_to_db(data)

                            offset = offset + size
                            if size < self.limit:
                                break
                        else:
                            break
                    # 更新下一次微批时间段
                    step_start = step_end + datetime.timedelta(days=1)
                break
            except Exception as e:
                if cur_retry < max_retry:
                    cur_retry += 1
                    logger.error("Get Exception[%s]" % e.__cause__)
                    time.sleep(self.interval)
                    continue
                else:
                    raise e

#     def exec_sync(self, start_date, end_date):
#         exec_sync_with_spec_date_column(
#         table_name='daily',
#         api_name='daily',
#         fields=[
#             "ts_code",
#             "trade_date",
#             "open",
#             "high",
#             "low",
#             "close",
#             "pre_close",
#             "change",
#             "pct_chg",
#             "vol",
#             "amount"
#         ],
#         date_column='trade_date',
#         start_date=start_date,
#         end_date=end_date,
#         limit=5000,
#         interval=0.3)


    # 全量初始化表数据
    def sync(self):
        pass

    def full_sync(self):
        pass

    def full_sync(self, drop_exist=True):
        # 创建表
        
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist)

        # 查询历史最大同步日期
        cfg = get_cfg()
        date_query_sql = "select max(trade_date) date from %s.daily" % cfg['mysql']['database']
        last_date = query_last_sync_date(date_query_sql)
        start_date = max_date(last_date, CONST_BEGIN_DATE)
        end_date = str(datetime.datetime.now().strftime('%Y%m%d'))

        self.exec_sync_with_spec_date_column_v2(start_date, end_date)


if __name__ == '__main__':
    pass
    # ts_codes_1 = get_ts_code_list(0.3, 1000)
    #print(ts_codes_1[0:10].str.cat(sep=','))
