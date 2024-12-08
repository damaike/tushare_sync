"""
数据同步类，用于将 Tushare 数据同步到本地数据库

主要功能:
1. 提供配置信息加载
2. 提供数据库连接对象创建
3. 提供 Tushare API 对象创建
4. 支持全量同步和增量同步
5. 支持从 SQL 文件中读取表结构和配置

属性:
    table_name (str): 数据表名
    api_name (str): Tushare API 名称，默认与表名相同
    date_column (str): 日期字段名，默认为 'trade_date'
    fields (list): 需要同步的字段列表
    extra_params (dict): 调用 Tushare API 的额外参数
    is_increasing (bool): 数据是否递增，默认为 True
    end_date (str): 同步截止日期，默认为当天
    limit (int): 每次从 Tushare 获取的数据量限制
    interval (float): 每次调用 API 的间隔时间(秒)

配置说明:
1. SQL 文件中可以通过注释定义以下配置:
   - api_name: Tushare API 名称，默认和表名相同
   - date_column: 默认值trade_date；日期字段名，调用 tushare API 时，会使用该字段作为日期参数
   - extra_params: 默认空；传给 tushare API 的额外参数
   - is_increasing: 是否递增表， 默认 True；递增表可以使用 increamental_sync 函数，非递增表只能使用 full_sync 函数
   - end_date: 结束日期
   - limit: 每次从 Tushare 获取的数据量限制
   - interval: 每次调用 API 的间隔时间(秒)

使用示例:
    # 全量同步
    sync = TushareSync("daily")
    sync.full_sync()
    
    # 增量同步
    sync = TushareSync("daily")
    sync.incremental_sync()
    
    # 更新基础数据
    sync = TushareSync("stock_basic")
    sync.update()
"""

"""
日志：
- 信息
    - 程序开始/结束时，打印时间
    - 同步一天数据后，显示日期，写入记录数
    - create table 时，打印创建的表名
    - drop table 时，打印删除的表名
- 错误
    - 连接数据库失败时，打印错误信息
    - 从TuShare API 获取数据失败时，打印错误信息
"""

"""
TODO:
2. 大数据量考虑使用 LOAD LOCAL INFILE 导入
"""

import configparser
import os, time, datetime
import logging

import sqlalchemy
import tushare as ts


class TushareSync:
    
    _CFG_FILENAME = 'application.ini'

    _BEGIN_DATE = '20100101' # 同步数据最早开始日期
    _LIMIT = 50000 # 每次从tushare同步数据量
    _INTERVAL = 0.5 # 每次同步数据间隔时间
    _MAX_RETRY = 3 # 最大重试次数
    _DEFAULT_DATE_COLUMN = "trade_date"

    class Flags:
        API_NAME = "api_name"
        DATE_COLUMN = "date_column"
        END_DATE = "end_date"
        EXTRA_PARAMS = "extra_params"
        IS_INCREASING = "is_increasing"
        LIMIT = "limit"
        INTERVAL = "interval"

    _FLAG_KEYS = [Flags.API_NAME, Flags.DATE_COLUMN, Flags.END_DATE, Flags.EXTRA_PARAMS, Flags.IS_INCREASING, Flags.LIMIT, Flags.INTERVAL]


    def __init__(self, table_name, limit=0):
        self.limit = limit if limit>0 else TushareSync._LIMIT
        self.interval = TushareSync._INTERVAL
        
        self._init_env()
        self.table_name = table_name
        self.end_date = self.today()

        # temp_api_name = api_name
        # if not temp_api_name:
        #     temp_api_name = table_name
        # self.set_setting(temp_api_name, date_column, end_date, extra_params)
        # 初始化 flags 字典,用于存储同步相关参数

        # 本来想用 flags 字典存储同步相关参数，但后来发现写起来很繁琐，作罢
        self.api_name = ""
        self.date_column = TushareSync._DEFAULT_DATE_COLUMN
        self.end_date = ""
        self.extra_params = {}
        self.is_increasing = True

        sql_data = self._extract_data_from_sql_script()
        self.fields = sql_data["fields"]
        # 参数优先级高于sql脚本中的定义
        if TushareSync.Flags.DATE_COLUMN in sql_data:
            self.date_column = sql_data[TushareSync.Flags.DATE_COLUMN]
        if TushareSync.Flags.API_NAME in sql_data:
            self.api_name = sql_data[TushareSync.Flags.API_NAME]
        else:
            self.api_name = self.table_name
        if TushareSync.Flags.END_DATE in sql_data:
            self.end_date = sql_data[TushareSync.Flags.END_DATE]
        if TushareSync.Flags.LIMIT in sql_data:
            self.limit = int(sql_data[TushareSync.Flags.LIMIT])
        if TushareSync.Flags.INTERVAL in sql_data:
            self.interval = float(sql_data[TushareSync.Flags.INTERVAL])

        if TushareSync.Flags.EXTRA_PARAMS in sql_data:
            try:
                self.extra_params = eval(sql_data[TushareSync.Flags.EXTRA_PARAMS])
            except Exception as e:
                pass

        if TushareSync.Flags.IS_INCREASING in sql_data:
            self.is_increasing = False if sql_data[TushareSync.Flags.IS_INCREASING].lower() =="false" else True

        if not self.end_date:
            self.end_date = self.today()
    

    # 初始化环境变量
    def _init_env(self):
        self._cfg = None
        self._tushare_api = None
        self._logger = None
        # self._mysql_conn = None
        self._sqlalchemy_db_engine = None

        self.fields = []
        self.table_name, self.api_name = "", ""
        self.date_column = TushareSync._DEFAULT_DATE_COLUMN
        self.extra_params = {}
        self.is_increasing = True
        self.end_date = ""

    # 设置同步相关参数
    def set_setting(self, api_name="", date_column="trade_date", end_date="", extra_params={}):
        if api_name:
            self.api_name = api_name
        if date_column:
            self.date_column = date_column
        if extra_params:
            self.extra_params = extra_params
        if end_date:
            self.end_date = end_date

    def __del__(self):
        if self._sqlalchemy_db_engine:
            self._sqlalchemy_db_engine.dispose()

    def pre_process_data(self, data):
        """
        对从tushare API 获取的数据进行预处理
        默认不进行处理
        子类可重写该方法

        Args:
            data: 从tushare API 获取的数据

        Returns:
            预处理后的数据
        """
        return data
    
    def before_sync(self):
        """
        同步前，进行一些操作
        默认无操作
        子类可重写该方法
        """
        pass

    def after_sync(self):
        """
        同步后，进行一些操作
        默认无操作
        子类可重写该方法
        """
        pass

    @property
    def BEGIN_DATE(self):
        return self._BEGIN_DATE 
    
    def get_cfg(self):
        # 加载配置信息函数
        # TODO: 检查必须配置是否存在，否则抛出异常
        # TODO: 改为单例模式

        if self._cfg is None:
            cfg = configparser.ConfigParser()
            cfg.read(os.path.join(os.getcwd(), self._CFG_FILENAME))
            self._cfg = cfg
        
        return self._cfg

    # 获取 SQLAlchemy Connection 对象
    def get_db_engine(self):
        if not self._sqlalchemy_db_engine:
            cfg = self.get_cfg()
            db_host = cfg['mysql']['host']
            db_user = cfg['mysql']['user']
            db_password = cfg['mysql']['password']
            db_port = cfg['mysql']['port']
            db_database = cfg['mysql']['database']
            db_url = f'mysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?charset=utf8&use_unicode=1'
            self._sqlalchemy_db_engine = sqlalchemy.create_engine(db_url)
        
        return self._sqlalchemy_db_engine

    # # 获取 pymysql Connection 对象
    # def get_pymysql_conn(self):
    #     if self._mysql_conn is None:
    #         cfg = self.get_cfg()
    #         self._mysql_conn = pymysql.connect(host=cfg['mysql']['host'],
    #                         port=int(cfg['mysql']['port']),
    #                         user=cfg['mysql']['user'],
    #                         passwd=cfg['mysql']['password'],
    #                         db=cfg['mysql']['database'],
    #                         charset='utf8')
            
    #     return self._mysql_conn
    

    def _extract_data_from_sql_script(self, sql_script=""):
        """
        从建表sql脚本中获取 fields, api_name, date_column

        Args:
            sql_script: 建表sql脚本; 默认是从 table_filepath 中读取
        Returns:
            dict: 一定包含 fields, 
                可能包涵 api_name, date_column, extra_params 的键值；在 TushareSync._FLAG_KEYS 中定义
        """
        ret = {}

        if not sql_script:
            sql_script = self._read_table_sql()
        # 按行分割SQL语句
        lines = sql_script.split('\n')
        columns, api_name, date_column = [], "", ""
        
        # 遍历每一行
        for line in lines:
            line = line.strip()

            for key in TushareSync._FLAG_KEYS:
                key_tag1 = f'-- {key}:'
                key_tag2 = f'--{key}:'
                if line.startswith(key_tag1) or line.startswith(key_tag2):
                    ret[key] = line.split(':', 1)[1].strip()
                    continue

            # 跳过空行和非字段定义行
            if not line or line.startswith(('CREATE', 'DROP', ')', 'ENGINE', 'UNIQUE', '/*!', 'PARTITION', '--')):
               continue

            #if '`' in line:
            if line.startswith('`'):
                # 提取字段名
                column_name = line.split('`')[1]  # 获取第一对反引号中的内容
                if column_name=="created_time" or column_name=="updated_time":
                    continue
                columns.append(column_name)
        
        ret["fields"] = columns
        return ret

    def _read_table_sql(self):
        """
        获取建表SQL语句
        """
        table_sql = ""
        try:
            with open(self.table_filepath(), 'r', encoding='utf-8') as f:
                table_sql = f.read()
        except Exception as e:
            print(f"读取SQL文件 <{self.table_filepath()}> 失败: {e}")
        return table_sql

    def _table_exist(self, table_name=""):
        """
        检查表是否存在
        """
        if table_name=="":
            table_name = self.table_name
            
        return self._fetch_one_from_db(
            f"SELECT COUNT(1) FROM information_schema.TABLES WHERE TABLE_NAME='{table_name}'"
            ) > 0
    
    # def table_exist(self, table_name) -> bool:
    #     try:
    #         sql = f"desc {table_name};"
    #         result = self.exec_sql(sql)
    #         return True 
    #     except Exception as e:
    #         return False

    
    
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


    # 执行 SQL 语句，返回 最后一条语句的 result
    def exec_sql(self, sql):
        clean_sql = self._clean_sql(sql)

        result = None

        # 执行多条语句
        with self.get_db_engine().connect() as conn:
            for row in clean_sql.split(';'):
                if row.strip() != '':
                    result = conn.execute(sqlalchemy.text(row))
            conn.commit()
        return result
    
    # 执行 SQL 语句，返回结果中的第一个值
    def _fetch_one_from_db(self, sql):
        result = self.exec_sql(sql)
        if result:
            return result.fetchone()[0]
        else:
            return None

    # 将tushare dataframe 数据写入数据库, 使用 SQLAlchemy
    def save_datafame_to_db(self, data, if_exists="append"):
        db_engine = self.get_db_engine()
        if db_engine:
            data.to_sql(self.table_name, db_engine, index=False, if_exists=if_exists, chunksize=self.limit)
        else:
            raise Exception("创建 sqlalchemy conn 失败.")

    # 构建 Tushare 查询 API 接口对象
    def get_tushare_api(self):
        if self._tushare_api is None:
            cfg = self.get_cfg()
            token = cfg['tushare']['token']
            self._tushare_api = ts.pro_api(token=token, timeout=300)
        
        return self._tushare_api

    def query_tushare_oneday(self, date, ts_code="", offset=0, extra_params={}, sleep=True):
        return self.query_tushare_period(date, date, ts_code=ts_code, offset=offset, extra_params=extra_params, sleep=sleep)
    
        # """
        # 执行tushare API 函数
        # 自动休眠 interval 秒，防止对tushare API 的频繁调用
        # """
        # try:
        #     ts_api = self.get_tushare_api()
        #     data = ts_api.query(self.api_name,
        #                         **{
        #                             self.date_column: date,
        #                             "start_date": date,
        #                             "end_date": date,
        #                             "offset": offset,
        #                             "limit": self.limit
        #                         },
        #                         fields=self.fields)
        #     if sleep:
        #         time.sleep(self.interval)
        #     return data
        # except Exception as e:
        #     return None

    
    def query_tushare_period(self, start_date, end_date, ts_code="", offset=0, extra_params={}, sleep=True):
        """
        执行tushare API 函数
        自动休眠 interval 秒，防止对tushare API 的频繁调用
        注意：
            1. ts_code为空时, 只能同步单个日期数据，不能同步时间段数据
            2. 周线数据在周五，月线数据在月末最后一个交易日
        """
        if start_date!=end_date and not ts_code:
            raise Exception("当 start_date 和 end_date 不同时，ts_code 不能为空")
        
        ts_api = self.get_tushare_api()
        params = {
            # self.date_column: start_date,
            # "start_date": start_date,
            # "end_date": end_date,
            "offset": offset,
            "limit": self.limit
        }

        if start_date==end_date: # 单个日期
            params.update({self.date_column: start_date})
        else: # 时间段
            params.update({"start_date": start_date, "end_date": end_date})

        if ts_code:
            params["ts_code"] = ts_code

        if extra_params:
            params.update(extra_params)

        try:
            data = ts_api.query(self.api_name, **params, fields=self.fields)

            if sleep:
                time.sleep(self.interval)
                
            return data
        except Exception as e:
            return None
        
    def _test_tushare(self, api_name, params, fields="", offset=0, limit=0):
        if not fields:
            fields = self.fields
        if limit==0:
            limit = self.limit
            
        ts_api = self.get_tushare_api()
        return ts_api.query(api_name, **params, fields=fields, offset=offset, limit=limit)

    
    # def log_info(self, msg):
    #     self.get_logger().info(msg)

    # def log_warning(self, msg):
    #     self.get_logger().warning(msg)

    # def log_error(self, msg):
    #     self.get_logger().error(msg)

    def get_logger(self):
        """
        获取日志打印对象

        Returns:
            logger: 日志打印对象
        """
        if not self._logger:
            logger, log_file = self.create_logger()
            logger.info(f"日志文件： [{log_file}]")
            self._logger = logger
            
        return self._logger

    def create_logger(self):
        """
        创建日志打印对象

        Returns:
            (logger, log_file): (日志打印对象，日志文件路径)
        """
        cfg = self.get_cfg()
            
        log_name = self.table_name
        logger = logging.getLogger(log_name)
        formatter = logging.Formatter(
                # fmt='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s [%(filename)s:%(lineno)s]',
                fmt='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
                datefmt='%m-%d %H:%M:%S'
                )

            # 添加控制台日志处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

            # 添加文件日志处理器
        file_name = cfg['logging']['filename']
            
        backup_days = int(cfg['logging']['backupDays'])
        logger.setLevel(cfg['logging']['level'])
        log_dir = os.path.join(os.getcwd(), 'logs')
        log_file = os.path.join(log_dir, f'{file_name}.{self.today(without_dash=False)}')

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
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger,log_file

    # 获取 SQL 脚本存储文件夹
    def sql_folder(self):
        cfg = self.get_cfg()
        return cfg['mysql']['sql-folder']
    
    # 获取表 SQL 脚本文件绝对路径
    def table_filepath(self):
        sql_folder = self.sql_folder()
        return os.path.join(os.getcwd(), sql_folder, f'{self.table_name}.sql')

    
    def create_table(self, drop_exist=True):
        """
        数据库中建表
        :param drop_exist: 如果表存在是否先 Drop 后再重建
        :return:
        """

        if (drop_exist) or (not self._table_exist(self.table_name)):
            self.exec_sql(f"DROP TABLE IF EXISTS {self.table_name};") 

            table_sql_filepath = self.table_filepath()
            table_sql = ""
            with open(table_sql_filepath, "r", encoding="utf-8") as table_file:
                table_sql = table_file.read()

            self.exec_sql(table_sql)
            

    def query_last_sync_date(self, sql):
        """
        查询历史同步数据的最大日期
        :param sql: 执行查询的SQL
        :return: 查询结果
        """
        logger = self.get_logger()
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


    def min_date(self, date1, date2):
        """
        获取两个日期的最小值
        """
        return date1 if date1 <= date2 else date2


    def max_date(self, date1, date2):
        """
        获取两个日期的最大值
        """
        return date1 if date1 >= date2 else date2
    
    
    def sync_from_tushare_to_db(self, start_date, end_date) -> int:
        """
        将数据从tushare同步到数据库

        :param start_date: 开始时间
        :param end_date: 结束时间
        :return: 同步的记录数量
        流程:
        1. 清理历史数据
        2. 按日期循环从tushare抓取数据，并保存到数据库
        """

        total_count = 0

        try:
            # 清理历史数据
            self.exec_sql(f"DELETE FROM {self.table_name} WHERE {self.date_column}>='{start_date}' AND {self.date_column}<='{end_date}'")
            self.get_logger().info(f'清理数据: {start_date} ~ {end_date}')

            # 数据同步时间开始时间和结束时间, 包含前后边界
            start = self.str_to_date(start_date)
            end = self.str_to_date(end_date)

            date = start
            while date <= end:
                date_str = self.date_to_str(date)
                offset = 0
                
                while True:
                    # 从Tushare抓取数据，为防止网络失败，最多抓取 MAX_RETRY 次
                    # self.get_logger().info(f"开始 Tushare ��取数据: {date_str},{offset},{self.limit}")
                    tushare_data = None
                    retry = 0
                    while retry < self._MAX_RETRY:
                        tushare_data = self.query_tushare_oneday(date_str, offset=offset, extra_params=self.extra_params)
                        retry += 1
                        if tushare_data is not None:
                            break
                        else:
                            # 多休息一会
                            time.sleep(self.interval * 5)
                    if tushare_data is None:
                        self.get_logger().error(f"TuShare抓取数据失败, {date_str},{offset},{self.limit}")
                        break

                    rows_count = len(tushare_data)
                    if rows_count>0:
                        # 如果有数据，将数据写入到数据库中
                        # 一般非工作日没有数据
                        tushare_data = self.pre_process_data(tushare_data)
                        self.save_datafame_to_db(tushare_data)
                        # self.get_logger().info(f" 写入记录数: Write [{rows_count}] records into table [{self.table_name}]")

                        # 更新偏移量
                        offset = offset + rows_count
                        total_count += len(tushare_data)

                    # 如果数据量小于限制，说明已经当日数据已经取完，退出循环，抓取下一日
                    if rows_count < self.limit:
                        break
                        
                sync_details = f"导入: {offset}" if offset>0 else "无数据"
                sync_details = f"本日{sync_details}, 累计: {total_count}"
                
                self.get_logger().info(f"{date_str} {sync_details}")
                date += datetime.timedelta(days=1)

        except Exception as e:
            self.get_logger().error(f"异常错误: {str(e)}")
            
        return total_count

    def date_to_str(self, date, without_dash=True):
        if without_dash:
            return datetime.datetime.strftime(date, '%Y%m%d')
        else:
            return datetime.datetime.strftime(date, '%Y-%m-%d')
    
    def str_to_date(self, date_str, without_dash=True):
        if without_dash:
            return datetime.datetime.strptime(date_str, '%Y%m%d')
        else:
            return datetime.datetime.strptime(date_str, '%Y-%m-%d')

    def today(self, without_dash=True):
        return self.date_to_str(datetime.datetime.today(), without_dash)


    def update(self):
        """
        一次性更新全部数据，适合基础数据表
        1. 清理数据
        2. 从tushare 一次性抓取数据，存入数据库
        """
        self.get_logger().info(f"开始更新")

        total_count = 0

        self.create_table(drop_exist=True)
        tushare_data = self.query_tushare_oneday(self.today())
        self.save_datafame_to_db(tushare_data)
        total_count = len(tushare_data)
        
        self.get_logger().info(f"更新完成, 写入 [{total_count}] 条记录")
    
    def full_sync(self):
        """
        全量初始化表数据，适合每日有新数据的表，比如 daily, weekly, monthly, stk_factor_pro
        1. 清理历史数据
        2. 从 tushare 从BEGIN_DATE 到当前日期，按日期抓取数据，并保存到数据库
        """
        self.get_logger().info(f"开始全量同步")
        
        self.create_table(drop_exist=True)
        self.get_logger().info(f"数据库建表: [{self.table_name}]")
        
        end_date = self.today()
        total_count = self.sync_from_tushare_to_db(self.BEGIN_DATE, end_date)
        
        self.get_logger().info(f"全量同步完成, 写入 [{total_count}] 条记录")


    def incremental_sync(self):
        """
        增量同步数据，适合每日有新数据的表，比如 daily, weekly, monthly, stk_factor_pro
        1. 清理历史数据
        2. 从 tushare 从数据库的 last_sync_date 到当前日期，按日期抓取数据，并保存到数据库
        """

        self.get_logger().info(f"开始增量同步")

        # 查询历史最大同步日期
        last_date = self._fetch_one_from_db(f"select max({self.date_column}) from {self.table_name}")
        start_date = self.max_date(last_date, self.BEGIN_DATE)
        end_date = self.today()

        total_count = self.sync_from_tushare_to_db(start_date, end_date)
        self.get_logger().info(f"增量同步完成, 写入 [{total_count}] 条记录")

    def sync(self):
        """
        同步数据
        """
        self.before_sync()
        if self.is_increasing:
            if self._table_exist():
                self.incremental_sync()
            else:
                self.full_sync()
        else:
            self.update()

        self.after_sync()

