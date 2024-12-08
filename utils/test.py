from tushare_sync import TushareSync

def test_get_fields(sync):
    print(sync._extract_fields_from_sql_script())

def test_table_exist(sync):
    print(sync._table_exist())

def test_create_table(sync):
    sync.create_table(True)

def test_fetch_one_from_db(sync):
    print(sync._fetch_one_from_db("select * from t"))
    
def test_exec_sql(sync):
    print(sync.exec_sql("insert into t values(999, 888)"))

def test_query_tushare_oneday(sync):
    print(sync.query_tushare_oneday("20241129"))

def test_query_tushare_period(sync):
    print(sync.query_tushare_period("20240101", "20241204", ts_code="000001.SZ"))

def test_query_tushare_oneday_with_tscode(sync):
    print(sync.query_tushare_oneday("20160105", ts_code="600036.SH"))
    
def test_query_tushare_period(sync):
    print(sync.query_tushare_period("20241101", "20241204"))

def test_query_tushare_period_with_tscode(sync):
    print(sync.query_tushare_period("20241101", "20241204", ts_code="000001.SZ,600036.SH"))

def test_save_dataframe_to_db(sync):
    sync.save_datafame_to_db(sync.query_tushare_oneday("20241204"))
    
def test_full_sync(sync):
    sync.full_sync()

def test_incremental_sync(sync):
    sync.incremental_sync()
    
def test_sql_config():
    sync = TushareSync("fund_nav")
    sync.sync()
    # print(sync.api_name, sync.date_column, sync.extra_params)

def test_index_daily():
    sync = TushareSync("index_daily")
    data = sync.query_tushare_oneday("20241204")
    print(len(data))
    print(data.head())
    print(data)

def test_sync_all_tables():
    # tables = ["stock_basic", "trade_cal", "daily", "fund_basic", "fund_nav", "stk_factor_pro", "adj_factor"]
    tables = ["fund_basic", "fund_nav", "stk_factor_pro", "adj_factor"]
    
    for table in tables:
        TushareSync(table).sync()


if __name__ == '__main__':
    # sync = TushareSync("daily", api_name="pro_bar", fields=["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"])
    # sync = TushareSync("stk_factor_pro")

    # test_get_fields(sync)
    # test_log(sync)
    # test_table_exist(sync)
    # test_create_table(sync)
    # test_fetch_one_from_db(sync)
    # test_exec_sql(sync)
    # test_query_tushare_oneday(sync)
    # test_query_tushare_oneday_with_tscode(sync)
    # test_query_tushare_period(sync)
    # test_query_tushare_period_with_tscode(sync)
    # test_save_dataframe_to_db(sync)
   
    # test_full_sync(sync)
    # test_incremental_sync(sync)

    # test_sql_config()
    # test_index_daily()

    print("hello")
    test_sync_all_tables()
