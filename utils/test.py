import sqlalchemy

def get_db_conn():
    host = "vm"
    user = "stock"
    password = "wwbwwb"
    database = "stock"
    
    # 构建连接URL,启用local_infile
    conn_url = f"mysql+pymysql://{user}:{password}@{host}/{database}?local_infile=1"
    
    # 创建engine时设置local_infile参数
    engine = sqlalchemy.create_engine(
        conn_url,
        connect_args={'local_infile': 1}
    )
        
    return engine


def test():
    # 获取数据库连接
    engine = get_db_conn()
    
    # LOAD DATA LOCAL INFILE 'd:/sqldata.csv'
    #  INTO TABLE t
    #  fields terminated by ','
    #  enclosed by '"'
    #  lines terminated by '\n'
    
    # 构建 LOAD DATA LOCAL INFILE SQL语句
    load_sql = """
        LOAD DATA LOCAL INFILE 'd:/sqldata.csv' 
        INTO TABLE t 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
    """
    
    # 执行SQL语句
    with engine.connect() as conn:
        result = conn.execute(sqlalchemy.text(load_sql))
        print(result)
        conn.commit()

test()
