"""
各种小工具函数
"""

def convert_table_desc_to_sql(table_desc: str, varchar_length=64) -> str:
    """将表描述文本转换为建表SQL脚本
    
    Args:
        table_desc: 表描述文本，第一行为"表名 表说明"，后面为字段描述，每行用制表符分割为3列
        
    Returns:
        str: 建表SQL脚本
    """
    lines = table_desc.strip().split('\n')
    
    # 解析第一行的表名和表说明
    first_line = lines[0].strip()
    sep = ',' if ',' in first_line else None
    table_name, table_comment = first_line.split(sep, 1)
    table_name, table_comment = table_name.strip(), table_comment.strip()
    
    # 构建字段定义
    fields = []
    for line in lines[1:]:
        if not line.strip():  # 跳过空行
            continue
            
        parts = line.split('\t')
        if len(parts) < 3:
            continue
            
        field_name = parts[0].strip()
        field_type = parts[1].strip().lower()
        field_comment = parts[2].strip()
        
        # 类型转换
        sql_type = f'varchar({varchar_length})' if field_type == 'str' else 'double' if field_type == 'float' else field_type
        
        # 构建字段定义SQL
        field_sql = f"`{field_name}` {sql_type} COMMENT '{field_comment}'"
        fields.append(field_sql)
    
    # 添加created_time和updated_time字段
    fields.extend([
        "`created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'",
        "`updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'"
    ])
    
    # 构建完整的建表SQL
    sql = f"""DROP TABLE IF EXISTS `{table_name}`;
CREATE TABLE `{table_name}` (
    {',\n    '.join(fields)}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_comment}';
"""
    return sql

def get_weekday(date_str: str) -> int:
    """计算指定日期的星期数，
    
    Args:
        date_str: 日期字符串，格式为'YYYYMMDD'
        
    Returns:
        int: 星期数（1-7，1表示星期一，7表示星期日）
    """
    from datetime import datetime
    date = datetime.strptime(date_str, '%Y%m%d')
    # datetime的weekday()返回1-7，1表示星期一
    return date.weekday()+1

def get_week_of_year(date_str: str) -> int:
    """计算指定日期是当年的第几周
    
    Args:
        date_str: 日期字符串，格式为'YYYYMMDD'
        
    Returns:
        int: 周数（1-53）
    """
    from datetime import datetime
    date = datetime.strptime(date_str, '%Y%m%d')
    # isocalendar()返回一个元组(year, week_number, weekday)
    # week_number表示当年的第几周
    return date.isocalendar()[1]

# ... existing code ...

def get_week_trade_cal(daily_trade_cal: list) -> list:
    """将日交易日历转换为周交易日历
    
    Args:
        daily_trade_cal: 日交易日历列表，每个元素为dict，包含cal_date和is_open字段
        
    Returns:
        list: 周交易日历列表，每个元素为dict，包含week_start_date和week_end_date字段
    """
    if not daily_trade_cal:
        return []
        
    # 按日期排序
    daily_trade_cal = sorted(daily_trade_cal, key=lambda x: x['cal_date'])
    
    week_cal = []
    current_week = {'week_start_date': "", 'week_end_date': ""}
    
    last_week_day = 0
    for day in daily_trade_cal:
        if day['is_open'] != 1:  # 跳过非交易日
            continue
            
        date_str = day['cal_date']
        if not current_week['week_start_date']:
            current_week['week_start_date'] = date_str
            
        # 检查是否是周五或最后一个交易日
        if date_str[-2:] == '05' or day == daily_trade_cal[-1]:
            current_week['week_end_date'] = date_str
            week_cal.append(current_week.copy())
            current_week = {'week_start_date': None, 'week_end_date': None}
            
    return week_cal

def get_month_trade_cal(daily_trade_cal: list) -> list:
    """将日交易日历转换为月交易日历
    
    Args:
        daily_trade_cal: 日交易日历列表，每个元素为dict，包含cal_date和is_open字段
        
    Returns:
        list: 月交易日历列表，每个元素为dict，包含month_start_date和month_end_date字段
    """
    if not daily_trade_cal:
        return []
        
    # 按日期排序
    daily_trade_cal = sorted(daily_trade_cal, key=lambda x: x['cal_date'])
    
    month_cal = []
    current_month = {'month_start_date': None, 'month_end_date': None}
    current_month_str = None
    
    for day in daily_trade_cal:
        if day['is_open'] != 1:  # 跳过非交易日
            continue
            
        date_str = day['cal_date']
        month_str = date_str[:6]  # 获取年月部分
        
        if month_str != current_month_str:
            if current_month['month_start_date']:
                # 保存上一个月的数据
                month_cal.append(current_month.copy())
            current_month = {'month_start_date': date_str, 'month_end_date': None}
            current_month_str = month_str
            
        current_month['month_end_date'] = date_str
        
    # 添加最后一个月
    if current_month['month_start_date']:
        month_cal.append(current_month)
        
    return month_cal
