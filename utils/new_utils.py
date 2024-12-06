"""
各种小工具函数
"""

def convert_table_desc_to_sql(table_desc: str) -> str:
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
        sql_type = 'varchar(16)' if field_type == 'str' else 'double' if field_type == 'float' else field_type
        
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
