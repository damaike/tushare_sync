def read_file_to_string(file_path: str) -> str:
    """
    读取文本文件内容到字符串
    
    Args:
        file_path: 文件路径
    
    Returns:
        文件内容字符串
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"找不到文件: {file_path}")
    except Exception as e:
        raise Exception(f"读取文件时发生错误: {str(e)}")

if __name__ == '__main__':
    pro = ts.pro_api()

    # 设置你的token
    df = pro.user(token='0727ee16a574094aefc773c03f505d48c692d8bb0dc22fbdf8b8038e')

    print(df)


