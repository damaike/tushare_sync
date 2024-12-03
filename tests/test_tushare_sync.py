import unittest
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from utils.tushare_sync import TushareSync

class TestTushareSync(unittest.TestCase):
    def setUp(self):
        """测试前的准备工作"""
        self.sync = TushareSync("stock", "stock_basic", "trade_date")
        
    def test_init(self):
        """测试初始化"""
        self.assertEqual(self.sync.table_name, "stock_basic")
        self.assertEqual(self.sync.api_name, "stock_basic")
        self.assertEqual(self.sync.date_column, "trade_date")
        
    @patch('configparser.ConfigParser')
    def test_get_cfg(self, mock_configparser):
        """测试配置加载"""
        mock_config = MagicMock()
        mock_configparser.return_value = mock_config
        mock_config.read.return_value = None
        
        cfg = self.sync.get_cfg()
        self.assertIsNotNone(cfg)
        mock_config.read.assert_called_once()
        
    @patch('pandas.DataFrame')
    def test_save_datafame_to_db(self, mock_df):
        """测试数据保存到数据库"""
        mock_data = MagicMock()
        mock_df.return_value = mock_data
        
        with patch.object(self.sync, 'get_db_conn') as mock_conn:
            self.sync.save_datafame_to_db(mock_data)
            mock_data.to_sql.assert_called_once_with(
                'stock_basic',
                mock_conn.return_value,
                index=False,
                if_exists='append',
                chunksize=self.sync._LIMIT
            )
            
    def test_clean_sql(self):
        """测试SQL清理功能"""
        sql = "SELECT * FROM table; # 这是注释\nWHERE id > 0 # 另一个注释"
        expected = "SELECT * FROM table;\nWHERE id > 0"
        
        result = self.sync._clean_sql(sql)
        self.assertEqual(result.strip(), expected.strip())
        
    def test_min_max_date(self):
        """测试日期比较函数"""
        date1 = "20230101"
        date2 = "20230102"
        
        self.assertEqual(self.sync.min_date(date1, date2), date1)
        self.assertEqual(self.sync.max_date(date1, date2), date2)
        
    @patch('tushare.pro_api')
    def test_exec_tushare_func(self, mock_ts_api):
        """测试tushare API调用"""
        mock_api = MagicMock()
        mock_ts_api.return_value = mock_api
        
        with patch.object(self.sync, 'get_tushare_api', return_value=mock_api):
            self.sync.exec_tushare_func("20230101", 0, sleep=False)
            mock_api.query.assert_called_once()

if __name__ == '__main__':
    unittest.main() 