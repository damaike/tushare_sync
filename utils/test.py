from tushare_sync import TushareSync

def test_index_basic():
    sync = TushareSync("index_basic")
    sync.full_sync()

test_index_basic()
