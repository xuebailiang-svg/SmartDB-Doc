import sys
print('Python path:', sys.path)
try:
    print('Attempting to import yasdb...')
    import yasdb
    print('yasdb imported successfully')
    print('YASDB_AVAILABLE = True')
    # 测试连接参数
    print('Testing yasdb.connect parameters...')
    help(yasdb.connect)
except Exception as e:
    print('Error:', type(e).__name__, ':', str(e))
    import traceback
    traceback.print_exc()
    print('YASDB_AVAILABLE = False')
