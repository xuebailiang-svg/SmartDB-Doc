import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
import pandas as pd
import oracledb
import os

# 尝试导入 yasdb 驱动
try:
    import yasdb
    YASDB_AVAILABLE = True
except ImportError:
    YASDB_AVAILABLE = False

# 尝试初始化 Oracle Client (Thick Mode)
try:
    oracledb.init_oracle_client()
    print("Oracle Thick Mode initialized successfully via PATH.")
except Exception as e:
    print(f"Oracle Client initialization info: {e}")

def get_engine(db_type, host, port, user, password, database):
    """
    创建多数据库引擎，适配 Oracle, MySQL, PostgreSQL, SQL Server, YashanDB
    """
    if db_type == "YashanDB" and YASDB_AVAILABLE:
        # 使用 yasdb 直接连接
        return {
            "type": "yasdb",
            "connection": {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "db": database
            }
        }
    elif db_type == "MySQL":
        # 使用 pymysql 驱动
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    elif db_type == "PostgreSQL":
        # 使用 psycopg2 驱动
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "Oracle":
        # 使用 oracledb 驱动
        url = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={database}"
    elif db_type == "SQL Server":
        # 使用 pyodbc 驱动，需安装 ODBC Driver 17/18
        connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
        url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
    engine = create_engine(
        url,
        pool_pre_ping=True
    )
    return engine

def get_sample_data(engine, table_name, schema=None, limit=5):
    """
    抓取不同数据库的前 N 行样本数据
    """
    # 处理 YashanDB 特殊情况
    if isinstance(engine, dict) and engine.get('type') == 'yasdb':
        conn_info = engine['connection']
        host = conn_info['host']
        port = conn_info['port']
        user = conn_info['user']
        password = conn_info['password']
        database = conn_info['database']
        
        if not schema:
            schema = 'public'
        
        try:
            conn = yasdb.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                db=database
            )
            cursor = conn.cursor()
            
            cursor.execute(f"SELECT * FROM {schema}.{table_name} LIMIT {limit}")
            rows = cursor.fetchall()
            
            # 获取列名
            column_names = [desc[0] for desc in cursor.description]
            
            # 构建样本数据
            sample_data = []
            for row in rows:
                sample_data.append(dict(zip(column_names, row)))
            
            cursor.close()
            conn.close()
            return sample_data
        except Exception as e:
            print(f"Failed to fetch sample data for {table_name}: {e}")
            return []
    
    # 其他数据库使用 SQLAlchemy
    db_type = engine.dialect.name
    
    # 处理表名转义
    if db_type == 'mysql':
        full_table_name = f'`{table_name}`'
    elif db_type == 'mssql':
        full_table_name = f'[{schema}].[{table_name}]' if schema else f'[{table_name}]'
    else:
        full_table_name = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
    
    # 根据数据库类型构造采样 SQL
    if db_type == 'oracle':
        query = f"SELECT * FROM (SELECT * FROM {full_table_name}) WHERE ROWNUM <= {limit}"
    elif db_type == 'mssql':
        query = f"SELECT TOP {limit} * FROM {full_table_name}"
    else: # MySQL, PostgreSQL
        query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            return df.to_dict(orient='records')
    except Exception as e:
        print(f"Failed to fetch sample data for {table_name}: {e}")
        return []

def get_schema_metadata(engine, scope_type="全库", target_schema=None, target_tables=None, enable_sampling=False):
    """
    提取数据库元数据，支持范围筛选和样本数据采样
    """
    # 处理 YashanDB 特殊情况
    if isinstance(engine, dict) and engine.get('type') == 'yasdb':
        return get_yashandb_metadata(engine, scope_type, target_schema, target_tables, enable_sampling)
    
    # 其他数据库使用 SQLAlchemy inspector
    inspector = inspect(engine)
    db_type = engine.dialect.name
    
    # 处理默认 Schema
    if not target_schema:
        if db_type == 'oracle':
            target_schema = engine.url.username.upper()
        elif db_type == 'postgresql':
            target_schema = 'public'
        elif db_type == 'mssql':
            target_schema = 'dbo'
        # MySQL 通常不需要指定 schema，因为连接时已指定 database
    
    # 获取表名列表
    if scope_type == "全库" or scope_type == "指定 Schema":
        table_names = inspector.get_table_names(schema=target_schema)
    elif scope_type == "指定表":
        if target_tables:
            requested_tables = [t.strip() for t in target_tables.split(',') if t.strip()]
            all_available = inspector.get_table_names(schema=target_schema)
            table_names = [t for t in requested_tables if t in all_available]
        else:
            table_names = []
    else:
        table_names = inspector.get_table_names(schema=target_schema)
    
    tables_metadata = []
    
    for table_name in table_names:
        try:
            table_comment = inspector.get_table_comment(table_name, schema=target_schema).get('text')
        except:
            table_comment = ""
            
        columns = inspector.get_columns(table_name, schema=target_schema)
        pk_constraint = inspector.get_pk_constraint(table_name, schema=target_schema)
        pk_columns = pk_constraint.get('constrained_columns', [])
        fk_constraints = inspector.get_foreign_keys(table_name, schema=target_schema)
        
        cols_metadata = []
        for col in columns:
            cols_metadata.append({
                "name": col['name'],
                "type": str(col['type']),
                "nullable": col['nullable'],
                "default": str(col.get('default', '')),
                "is_pk": col['name'] in pk_columns,
                "comment": col.get('comment', '')
            })
            
        sample_data = []
        if enable_sampling:
            sample_data = get_sample_data(engine, table_name, schema=target_schema)
            
        tables_metadata.append({
            "table_name": table_name,
            "table_comment": table_comment or "",
            "columns": cols_metadata,
            "foreign_keys": fk_constraints,
            "sample_data": sample_data
        })
        
    return tables_metadata

def get_yashandb_metadata(engine_config, scope_type="全库", target_schema=None, target_tables=None, enable_sampling=False):
    """
    使用 yasdb 直接获取 YashanDB 元数据
    """
    conn_info = engine_config['connection']
    host = conn_info['host']
    port = conn_info['port']
    user = conn_info['user']
    password = conn_info['password']
    database = conn_info['db']
    
    # 建立连接
    conn = yasdb.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        db=database
    )
    
    cursor = conn.cursor()
    tables_metadata = []
    
    try:
        # 处理默认 Schema
        if not target_schema:
            target_schema = 'public'
        
        # 获取表名列表
        if scope_type == "全库" or scope_type == "指定 Schema":
            # 查询指定 schema 下的所有表
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{target_schema}'")
            table_names = [row[0] for row in cursor.fetchall()]
        elif scope_type == "指定表":
            if target_tables:
                requested_tables = [t.strip() for t in target_tables.split(',') if t.strip()]
                # 查询指定表是否存在
                table_names = []
                for table in requested_tables:
                    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{target_schema}' AND table_name = '{table}'")
                    if cursor.fetchone():
                        table_names.append(table)
            else:
                table_names = []
        else:
            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{target_schema}'")
            table_names = [row[0] for row in cursor.fetchall()]
        
        # 遍历表获取详细信息
        for table_name in table_names:
            # 获取表注释
            cursor.execute(f"SELECT table_comment FROM information_schema.tables WHERE table_schema = '{target_schema}' AND table_name = '{table_name}'")
            table_comment_row = cursor.fetchone()
            table_comment = table_comment_row[0] if table_comment_row else ""
            
            # 获取列信息
            cursor.execute(f"SELECT column_name, data_type, is_nullable, column_default, column_comment FROM information_schema.columns WHERE table_schema = '{target_schema}' AND table_name = '{table_name}'")
            columns = cursor.fetchall()
            
            # 获取主键信息
            cursor.execute(f"SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = '{target_schema}' AND table_name = '{table_name}' AND constraint_name = (SELECT constraint_name FROM information_schema.table_constraints WHERE table_schema = '{target_schema}' AND table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY')")
            pk_columns = [row[0] for row in cursor.fetchall()]
            
            # 构建列元数据
            cols_metadata = []
            for col in columns:
                cols_metadata.append({
                    "name": col[0],
                    "type": col[1],
                    "nullable": col[2] == 'YES',
                    "default": col[3] or "",
                    "is_pk": col[0] in pk_columns,
                    "comment": col[4] or ""
                })
            
            # 获取外键信息
            fk_constraints = []
            cursor.execute(f"SELECT constraint_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name FROM information_schema.key_column_usage WHERE table_schema = '{target_schema}' AND table_name = '{table_name}' AND referenced_table_name IS NOT NULL")
            fk_rows = cursor.fetchall()
            
            # 按约束名分组
            fk_dict = {}
            for row in fk_rows:
                constraint_name = row[0]
                if constraint_name not in fk_dict:
                    fk_dict[constraint_name] = {
                        "name": constraint_name,
                        "constrained_columns": [],
                        "referred_schema": row[2],
                        "referred_table": row[3],
                        "referred_columns": []
                    }
                fk_dict[constraint_name]["constrained_columns"].append(row[1])
                fk_dict[constraint_name]["referred_columns"].append(row[4])
            
            fk_constraints = list(fk_dict.values())
            
            # 获取样本数据
            sample_data = []
            if enable_sampling:
                try:
                    cursor.execute(f"SELECT * FROM {target_schema}.{table_name} LIMIT 5")
                    rows = cursor.fetchall()
                    # 获取列名
                    column_names = [desc[0] for desc in cursor.description]
                    # 构建样本数据
                    for row in rows:
                        sample_data.append(dict(zip(column_names, row)))
                except Exception as e:
                    print(f"Failed to fetch sample data for {table_name}: {e}")
            
            # 添加表元数据
            tables_metadata.append({
                "table_name": table_name,
                "table_comment": table_comment or "",
                "columns": cols_metadata,
                "foreign_keys": fk_constraints,
                "sample_data": sample_data
            })
    finally:
        cursor.close()
        conn.close()
    
    return tables_metadata
