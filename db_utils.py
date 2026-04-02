import sqlalchemy
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
import pandas as pd
import oracledb
import os

# 尝试初始化 Oracle Client (Thick Mode)
try:
    oracledb.init_oracle_client()
    print("Oracle Thick Mode initialized successfully via PATH.")
except Exception as e:
    print(f"Oracle Client initialization info: {e}")

def get_engine(db_type, host, port, user, password, database):
    """
    创建多数据库引擎，适配 Oracle, MySQL, PostgreSQL, SQL Server
    """
    if db_type == "MySQL":
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
