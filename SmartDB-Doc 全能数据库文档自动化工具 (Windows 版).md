# SmartDB-Doc 全能数据库文档自动化工具 (Windows 版)

SmartDB-Doc 是一个轻量级的数据库文档自动化生成工具。它利用 Streamlit 构建交互式 Web UI，通过 SQLAlchemy 连接多种主流数据库，提取元数据，并结合大型语言模型（LLM）对表和字段的业务含义进行智能解析。

**本版本支持 Oracle, MySQL, SQL Server, PostgreSQL，并具备样本数据采样功能。**

## 功能特性

-   **多数据库支持**: 
    -   **Oracle**: 支持 11g, 12c, 19c 等版本（需安装 Instant Client）。
    -   **MySQL**: 支持 5.7, 8.0+。
    -   **PostgreSQL**: 支持 10.0+。
    -   **SQL Server**: 支持 2012+（需安装 ODBC Driver）。
-   **元数据提取**: 自动读取表名、表备注、字段名、字段类型、主外键关系、是否为空、字段注释等。
-   **样本数据采样 (核心功能)**: 可自动抓取每张表的前 5 行数据，辅助 AI 更精准地推断业务含义。
-   **范围筛选**: 支持分析全库对象、指定 Schema/用户、或指定特定表。
-   **AI 增强解析**: 对接 OpenAI 兼容接口，利用 LLM 推断“中文业务含义”及“字段业务解释”。
-   **Mermaid ER 图**: 自动生成数据库的实体关系图 (ERD) 代码。
-   **文档导出**: 支持导出为 `.md` 和 `.docx` 格式。

## 技术栈

-   **Python**: 3.10+
-   **Web UI**: Streamlit
-   **数据库驱动**: oracledb, pymysql, psycopg2, pyodbc
-   **LLM 接口**: OpenAI Python Client
-   **文档处理**: python-docx

## 快速开始

### 1. 环境准备 (Windows)

确保您的系统已安装 Python 3.10+。

### 2. 数据库驱动配置 (重要！)

根据您要连接的数据库，可能需要安装额外的系统级驱动：

-   **Oracle**: 
    -   下载并安装 [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client/downloads.html)。
    -   将安装路径添加到系统 `PATH` 环境变量中。
-   **SQL Server**: 
    -   下载并安装 [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/zh-cn/sql/connect/odbc/download-odbc-driver-for-sql-server)。
-   **MySQL / PostgreSQL**: 
    -   通常只需通过 `pip` 安装 Python 库即可（已包含在 `start.bat` 中）。

### 3. 启动应用

1.  将项目文件下载到本地文件夹。
2.  双击运行 `start.bat`。它会自动安装依赖并启动 Web 界面。
3.  浏览器将自动打开 `http://localhost:8501`。

## 使用说明

1.  **连接配置**: 在侧边栏选择数据库类型，输入连接参数。
2.  **采样配置**: 勾选“启用样本数据采样”，AI 将参考真实数据内容进行推断，解析更准确。
3.  **AI 配置**: 输入您的 API Key 和 Base URL。
4.  **执行解析**: 点击连接并提取元数据，然后在“AI 增强解析”选项卡中点击“开始批量解析”。
5.  **导出文档**: 在“文档导出”选项卡中预览并下载文档。

---

**Manus AI** 生成
