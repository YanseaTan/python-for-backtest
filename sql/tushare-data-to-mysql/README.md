# tushare-data-to-mysql

### 包含模块

- **TushareDataToMySQL**
  - 将 Tushare 数据平台的数据导入到 MySQL 数据库中的自动化脚本
- **FuturesMDToSpread**
  - 通过数据库中的期货合约日行情数据，计算所有期货合约日行情价差并存入数据库的自动化脚本
- **WriteSpreadDataToXlsx**
  - 统计分析数据库中的期货价差日行情数据，将例如组合合约最低价差数据导出到 Excel 表格文件中，方便其他人查看
- **UpdateSpreadConfig**
  - 通过数据库中的期货价差日行情数据，统计分析得到所有品种下所有跨月组合 10% 最低价差，并以此更新行情服务器所需的价差配置文件
- **DatabaseTools**
  - 包含与数据库交互的工具函数

### 环境依赖

- [python-3.8.10](https://www.python.org/downloads/release/python-3810/)
- [Tushare 数据接口](https://tushare.pro/)
- MySQL 5+
- python 依赖包
  - pandas
  - sqlalchemy
  - mysqlclient
  - xlwings

### 使用方法

- 安装并运行 MySQL 数据库，提前建立所需的 database。
- 将脚本中的数据库用户名、密码、数据库名称等信息，以及 Tushare 平台的 token 进行替换。
- 按照注释选择脚本中需要运行的方法，点击运行。