# tushare-data-to-mysql

将 Tushare 数据平台的数据导入到 MySQL 数据库中的自动化脚本

### 环境依赖

- [python-3.8.10](https://www.python.org/downloads/release/python-3810/)
- [Tushare 数据接口](https://tushare.pro/)
- MySQL 5+
- python 依赖包
  - pandas
  - sqlalchemy
  - mysqlclient

### 使用方法

- 安装并运行 MySQL 数据库，提前建立所需的 database。
- 将脚本中的数据库用户名、密码、数据库名称等信息，以及 Tushare 平台的 token 进行替换。
- 按照注释选择脚本中需要运行的方法，点击运行。