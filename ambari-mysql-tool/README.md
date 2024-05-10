# 传入参数
1. FE的ip:port ,如  ： bigdata-01:9030
2. 执行的SQL语句，有空格需要用双引号包起来
3. 用户名
4. 密码，无密码可以不填

# 使用样例
TODO 使用方法未更新
java -jar ambari-mysql-tool.jar bigdata-01:9030 "ALTER SYSTEM ADD BACKEND \"172.16.24.190:9050\";" root

# 正式使用版本
在当前项目下的release目录
