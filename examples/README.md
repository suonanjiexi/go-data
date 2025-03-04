# Go-Data 数据库使用指南

## 索引功能

### 索引操作

```sql
-- 创建普通索引
CREATE INDEX idx_city ON users (city)

-- 创建唯一索引
CREATE UNIQUE INDEX idx_id ON users (id)

-- 删除索引
DROP INDEX idx_city ON users
```

索引可以显著提高查询性能，特别是在大型数据集上。Go-Data支持两种类型的索引：
- 普通索引：允许索引列有重复值
- 唯一索引：确保索引列的值在表中是唯一的

## 事务管理

### 事务隔离级别

Go-Data支持四种标准的事务隔离级别：

1. 读未提交（Read Uncommitted）
2. 读已提交（Read Committed）
3. 可重复读（Repeatable Read，默认）
4. 串行化（Serializable）

### 事务操作示例

```sql
-- 开始事务
BEGIN;

-- 插入数据
INSERT INTO accounts (id, name, balance) VALUES (1, "张三", 1000);

-- 更新余额
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

-- 检查余额
SELECT balance FROM accounts WHERE id = 1;

-- 如果操作正确则提交事务
COMMIT;

-- 如果出现问题则回滚事务
ROLLBACK;
```

### 并发控制示例

以下示例展示了两个并发事务的操作：

事务1：
```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
SELECT balance FROM accounts WHERE id = 1;
COMMIT;
```

事务2：
```sql
BEGIN;
UPDATE accounts SET balance = balance + 50 WHERE id = 1;
SELECT balance FROM accounts WHERE id = 1;
COMMIT;
```

## 编译和运行服务器

首先，您需要编译并运行Go-Data数据库服务器：

```bash
# 进入项目目录
cd /path/to/go-data

# 编译服务器
go build -o godata ./cmd/godata

# 运行服务器（使用默认配置）
./godata

# 或者指定主机、端口和数据库路径
./godata -host 127.0.0.1 -port 3306 -db ./mydata
```

服务器默认配置：
- 主机：localhost
- 端口：3306
- 数据库文件路径：./data

## 使用客户端连接

### 方法1：使用示例客户端

项目提供了一个简单的客户端示例，您可以编译并运行它：

```bash
# 编译客户端
go build -o client ./examples/client.go

# 运行客户端（连接到默认地址）
./client

# 或者指定主机和端口
./client 127.0.0.1 3306

# 运行SQL测试示例模式
./client --test
# 或使用简写形式
./client -t
```

### 方法2：使用telnet或nc命令

您也可以使用telnet或nc等工具直接连接到服务器：

```bash
# 使用telnet
telnet localhost 3306

# 或使用nc
nc localhost 3306
```

## SQL命令示例

### 基本操作

```sql
-- 创建表
CREATE TABLE users (id, name, age, city)

-- 插入数据
INSERT INTO users (id, name, age, city) VALUES (1, "张三", 25, "北京")
INSERT INTO users (id, name, age, city) VALUES (2, "李四", 30, "上海")

-- 基本查询
SELECT * FROM users WHERE id = 1

-- 条件查询
SELECT * FROM users WHERE age > 25
```

### 高级查询

```sql
-- 模糊匹配
SELECT * FROM users WHERE name LIKE "%三%"

-- 多条件查询
SELECT * FROM users WHERE age > 25 AND city = "北京"

-- 排序
SELECT * FROM users ORDER BY age DESC

-- 分页
SELECT * FROM users LIMIT 10 OFFSET 0

-- 分组统计
SELECT city, COUNT(*) FROM users GROUP BY city

-- 聚合函数
SELECT MAX(age), MIN(age), AVG(age) FROM users
```

## 性能优化建议

1. 合理使用索引
   - 频繁查询的列建议创建索引
   - 避免在频繁更新的列上创建过多索引

2. 事务处理
   - 控制事务大小，避免长时间运行的事务
   - 在高并发场景下使用适当的隔离级别
   - 及时提交或回滚事务

3. 查询优化
   - 只查询需要的列，避免SELECT *
   - 使用合适的WHERE条件过滤数据
   - 适当使用LIMIT限制结果集大小

4. 并发控制
   - 避免长时间持有锁
   - 合理设置事务超时时间
   - 注意死锁问题
```

## 测试模式

使用测试模式可以快速体验Go-Data数据库的各项功能：

```bash
./client --test
```

测试模式会自动执行一系列SQL命令，展示数据库的基本功能，包括：
1. 创建表
2. 插入数据（支持ID自增）
3. 基本查询和条件查询
4. 模糊匹配查询（LIKE操作符）
5. 聚合函数查询（MAX、MIN、SUM、AVG）
6. 排序和分页
7. 更新和删除数据
8. 事务管理（开始、提交、回滚）
9. 索引性能测试
10. 清理测试数据

## 退出连接

输入`exit`命令可以退出客户端连接：

```
SQL> exit
```

## 注意事项

1. 服务器使用TCP协议通信，确保防火墙允许指定端口的连接
2. 数据库文件会保存在指定的路径中，确保该路径有写入权限
3. 使用Ctrl+C可以优雅地关闭服务器
4. 默认事务超时时间为30秒
5. 客户端连接超时时间为5分钟