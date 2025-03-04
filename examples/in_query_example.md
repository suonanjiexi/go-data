# IN 查询示例

本文档展示了如何在Go-Data数据库中使用IN查询。

## 创建测试表和数据

```sql
-- 创建用户表
CREATE TABLE users (id, name, age, city)

-- 插入测试数据
INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 25, 'Beijing')
INSERT INTO users (id, name, age, city) VALUES (2, 'Bob', 30, 'Shanghai')
INSERT INTO users (id, name, age, city) VALUES (3, 'Charlie', 35, 'Guangzhou')
INSERT INTO users (id, name, age, city) VALUES (4, 'David', 28, 'Beijing')
```

## IN 查询示例

1. 查询特定城市的用户：
```sql
-- 查询在北京或上海的用户
SELECT * FROM users WHERE city IN ('Beijing', 'Shanghai')
```

2. 查询特定ID的用户：
```sql
-- 查询ID为1、3的用户
SELECT * FROM users WHERE id IN (1, 3)
```

3. 结合其他条件：
```sql
-- 查询在北京或上海，且年龄大于25的用户
SELECT * FROM users WHERE city IN ('Beijing', 'Shanghai') AND age > 25
```

## 注意事项

1. IN查询支持多个值的匹配，值之间用逗号分隔
2. IN查询的值列表必须用括号包围
3. 字符串值需要用单引号或双引号包围
4. 数字值可以直接使用，无需引号
5. IN查询可以与其他条件（如AND、OR）组合使用