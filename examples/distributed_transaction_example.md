# 分布式事务示例

本文档展示了如何在Cyber-DB数据库中使用分布式事务功能。分布式事务采用两阶段提交（2PC）协议来保证跨节点事务的一致性。

## 功能特点

- 两阶段提交协议（2PC）实现
- 自动故障恢复机制
- 事务状态追踪
- 超时自动回滚
- 分布式一致性保证

## 准备工作

启动多个数据库节点：
```bash
# 启动节点1（主节点）
./godata -host 127.0.0.1 -port 3306 -node-id node1

# 启动节点2
./godata -host 127.0.0.1 -port 3307 -node-id node2

# 启动节点3
./godata -host 127.0.0.1 -port 3308 -node-id node3
```

## 创建测试表

在各个节点上创建相同的表结构：
```sql
-- 创建账户表
CREATE TABLE accounts (
    id VARCHAR(50),
    balance DECIMAL(10,2),
    PRIMARY KEY (id)
)

-- 创建转账记录表
CREATE TABLE transfers (
    id VARCHAR(50),
    from_account VARCHAR(50),
    to_account VARCHAR(50),
    amount DECIMAL(10,2),
    status VARCHAR(20),
    create_time TIMESTAMP,
    PRIMARY KEY (id)
)
```

## 分布式事务示例

### 1. 转账事务

```go
// 开始分布式事务
txID, err := tc.Begin([]string{"node1", "node2"}, 30*time.Second)
if err != nil {
    log.Fatal("开始事务失败:", err)
}

// 准备阶段
err = tc.Prepare(txID)
if err != nil {
    log.Fatal("事务准备失败:", err)
}

// 执行转账操作
// 在node1上扣款
executeSQL(conn1, "UPDATE accounts SET balance = balance - 100 WHERE id = 'A'")
// 在node2上增加余额
executeSQL(conn2, "UPDATE accounts SET balance = balance + 100 WHERE id = 'B'")

// 提交事务
err = tc.Commit(txID)
if err != nil {
    // 如果提交失败，执行回滚
    tc.Abort(txID)
    log.Fatal("事务提交失败:", err)
}
```

### 2. 事务超时处理

```go
// 设置较短的超时时间
txID, err := tc.Begin([]string{"node1", "node2"}, 5*time.Second)

// 模拟长时间操作
time.Sleep(6 * time.Second)

// 准备阶段会失败
err = tc.Prepare(txID)
if err != nil {
    log.Println("事务超时:", err)
    // 执行清理操作
    tc.Cleanup()
}
```

### 3. 节点故障处理

```go
// 开始分布式事务
txID, err := tc.Begin([]string{"node1", "node2", "node3"}, 30*time.Second)

// 准备阶段
err = tc.Prepare(txID)

// 模拟节点故障
// 如果在提交过程中有节点故障，事务协调器会处理故障恢复
err = tc.Commit(txID)
if err != nil {
    log.Println("提交失败，可能是节点故障:", err)
    // 等待节点恢复后重试或执行回滚
    tc.Abort(txID)
}
```

## 注意事项

1. 分布式事务需要确保所有参与节点都可用
2. 合理设置事务超时时间，避免长时间占用资源
3. 实现适当的错误处理和重试机制
4. 保持事务粒度适中，避免大事务
5. 考虑节点故障的恢复机制
6. 定期清理已完成的事务记录