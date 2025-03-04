package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// 执行SQL命令并打印结果
func executeSQL(conn net.Conn, serverReader *bufio.Reader, sql string) {
	fmt.Printf("SQL> %s\n", sql)

	// 发送命令到服务器
	fmt.Fprintf(conn, "%s\n", sql)

	// 读取服务器响应
	response, err := serverReader.ReadString('\n')
	if err != nil {
		fmt.Printf("读取服务器响应失败: %v\n", err)
		return
	}

	// 显示响应
	fmt.Print(response)

	// 等待一小段时间，让用户能看清结果
	time.Sleep(500 * time.Millisecond)
}

// 运行SQL测试示例
func runSQLTestExamples(conn net.Conn, serverReader *bufio.Reader) {
	fmt.Println("\n===== 开始执行SQL测试示例 =====")
	fmt.Println("这个示例将展示Go-Data数据库的基本功能，包括创建表、插入数据、查询、更新、删除操作以及事务和并发控制。\n")

	// 1. 创建表
	fmt.Println("1. 创建用户表和账户表")
	executeSQL(conn, serverReader, "CREATE TABLE users (id, name, age, city)")
	executeSQL(conn, serverReader, "CREATE TABLE accounts (id, name, balance)")

	// 2. 插入数据
	fmt.Println("\n2. 插入用户数据")
	executeSQL(conn, serverReader, "INSERT INTO users (id, name, age, city) VALUES (1, \"张三\", 25, \"北京\")")
	executeSQL(conn, serverReader, "INSERT INTO users (id, name, age, city) VALUES (2, \"李四\", 30, \"上海\")")
	executeSQL(conn, serverReader, "INSERT INTO users (id, name, age, city) VALUES (3, \"王五\", 28, \"广州\")")
	executeSQL(conn, serverReader, "INSERT INTO users (id, name, age, city) VALUES (4, \"赵六\", 35, \"北京\")")

	// 3. 创建索引
	fmt.Println("\n3. 创建索引")
	fmt.Println("3.1 创建普通索引")
	executeSQL(conn, serverReader, "CREATE INDEX idx_city ON users (city)")

	fmt.Println("\n3.2 创建唯一索引")
	executeSQL(conn, serverReader, "CREATE UNIQUE INDEX idx_id ON users (id)")

	// 4. 基本查询
	fmt.Println("\n4. 基本查询操作")
	fmt.Println("4.1 查询单条记录")
	executeSQL(conn, serverReader, "SELECT * FROM users WHERE id = 1")

	fmt.Println("\n4.2 条件查询")
	executeSQL(conn, serverReader, "SELECT * FROM users WHERE age > 25")

	fmt.Println("\n4.3 多条件查询")
	executeSQL(conn, serverReader, "SELECT * FROM users WHERE age > 20 AND city = \"北京\"")

	// 5. 事务测试
	fmt.Println("\n5. 事务操作测试")
	fmt.Println("5.1 正常事务")
	executeSQL(conn, serverReader, "BEGIN;")
	executeSQL(conn, serverReader, "INSERT INTO accounts (id, name, balance) VALUES (1, \"张三\", 1000);")
	executeSQL(conn, serverReader, "UPDATE accounts SET balance = balance - 100 WHERE id = 1;")
	executeSQL(conn, serverReader, "SELECT balance FROM accounts WHERE id = 1;")
	executeSQL(conn, serverReader, "COMMIT;")

	fmt.Println("\n5.2 事务回滚")
	executeSQL(conn, serverReader, "BEGIN;")
	executeSQL(conn, serverReader, "UPDATE accounts SET balance = balance - 2000 WHERE id = 1;")
	executeSQL(conn, serverReader, "SELECT balance FROM accounts WHERE id = 1;")
	executeSQL(conn, serverReader, "ROLLBACK;")
	executeSQL(conn, serverReader, "SELECT balance FROM accounts WHERE id = 1;")

	// 6. 并发控制测试
	fmt.Println("\n6. 并发控制测试")
	fmt.Println("6.1 模拟并发事务")
	executeSQL(conn, serverReader, "BEGIN;")
	executeSQL(conn, serverReader, "UPDATE accounts SET balance = balance - 50 WHERE id = 1;")
	executeSQL(conn, serverReader, "SELECT balance FROM accounts WHERE id = 1;")
	executeSQL(conn, serverReader, "COMMIT;")

	// 7. 高级查询功能
	fmt.Println("\n7. 高级查询功能")
	fmt.Println("7.1 模糊匹配查询")
	executeSQL(conn, serverReader, "SELECT * FROM users WHERE name LIKE \"%三%\"")

	fmt.Println("\n7.2 聚合函数查询")
	executeSQL(conn, serverReader, "SELECT MAX(age) FROM users")
	executeSQL(conn, serverReader, "SELECT MIN(age) FROM users")
	executeSQL(conn, serverReader, "SELECT AVG(age) FROM users")
	executeSQL(conn, serverReader, "SELECT SUM(age) FROM users")

	fmt.Println("\n7.3 分组查询")
	executeSQL(conn, serverReader, "SELECT city, COUNT(*) FROM users GROUP BY city")

	fmt.Println("\n===== SQL测试示例执行完成 =====")
}

func main() {
	// 解析命令行参数
	host := "localhost"
	port := "3306"
	testMode := false

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "--test" || arg == "-t" {
			testMode = true
		} else if i == 1 && !strings.HasPrefix(arg, "-") {
			host = arg
		} else if i == 2 && !strings.HasPrefix(arg, "-") {
			port = arg
		}
	}

	// 连接到服务器
	addr := fmt.Sprintf("%s:%s", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("连接服务器失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 创建服务器响应读取器
	serverReader := bufio.NewReader(conn)

	// 读取欢迎消息
	welcome, err := serverReader.ReadString('\n')
	if err != nil {
		fmt.Printf("读取欢迎消息失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(welcome)

	// 如果是测试模式，运行测试示例
	if testMode {
		runSQLTestExamples(conn, serverReader)
		return
	}

	// 交互式模式
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("SQL> ")
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("读取输入失败: %v\n", err)
			continue
		}

		cmd = strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		if strings.ToLower(cmd) == "exit" {
			fmt.Println("再见！")
			break
		}

		// 发送命令到服务器
		fmt.Fprintf(conn, "%s\n", cmd)

		// 读取服务器响应
		response, err := serverReader.ReadString('\n')
		if err != nil {
			fmt.Printf("读取服务器响应失败: %v\n", err)
			continue
		}

		// 显示响应
		fmt.Print(response)
	}
}
