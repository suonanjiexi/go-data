package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/suonanjiexi/go-data/pkg/server"
	"github.com/suonanjiexi/go-data/pkg/storage"
)

func main() {
	// 解析命令行参数
	host := flag.String("host", "localhost", "服务器主机地址")
	port := flag.Int("port", 3306, "服务器端口")
	dbPath := flag.String("db", "./data", "数据库文件路径")
	flag.Parse()

	// 确保数据库目录存在
	dbDir := filepath.Dir(*dbPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Fatalf("无法创建数据库目录: %v\n", err)
	}

	// 创建并打开数据库
	db := storage.NewDB(*dbPath, true) // 启用持久化存储
	// 数据库已默认设置为使用utf8mb4字符集
	if err := db.Open(); err != nil {
		log.Fatalf("无法打开数据库: %v\n", err)
	}
	defer db.Close()

	// 创建并启动服务器
	srv := server.NewServer(*host, *port, db)
	if err := srv.Start(); err != nil {
		log.Fatalf("无法启动服务器: %v\n", err)
	}

	fmt.Printf("Go-Data 数据库服务器已启动，监听 %s:%d\n", *host, *port)
	fmt.Println("按 Ctrl+C 停止服务器")

	// 等待中断信号以优雅地关闭服务器
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("正在关闭服务器...")
	if err := srv.Stop(); err != nil {
		log.Printf("关闭服务器时出错: %v\n", err)
	}

	fmt.Println("服务器已关闭")
}
