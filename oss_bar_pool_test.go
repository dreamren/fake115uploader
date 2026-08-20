package main

import (
	"fmt"
	"testing"
	"time"
)

// 完整池渲染流程测试（需要 TTY，用 script 命令运行）：
// script -qec "stty cols 120 rows 40; ./barflow.test -test.run TestPoolRenderFlow -test.v" flow.log
func TestPoolRenderFlow(t *testing.T) {
	if !stdoutIsTTY {
		t.Skip("需要 TTY 环境（用 script -qec 运行）")
	}

	startBarPool()

	// worker 1：大文件 a.mkv，校验 → 上传 → 100% 定格
	tb1 := newTaskBar()
	tb1.beginPhase("校验 a.mkv", 5<<20)
	tb1.bar.Add64(2 << 20)
	time.Sleep(300 * time.Millisecond)

	tb1.beginUpload("a.mkv", 5<<20)
	tb1.bar.Add64(3 << 20)
	time.Sleep(300 * time.Millisecond)

	// 上传走满：无需 finish，自动定格 100%
	tb1.bar.Add64(2 << 20)
	time.Sleep(300 * time.Millisecond)

	// 小文件：不打扰进度条，也不产生新行
	tb1.beginUpload("tiny.txt", 100*1024)
	time.Sleep(200 * time.Millisecond)

	// worker 2：晚开始的大文件 b.mkv（worker 1 已定格后新加入，必须能正常渲染）
	tb2 := newTaskBar()
	tb2.beginPhase("校验 b.mkv", 8<<20)
	tb2.bar.Add64(2 << 20)
	time.Sleep(300 * time.Millisecond)

	tb2.beginUpload("b.mkv", 8<<20)
	tb2.bar.Add64(8 << 20)
	time.Sleep(300 * time.Millisecond)

	stopBarPool()
	fmt.Println("=== 测试结束 ===")
}

// 验证只处理小文件的 worker 不产生空行
func TestPoolNoEmptyLineForSmallFiles(t *testing.T) {
	if !stdoutIsTTY {
		t.Skip("需要 TTY 环境（用 script -qec 运行）")
	}

	startBarPool()

	// worker 1：先来一个大文件
	tb1 := newTaskBar()
	tb1.beginPhase("校验 a.mkv", 5<<20)
	tb1.bar.Add64(5 << 20)
	time.Sleep(300 * time.Millisecond)

	// worker 2：只处理小文件——它的 bar 永不加入池，不产生空行
	tb2 := newTaskBar()
	tb2.beginUpload("tiny1.txt", 100*1024)
	tb2.beginUpload("tiny2.txt", 200*1024)
	time.Sleep(300 * time.Millisecond)

	// 池里应该只有 tb1 一行
	if !tb1.joined() {
		t.Errorf("tb1 应已加入池")
	}
	if tb2.joined() {
		t.Errorf("只处理小文件的 tb2 不应加入池（会产生空行）")
	}

	stopBarPool()
	fmt.Println("=== 小文件测试结束 ===")
}
