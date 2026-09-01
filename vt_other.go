//go:build !windows

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// enableConsoleAnsi 在非 Windows 平台为空操作，Un*x 终端默认解释 ANSI 序列。
func enableConsoleAnsi() {}

// terminalIsTTY 判断 stdout 是否是终端。
func terminalIsTTY() bool {
	info, err := os.Stdout.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// consoleWidth 返回终端宽度（列数），查询失败时返回默认宽度。
func consoleWidth() int {
	ws, err := unix.IoctlGetWinsize(int(os.Stdout.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws == nil || ws.Col == 0 {
		return renderColWidth
	}
	return int(ws.Col)
}
