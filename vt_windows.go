//go:build windows

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// enableConsoleAnsi 在 Windows 上控制台（conhost/cmd）默认不解释 ANSI 转义序列，
// 不开启虚拟终端处理（VT）时，进度渲染的 \033[H / \033[2K / \033[J 会以文本形式
// 原样打印出来，导致整屏画面反复堆叠。此函数开启 VT 支持后修复该问题。
// 渲染库（无论是自研还是 mpb/pb 等第三方）都用同一套 ANSI 机制，故必须先开启。
func enableConsoleAnsi() {
	handles := []windows.Handle{
		windows.Handle(os.Stdout.Fd()),
		windows.Handle(os.Stderr.Fd()),
	}
	for _, h := range handles {
		var mode uint32
		if err := windows.GetConsoleMode(h, &mode); err != nil {
			continue
		}
		_ = windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING)
	}
}

// terminalIsTTY 判断 stdout 是否是控制台终端。
func terminalIsTTY() bool {
	var mode uint32
	return windows.GetConsoleMode(windows.Handle(os.Stdout.Fd()), &mode) == nil
}

// consoleWidth 返回控制台当前宽度（列数），查询失败时返回默认宽度。
func consoleWidth() int {
	var bi windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(os.Stdout.Fd()), &bi); err != nil {
		return renderColWidth
	}
	w := int(bi.Window.Right - bi.Window.Left + 1)
	if w <= 0 {
		return renderColWidth
	}
	return w
}
