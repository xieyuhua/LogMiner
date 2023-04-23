package signal

import (
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// 处理退出信号量
func SetupSignalHandler(shutdownFunc func()) {
	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-closeSignalChan
		zap.L().Info("got signal to exit", zap.Stringer("signal", sig))
		shutdownFunc()
	}()
}
