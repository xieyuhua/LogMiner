
package signal

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"go.uber.org/zap"
)

// 处理退出信号量
func SetupSignalHandler(shutdownFunc func()) {
	usrDefSignalChan := make(chan os.Signal, 1)

	signal.Notify(usrDefSignalChan, syscall.SIGUSR1)
	go func() {
		buf := make([]byte, 1<<16)
		for {
			sig := <-usrDefSignalChan
			if sig == syscall.SIGUSR1 {
				stackLen := runtime.Stack(buf, true)
				zap.L().Info(" dump goroutine stack", zap.String("stack", fmt.Sprintf("=== Got signal [%s] to dump goroutine stack. ===\n%s\n=== Finished dumping goroutine stack. ===", sig, buf[:stackLen])))
			}
		}
	}()

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
