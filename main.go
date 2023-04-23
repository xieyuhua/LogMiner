/*
 * @Descripttion:
 * @version:
 * @Author: seaslog
 * @Date: 2023-04-21 15:10:42
 * @LastEditors: 谢余华
 * @LastEditTime: 2023-04-21 16:08:30
 */
package main

import (
	"context"
	"gominerlog/config"
	"gominerlog/logger"
	"gominerlog/server"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func main() {
	cfg := config.NewConfig()
	if err := cfg.Parse(os.Args[1:]); err != nil {
		log.Fatalf("start meta failed. error is [%s], Use '--help' for help.", err)
	}

	// 初始化日志 logger
	logger.NewZapLogger(cfg)
	config.RecordAppVersion("gominerlog", cfg)

	go func() {
		if err := http.ListenAndServe(cfg.AppConfig.PprofPort, nil); err != nil {
			zap.L().Fatal("listen and serve pprof failed", zap.Error(errors.Cause(err)))
		}
		os.Exit(0)
	}()

	// 信号量监听处理
	// signal.SetupSignalHandler(func() {
	// 	os.Exit(1)
	// })
	// 程序运行
	ctx := context.Background()
	if err := server.Run(ctx, cfg); err != nil {
		zap.L().Fatal("server run failed", zap.Error(errors.Cause(err)))
	}
}
