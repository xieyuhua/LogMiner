package server

import (
	"context"
	"gominerlog/config"
)

// 程序运行
func Run(ctx context.Context, cfg *config.Config) error {
	// 全量 + 增量数据同步阶段 - logminer
	err := IMigrateIncr(ctx, cfg)

	if err != nil {
		return err
	}
	return nil
}
