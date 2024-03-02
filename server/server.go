/*
 * @Author: "xieyuhua" "1130
 * @Date: 2023-04-21 15:12:33
 * @LastEditors: "xieyuhua" "1130
 * @LastEditTime: 2024-03-02 15:14:24
 * @FilePath: \LogMiner\server\server.go
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
package server

import (
	"context"
	"fmt"
	"gominerlog/common"
	"gominerlog/config"
	"strings"
)

// 程序运行
func Run(ctx context.Context, cfg *config.Config) error {

	switch strings.ToUpper(strings.TrimSpace(cfg.TaskMode)) {
	case common.TaskModeINCR:
		// 增量数据同步阶段
		err := IMigrateIncr(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeFull:
		// 全量数据 ETL 非一致性（基于某个时间点，而是直接基于现有 SCN）抽取，离线环境提供与原库一致性
		err := IMigrateFull(ctx, cfg)
		if err != nil {
			return err
		}
	case common.TaskModeAll:
		// 全量 + 增量数据同步阶段 - logminer
		err := IMigrateALLIncr(ctx, cfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("flag [mode] can not null or value configure error")
	}
	return nil
}
