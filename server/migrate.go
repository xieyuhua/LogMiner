package server

import (
	"context"
	"gominerlog/common"
	"gominerlog/config"
	"gominerlog/module/migrate"
	"gominerlog/module/migrate/o2m"
	"strings"
)

func IMigrateIncr(ctx context.Context, cfg *config.Config) error {
	var (
		i   migrate.Increr
		err error
	)
	switch {
	case strings.EqualFold(cfg.DBTypeS, common.DatabaseTypeOracle) && strings.EqualFold(cfg.DBTypeT, common.DatabaseTypeMySQL):
		i, err = o2m.NewIncr(ctx, cfg)
		if err != nil {
			return err
		}
	}
	//全量 + 增量同步
	err = i.Incr()
	if err != nil {
		return err
	}
	return nil
}
