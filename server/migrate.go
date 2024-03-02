package server

import (
	"context"
	"gominerlog/common"
	"gominerlog/config"
	"gominerlog/module/migrate"
	"gominerlog/module/migrate/o2m"
	"strings"
)

//全量 + 增量同步
func IMigrateALLIncr(ctx context.Context, cfg *config.Config) error {
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
	err = i.FullIncr()
	if err != nil {
		return err
	}
	return nil
}

//全量
func IMigrateFull(ctx context.Context, cfg *config.Config) error {
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
	//全量同步
	err = i.Full()
	if err != nil {
		return err
	}
	return nil
}

//增量同步
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
	//增量同步
	err = i.Incr()
	if err != nil {
		return err
	}
	return nil
}
