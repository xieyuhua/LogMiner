package o2m

import (
	"context"
	"fmt"
	"gominerlog/common"
	"gominerlog/config"
	"gominerlog/database/oracle"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type Migrate struct {
	Ctx         context.Context
	Cfg         *config.Config
	Oracle      *oracle.Oracle
	OracleMiner *oracle.Oracle
}

var SourceTableSCN uint64

func NewIncr(ctx context.Context, cfg *config.Config) (*Migrate, error) {
	oracleDB, err := oracle.NewOracleDBEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}

	oracleMiner, err := oracle.NewOracleLogminerEngine(ctx, cfg.OracleConfig)
	if err != nil {
		return nil, err
	}
	firstSCN, maxSCN, LOG_FILE, err := oracleMiner.GetOracleCurrentRedoMaxSCN()
	fmt.Println(firstSCN)
	fmt.Println(maxSCN)
	fmt.Println(LOG_FILE)
	SourceTableSCN = 0
	return &Migrate{
		Ctx:         ctx,
		Cfg:         cfg,
		Oracle:      oracleDB,
		OracleMiner: oracleMiner,
	}, nil
}

func (r *Migrate) Incr() error {

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using transferdb tools", oraDBVersion)
	}
	fmt.Println("###########全量###############")

	// 获取 oracle 所有数据表
	exporters, err := filterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}
	fmt.Println(exporters)
	for _, tableName := range exporters {
		cols, res, err := r.Oracle.GetOracleTableRowsData(`select * from `+tableName, 10)
		if err != nil {
			return err
		}
		fmt.Println(cols)
		fmt.Println(res)
	}

	fmt.Println("############增量##############")

	// 增量数据同步
	for range time.Tick(300 * time.Millisecond) {
		if err = r.syncTableIncrRecord(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Migrate) syncTableIncrRecord() error {

	// 获取增量所需得日志文件
	logFiles, err := r.getTableIncrRecordLogfile()
	if err != nil {
		return err
	}
	// fmt.Println(logFiles)
	zap.L().Info("increment table log file get",
		zap.String("logfile", fmt.Sprintf("%v", logFiles)))
	// 遍历所有日志文件
	for _, log := range logFiles {
		// 获取日志文件起始 SCN
		logFileStartSCN, err := common.StrconvUintBitSize(log["FIRST_CHANGE"], 64)
		if err != nil {
			return fmt.Errorf("get oracle log file start scn %s utils.StrconvUintBitSize failed: %v", log["FIRST_CHANGE"], err)
		}

		// 获取日志文件结束 SCN
		logFileEndSCN, err := common.StrconvUintBitSize(log["NEXT_CHANGE"], 64)
		if err != nil {
			return fmt.Errorf("get oracle log file end scn %s utils.StrconvUintBitSize failed: %v", log["NEXT_CHANGE"], err)
		}
		//是否读取过
		if SourceTableSCN < logFileStartSCN {
			SourceTableSCN = logFileStartSCN
			fmt.Println(99999999)
		}

		zap.L().Info("increment table log file logminer",
			zap.String("logfile", log["LOG_FILE"]),
			zap.Uint64("logfile start scn", logFileStartSCN),
			zap.Uint64("logminer start scn", logFileStartSCN),
			zap.Uint64("logfile end scn", logFileEndSCN))

		// logminer 运行
		if err = r.OracleMiner.AddOracleLogminerlogFile(log["LOG_FILE"]); err != nil {
			return err
		}

		//GetOracleIncrRecord
		if err = r.OracleMiner.StartOracleLogminerStoredProcedure(log["FIRST_CHANGE"]); err != nil {
			return err
		}

		if err != nil {
			return err
		}
		exporters, err := filterCFGTable(r.Cfg, r.Oracle)
		if err != nil {
			return err
		}
		// // 捕获数据
		rowsResult, MaxSCN, _ := r.OracleMiner.GetOracleIncrRecord(common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			common.StringArrayToCapitalChar(exporters),
			strconv.FormatUint(SourceTableSCN, 10),
			r.Cfg.AllConfig.LogminerQueryTimeout)
		//更新到最大
		SourceTableSCN = MaxSCN
		if len(rowsResult) > 0 {
			fmt.Println(logFileStartSCN)
			fmt.Println(MaxSCN)
			fmt.Println(rowsResult)
		}

		if err != nil {
			return err
		}
		zap.L().Info("increment table log extractor", zap.String("logfile", log["LOG_FILE"]),
			zap.Uint64("logfile start scn", logFileStartSCN),
			zap.Uint64("source table last scn", logFileEndSCN),
			zap.Int("row counts", len(rowsResult)))

		// logminer 关闭
		if err = r.OracleMiner.EndOracleLogminerStoredProcedure(); err != nil {
			return err
		}

		// redoLogList, err := r.OracleMiner.GetOracleALLRedoLogFile()
		// if err != nil {
		// 	return err
		// }
		// fmt.Println(redoLogList)
		continue
	}
	return nil
}

func (r *Migrate) getTableIncrRecordLogfile() ([]map[string]string, error) {
	var logFiles []map[string]string
	firstSCN, _, _, err := r.OracleMiner.GetOracleCurrentRedoMaxSCN()
	// fmt.Println(firstSCN)
	// 获取增量表起始最小 SCN 号
	strGlobalSCN := strconv.FormatUint(firstSCN, 10)

	// 判断数据是在 archived log Or redo log
	// 如果 redoSCN 等于 0，说明数据在归档日志
	redoScn, err := r.OracleMiner.GetOracleRedoLogSCN(strGlobalSCN)
	if err != nil {
		return logFiles, err
	}

	archivedScn, err := r.OracleMiner.GetOracleArchivedLogSCN(strGlobalSCN)
	if err != nil {
		return logFiles, err
	}

	// 获取所需挖掘的日志文件
	if redoScn == 0 {
		strArchivedSCN := strconv.FormatUint(archivedScn, 10)
		logFiles, err = r.OracleMiner.GetOracleArchivedLogFile(strArchivedSCN)
		if err != nil {
			return logFiles, err
		}

	} else {
		strRedoCN := strconv.FormatUint(redoScn, 10)
		logFiles, err = r.OracleMiner.GetOracleRedoLogFile(strRedoCN)
		if err != nil {
			return logFiles, err
		}
	}

	return logFiles, nil
}
