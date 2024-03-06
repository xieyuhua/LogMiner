package o2m

import (
	"context"
	"fmt"
	"gominerlog/common"
	"gominerlog/config"
	"gominerlog/database/oracle"
	"strconv"
	"strings"
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
	IncrTime := cfg.OracleConfig.IncrTime
	fmt.Println("firstSCN:", firstSCN, "maxSCN:", maxSCN, "IncrTime:", IncrTime, "LOG_FILE:", LOG_FILE)
	SourceTableSCN = 0
	return &Migrate{
		Ctx:         ctx,
		Cfg:         cfg,
		Oracle:      oracleDB,
		OracleMiner: oracleMiner,
	}, nil
}

//全量同步
func (r *Migrate) Full() error {

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using gominerlog tools", oraDBVersion)
	}
	fmt.Println("###########全量###############")

	// 获取 oracle 所有数据表
	exporters, err := filterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}
	fmt.Println(exporters)
	for _, tableName := range exporters {
		_, _, err := r.Oracle.GetOracleTableRowsData(`select * from `+tableName, 10)
		if err != nil {
			return err
		}
	}
	return nil
}

//增量同步
func (r *Migrate) Incr() error {
	var err error
	fmt.Println("############增量##############")
	// 增量数据同步
	IncrTime := r.Cfg.OracleConfig.IncrTime
	for range time.Tick(time.Duration(IncrTime) * time.Millisecond) {
		if err = r.syncTableIncrRecord(); err != nil {
			return err
		}
	}
	return nil
}

//全量同步 + //增量同步
func (r *Migrate) FullIncr() error {

	// 判断上游 Oracle 数据库版本
	// 需要 oracle 11g 及以上
	oraDBVersion, err := r.Oracle.GetOracleDBVersion()
	if err != nil {
		return err
	}
	if common.VersionOrdinal(oraDBVersion) < common.VersionOrdinal(common.RequireOracleDBVersion) {
		return fmt.Errorf("oracle db version [%v] is less than 11g, can't be using gominerlog tools", oraDBVersion)
	}
	fmt.Println("###########全量###############")

	// 获取 oracle 所有数据表
	exporters, err := filterCFGTable(r.Cfg, r.Oracle)
	if err != nil {
		return err
	}
	fmt.Println(exporters)
	for _, tableName := range exporters {
		_, _, err := r.Oracle.GetOracleTableRowsData(`select * from `+tableName, 10)
		if err != nil {
			return err
		}
	}

	fmt.Println("############增量##############")

	// 增量数据同步
	IncrTime := r.Cfg.OracleConfig.IncrTime
	for range time.Tick(time.Duration(IncrTime) * time.Millisecond) {
		if err = r.syncTableIncrRecord(); err != nil {
			return err
		}
	}
	return nil
}

//获取增量同步数据
func (r *Migrate) syncTableIncrRecord() error {

	// 获取增量所需得日志文件
	logFiles, err := r.getTableIncrRecordLogfile()
	if err != nil {
		return err
	}
	// fmt.Println(logFiles)
	// zap.L().Info("increment table log file get",
	// 	zap.String("logfile", fmt.Sprintf("%v", logFiles)))
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
		}

		// zap.L().Info("increment table log file logminer",
		// 	zap.String("logfile", log["LOG_FILE"]),
		// 	zap.Uint64("logfile start scn", logFileStartSCN),
		// 	zap.Uint64("logminer start scn", logFileStartSCN),
		// 	zap.Uint64("logfile end scn", logFileEndSCN))

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
		startTime := time.Now()
		rowsResult, MaxSCN, _ := r.OracleMiner.GetOracleIncrRecord(common.StringUPPER(r.Cfg.OracleConfig.SchemaName),
			common.StringArrayToCapitalChar(exporters),
			strconv.FormatUint(SourceTableSCN, 10),
			r.Cfg.AllConfig.LogminerQueryTimeout)
		endTime := time.Now()
		//更新到最大
		SourceTableSCN = MaxSCN
		if len(rowsResult) > 0 {
			// fmt.Println(logFileStartSCN)
			// fmt.Println(MaxSCN)
			for _, rs := range rowsResult {
				// fmt.Println(rs.SCN)
				// fmt.Println(rs.SourceSchema)
				// fmt.Println(rs.SourceTable)
				// fmt.Println(rs.Operation)
				splitDDL := strings.Split(rs.SQLRedo, ` `)
				fmt.Println(splitDDL)
				zap.L().Info("increment table log extractor sql",
					zap.String("get sql", fmt.Sprintf("%v", splitDDL)))
				// SQLUndo := strings.Split(rs.SQLUndo, ` `)
				// fmt.Println(SQLUndo)
			}
		}
		if err != nil {
			return err
		}
		zap.L().Info("increment table log extractor", zap.String("logfile", log["LOG_FILE"]),
			zap.Uint64("logfile start scn", logFileStartSCN),
			zap.Uint64("source table last scn", logFileEndSCN),
			zap.Int("row counts", len(rowsResult)),
			zap.String("cost time", endTime.Sub(startTime).String()))

		// logminer 关闭
		if err = r.OracleMiner.EndOracleLogminerStoredProcedure(); err != nil {
			// return err
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

//获取增量所需得日志文件
func (r *Migrate) getTableIncrRecordLogfile() ([]map[string]string, error) {
	var logFiles []map[string]string
	firstSCN, _, _, err := r.OracleMiner.GetOracleCurrentRedoMaxSCN()
	if err != nil {
		return logFiles, err
	}
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
