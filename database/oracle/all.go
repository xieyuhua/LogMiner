package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"gominerlog/common"
	"time"

	"go.uber.org/zap"
)

type logminers struct {
	SCN          uint64
	SourceSchema string
	SourceTable  string
	SQLRedo      string
	SQLUndo      string
	Operation    string
}

// 捕获增量数据
func (o *Oracle) GetOracleIncrRecord(sourceSchema, lastCheckpoint string, queryTimeout int) ([]logminers, uint64, error) {
	var lcs []logminers
	var MaxSCN uint64
	logFileEndSCN, err := common.StrconvUintBitSize(lastCheckpoint, 64)
	MaxSCN = logFileEndSCN
	querySQL := common.StringsBuilder(`SELECT SCN,
       SEG_OWNER AS SOURCE_SCHEMA,
       TABLE_NAME AS SOURCE_TABLE,
       SQL_REDO,
       SQL_UNDO,
       OPERATION
  FROM V$LOGMNR_CONTENTS
 WHERE 1 = 1
   AND UPPER(SEG_OWNER) = '`, common.StringUPPER(sourceSchema), `'
   AND OPERATION IN ('INSERT', 'DELETE', 'UPDATE', 'DDL')
   AND SCN > `, lastCheckpoint, ` ORDER BY SCN`)
	rows, err := o.OracleDB.Query(querySQL)
	if err != nil {
		return lcs, MaxSCN, err
	}
	startTime := time.Now()
	for rows.Next() {
		var lc logminers
		if err = rows.Scan(&lc.SCN, &lc.SourceSchema, &lc.SourceTable, &lc.SQLRedo, &lc.SQLUndo, &lc.Operation); err != nil {
			return lcs, MaxSCN, err
		}
		if lc.SCN > MaxSCN {
			MaxSCN = lc.SCN
		}
		// 目标库名以及表名
		lcs = append(lcs, lc)
	}
	defer rows.Close()
	endTime := time.Now()

	jsonLCS, err := json.Marshal(lcs)
	if err != nil {
		return lcs, MaxSCN, fmt.Errorf("json Marshal logminer failed: %v", err)
	}
	zap.L().Info("logminer sql",
		zap.String("sql", querySQL),
		zap.String("json logminer content", string(jsonLCS)),
		zap.String("start time", startTime.String()),
		zap.String("end time", endTime.String()),
		zap.String("cost time", endTime.Sub(startTime).String()))
	return lcs, MaxSCN, nil

}

func (o *Oracle) GetOracleRedoLogSCN(scn string) (uint64, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`select  FIRST_CHANGE# AS SCN
  from (SELECT  GROUP#,
               first_change#,
               MEMBERS,
               FIRST_TIME
          FROM v$LOG
         WHERE FIRST_CHANGE# <= `, scn, ` order by FIRST_CHANGE# desc)
 where rownum = 1`))
	var globalSCN uint64
	if err != nil {
		return globalSCN, err
	}
	if len(res) > 0 {
		globalSCN, err = common.StrconvUintBitSize(res[0]["SCN"], 64)
		if err != nil {
			return globalSCN, fmt.Errorf("get oracle redo log scn %s utils.StrconvUintBitSize failed: %v", res[0]["SCN"], err)
		}
	}
	return globalSCN, nil
}

func (o *Oracle) GetOracleArchivedLogSCN(scn string) (uint64, error) {
	var globalSCN uint64
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`select FIRST_CHANGE# AS SCN
  from (select FIRST_CHANGE#,FIRST_TIME
          from v$archived_log
         where STATUS = 'A'
           and DELETED='NO'
           and FIRST_CHANGE# <= `, scn, ` order by FIRST_CHANGE# desc)
 where rownum = 1`))
	if err != nil {
		return globalSCN, err
	}
	if len(res) > 0 {
		globalSCN, err = common.StrconvUintBitSize(res[0]["SCN"], 64)
		if err != nil {
			return globalSCN, fmt.Errorf("get oracle archive log scn %s utils.StrconvUintBitSize failed: %v", res[0]["SCN"], err)
		}
	}
	return globalSCN, nil
}

func (o *Oracle) GetOracleRedoLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT 
       --l.GROUP# GROUP_NUMBER,
       l.FIRST_CHANGE# AS FIRST_CHANGE,
       --l.BYTES / 1024 / 1024 AS LOG_SIZE,
       L.NEXT_CHANGE# AS NEXT_CHANGE,
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf, v$LOG l
 WHERE l.GROUP# = lf.GROUP#
   AND l.FIRST_CHANGE# >= `, scn, ` ORDER BY l.FIRST_CHANGE# ASC`))
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (o *Oracle) GetOracleArchivedLogFile(scn string) ([]map[string]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT NAME AS LOG_FILE,
       NEXT_CHANGE# AS NEXT_CHANGE,
       --BLOCKS * BLOCK_SIZE / 1024 / 1024 AS LOG_SIZE,
       FIRST_CHANGE# AS FIRST_CHANGE
  FROM v$ARCHIVED_LOG
 WHERE STATUS = 'A'
   AND DELETED = 'NO'
   AND FIRST_CHANGE# >= `, scn, ` ORDER BY FIRST_CHANGE# ASC`))
	if err != nil {
		return []map[string]string{}, err
	}
	return res, nil
}

func (o *Oracle) GetOracleCurrentRedoMaxSCN() (uint64, uint64, string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT
       l.FIRST_CHANGE# AS FIRST_CHANGE,
       l.NEXT_CHANGE# AS NEXT_CHANGE,
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf, v$LOG l
 WHERE l.GROUP# = lf.GROUP#
 AND l.STATUS='CURRENT'`))
	if err != nil {
		return 0, 0, "", err
	}
	if len(res) == 0 {
		return 0, 0, "", fmt.Errorf("oracle current redo log can't null")
	}
	firstSCN, err := common.StrconvUintBitSize(res[0]["FIRST_CHANGE"], 64)
	if err != nil {
		return firstSCN, 0, res[0]["LOG_FILE"], fmt.Errorf("get oracle current redo first_change scn %s utils.StrconvUintBitSize falied: %v", res[0]["FIRST_CHANGE"], err)
	}
	maxSCN, err := common.StrconvUintBitSize(res[0]["NEXT_CHANGE"], 64)
	if err != nil {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("get oracle current redo next_change scn %s utils.StrconvUintBitSize falied: %v", res[0]["NEXT_CHANGE"], err)
	}
	if maxSCN == 0 || firstSCN == 0 {
		return firstSCN, maxSCN, res[0]["LOG_FILE"], fmt.Errorf("GetOracleCurrentRedoMaxSCN value is euqal to 0, does't meet expectations")
	}
	return firstSCN, maxSCN, res[0]["LOG_FILE"], nil
}

func (o *Oracle) GetOracleALLRedoLogFile() ([]string, error) {
	_, res, err := Query(o.Ctx, o.OracleDB, common.StringsBuilder(`SELECT
       lf.MEMBER LOG_FILE
  FROM v$LOGFILE lf`))
	if err != nil {
		return []string{}, err
	}
	if len(res) == 0 {
		return []string{}, fmt.Errorf("oracle all redo log can't null")
	}

	var logs []string
	for _, r := range res {
		logs = append(logs, r["LOG_FILE"])
	}
	return logs, nil
}

func (o *Oracle) AddOracleLogminerlogFile(logFile string) error {
	ctx, _ := context.WithCancel(context.Background())
	sql := common.StringsBuilder(`BEGIN
  dbms_logmnr.add_logfile(logfilename => '`, logFile, `',
                          options     => dbms_logmnr.NEW);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("oracle logminer sql [%v] add log file [%s] failed: %v", sql, logFile, err)
	}
	return nil
}

func (o *Oracle) StartOracleLogminerStoredProcedure(scn string) error {
	ctx, _ := context.WithCancel(context.Background())
	sql := common.StringsBuilder(`BEGIN
  dbms_logmnr.start_logmnr(startSCN => `, scn, `,
                           options  => SYS.DBMS_LOGMNR.SKIP_CORRUPTION +       -- 日志遇到坏块，不报错退出，直接跳过
                                       SYS.DBMS_LOGMNR.NO_SQL_DELIMITER +
                                       SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT +
                                       SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY +
                                       SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                                       SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);
END;`)
	_, err := o.OracleDB.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure sql [%v] startscn [%v] failed: %v", sql, scn, err)
	}

	return nil
}

func (o *Oracle) EndOracleLogminerStoredProcedure() error {
	ctx, _ := context.WithCancel(context.Background())
	_, err := o.OracleDB.ExecContext(ctx, common.StringsBuilder(`BEGIN
  sys.dbms_logmnr.end_logmnr();
END;`))
	if err != nil {
		return fmt.Errorf("oracle logminer stored procedure end failed: %v", err)
	}
	return nil
}
