package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"gominerlog/common"
	"gominerlog/config"
	"strconv"
	"strings"

	sqloracle "github.com/wdrabbit/gorm-oracle"
	"gorm.io/gorm"
)

type Oracle struct {
	Ctx      context.Context
	OracleDB *sql.DB
}

// 创建 oracle 数据库引擎
func NewOracleDBEngine(ctx context.Context, oraCfg config.OracleConfig) (*Oracle, error) {
	connString := fmt.Sprintf("oracle://%s:%s@%s/%s",
		oraCfg.Username,
		oraCfg.Password,
		common.StringsBuilder(oraCfg.Host, ":", strconv.Itoa(oraCfg.Port)),
		oraCfg.NLSLang)
	fmt.Println(connString)
	db, err := gorm.Open(sqloracle.Open(connString), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
	}
	sqlDB, _ := db.DB()
	err = sqlDB.Ping()

	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}

	return &Oracle{
		Ctx:      ctx,
		OracleDB: sqlDB,
	}, nil
}

// Only Used for ALL Mode
func NewOracleLogminerEngine(ctx context.Context, oraCfg config.OracleConfig) (*Oracle, error) {
	connString := fmt.Sprintf("oracle://%s:%s@%s/%s",
		oraCfg.Username,
		oraCfg.Password,
		common.StringsBuilder(oraCfg.Host, ":", strconv.Itoa(oraCfg.Port)),
		oraCfg.NLSLang)
	db, err := gorm.Open(sqloracle.Open(connString), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
	}
	sqlDB, _ := db.DB()
	err = sqlDB.Ping()

	if err != nil {
		return nil, fmt.Errorf("error on ping oracle database connection:%v", err)
	}
	return &Oracle{
		Ctx:      ctx,
		OracleDB: sqlDB,
	}, nil
}

func Query(ctx context.Context, db *sql.DB, querySQL string) ([]string, []map[string]string, error) {
	var (
		cols []string
		res  []map[string]string
	)
	rows, err := db.QueryContext(ctx, querySQL)
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query failed: [%v]", querySQL, err.Error())
	}
	defer rows.Close()

	//不确定字段通用查询，自动获取字段名称
	cols, err = rows.Columns()
	if err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Columns failed: [%v]", querySQL, err.Error())
	}

	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return cols, res, fmt.Errorf("general sql [%v] query rows.Scan failed: [%v]", querySQL, err.Error())
		}

		row := make(map[string]string)
		for k, v := range values {
			// Oracle/Mysql 对于 'NULL' 统一字符 NULL 处理，查询出来转成 NULL,所以需要判断处理
			// 查询字段值 NULL
			// 如果字段值 = NULLABLE 则表示值是 NULL
			// 如果字段值 = "" 则表示值是空字符串
			// 如果字段值 = 'NULL' 则表示值是 NULL 字符串
			// 如果字段值 = 'null' 则表示值是 null 字符串
			if v == nil {
				row[cols[k]] = "NULLABLE"
			} else {
				// 处理空字符串以及其他值情况
				// 数据统一 string 格式显示
				row[cols[k]] = string(v)
			}
		}
		res = append(res, row)
	}

	if err = rows.Err(); err != nil {
		return cols, res, fmt.Errorf("general sql [%v] query rows.Next failed: [%v]", querySQL, err.Error())
	}
	return cols, res, nil
}

func (o *Oracle) GetOracleSchemas() ([]string, error) {
	var (
		schemas []string
		err     error
	)
	cols, res, err := Query(o.Ctx, o.OracleDB, `SELECT DISTINCT username FROM DBA_USERS`)
	if err != nil {
		return schemas, err
	}
	for _, col := range cols {
		for _, r := range res {
			schemas = append(schemas, common.StringUPPER(r[col]))
		}
	}
	return schemas, nil
}

func (o *Oracle) GetOracleSchemaTable(schemaName string) ([]string, error) {
	var (
		tables []string
		err    error
	)
	_, res, err := Query(o.Ctx, o.OracleDB, fmt.Sprintf(`SELECT table_name AS TABLE_NAME FROM DBA_TABLES WHERE UPPER(owner) = UPPER('%s') AND (IOT_TYPE IS NUll OR IOT_TYPE='IOT')`, schemaName))
	if err != nil {
		return tables, err
	}
	for _, r := range res {
		tables = append(tables, strings.ToUpper(r["TABLE_NAME"]))
	}

	return tables, nil
}
