package common

import "time"

// MySQL 连接配置
const (
	MySQLMaxIdleConn     = 512
	MySQLMaxConn         = 1024
	MySQLConnMaxLifeTime = 300 * time.Second
	MySQLConnMaxIdleTime = 200 * time.Second
)

// 任务并发通道 Channle Size
const ChannelBufferSize = 1024

// 任务模式
const (
	TaskModePrepare = "PREPARE"
	TaskModeAssess  = "ASSESS"
	TaskModeReverse = "REVERSE"
	TaskModeCheck   = "CHECK"
	TaskModeCompare = "COMPARE"
	TaskModeCSV     = "CSV"
	//现在使用的
	TaskModeFull = "FULL"
	TaskModeAll  = "ALL"
	TaskModeINCR = "INCR"
)

// 任务状态
const (
	TaskStatusWaiting = "WAITING"
	TaskStatusRunning = "RUNNING"
	TaskStatusSuccess = "SUCCESS"
	TaskStatusFailed  = "FAILED"
)

// 任务初始值
const (
	// 值 0 代表源端表未进行初始化 -> 适用于 full/csv/all 模式
	TaskTableDefaultSourceGlobalSCN = 0
	// 值 -1 代表源端表未进行 chunk 切分 -> 适用于 full/csv/all 模式
	TaskTableDefaultSplitChunkNums = -1
)

// 任务 DB 类型
const (
	DatabaseTypeOracle = "ORACLE"
	DatabaseTypeTiDB   = "TIDB"
	DatabaseTypeMySQL  = "MYSQL"
)
